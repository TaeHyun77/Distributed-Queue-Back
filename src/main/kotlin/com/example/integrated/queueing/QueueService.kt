package com.example.integrated.queueing

import com.example.integrated.Loggable
import com.example.integrated.idempotency.IdempotencyService
import com.example.integrated.redis.lock.RedisLockUtil
import com.example.integrated.queueing.kafka.KafkaProducerService
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import com.example.integrated.util.ACCESS_TOKEN
import com.example.integrated.util.ALLOW_QUEUE
import com.example.integrated.util.TOKEN_TTL_INFO
import com.example.integrated.util.WAIT_QUEUE
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseCookie
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Sinks
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.time.Duration
import java.time.Instant
import kotlin.system.measureTimeMillis

@Service
class QueueService (

    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
    private val idempotencyService: IdempotencyService,
    private val redisLockUtil: RedisLockUtil,
    private val kafkaProducerService: KafkaProducerService
): Loggable {

    suspend fun register(
        userId: String,
        queueType: String,
        enterTimestamp: Long,
        idempotencyKey: String
    ): ResponseEntity<String> {

        return redisLockUtil.acquireLockAndRun("$userId:$queueType:queueing")
        { idempotencyService.execute(
            key = idempotencyKey,
            url = "/queue/register",
            method = "POST",
        ) { registerUserToWaitQueue(userId, queueType, enterTimestamp) }
        }
    }

    /**
     * 대기열 등록
     *
     * queueType은 "reserve_[대기열 종류]"의 형태로 되어 있음
    * */
    suspend fun registerUserToWaitQueue(
        userId: String,
        queueType: String,
        enterTimestamp: Long
    ): String {

        log.info { "Current thread: ${Thread.currentThread().name}" }

        coroutineScope {

            val waitTime = measureTimeMillis {

                // 두 작업을 병렬로 실행
                val inWaitDeferred = async { searchUserRanking(userId, queueType, "wait") }
                val inAllowDeferred = async { searchUserRanking(userId, queueType, "allow") }

                val inWait = inWaitDeferred.await()
                val inAllow = inAllowDeferred.await()


                if (inWait != -1L || inAllow != -1L) {
                    throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.ALREADY_REGISTERED_USER)
                }
            }
            log.info { "wait : $waitTime" }
        }

        val waitQueueKey = queueType + WAIT_QUEUE

        val wasAdded = reactiveRedisTemplate.opsForZSet()
            .add(waitQueueKey, userId, enterTimestamp.toDouble())
            .awaitFirstOrNull() // Boolean 혹은 null 반환

        if (wasAdded == null || !wasAdded) {
            throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.ALREADY_REGISTERED_USER)
        } else {
            // 대기열에 성공적으로 추가 되었다면 카프카 메세지 전송
            kafkaProducerService.sendMessage(queueType)
        }

        val rank = searchUserRanking(userId, queueType, "wait")
        log.info{"${userId}님 ${rank}번째로 사용자 대기열 등록 성공" }

        return "해당 사용자는 이미 대기열에 등록되셨습니다."
    }

    /**
     * 대기열 or 참가열에서 사용자 순위 조회
     * 존재하지 않는다면 -1L 반환, 존재한다면 사용자의 순위 반환
     * */
    suspend fun searchUserRanking(
        userId: String,
        queueType: String,
        queueCategory: String
    ): Long {

        val keyType = if (queueCategory == "wait") WAIT_QUEUE else ALLOW_QUEUE

        val queueKey = queueType + keyType

        val redisRank = reactiveRedisTemplate.opsForZSet()
            .rank(queueKey, userId)
            .awaitFirstOrNull()

        val rank = redisRank?.takeIf { it >= 0 }?.plus(1) ?: -1

        if (rank == -1L) {
            log.info { "[$queueCategory] $userId 님이 존재하지 않습니다." }
        } else {
            log.info { "[$queueCategory] $userId 님의 현재 순위 $rank" }
        }

        return rank
    }

    suspend fun cancelUser(userId: String, queueType: String, queueCategory: String): ResponseEntity<String> {
        return when (queueCategory) {
            "wait" -> cancelWaitOrAllow(userId, queueType)
            "allow" -> cancelAllowUser(userId, queueType)
            else -> throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.INVALID_QUEUE_CATEGORY)
        }
    }

    /*
    * 문제 상황 발생 가능성
    *
    * 승격 로직이 wait에서 사용자를 삭제하고 allow로 옮기기 전에 취소 로직이 allow에서 삭제를 진행하는 경우
    * ⇒ 이러한 타이밍으로 인한 경쟁 상태를 별도로 관리하여 문제를 해결
    * */
    private suspend fun cancelWaitOrAllow(userId: String, queueType: String): ResponseEntity<String> {
        val waitQueueKey = "$queueType$WAIT_QUEUE"

        val removedCount = reactiveRedisTemplate.opsForZSet()
            .remove(waitQueueKey, userId)
            .awaitSingle()

        return if (removedCount > 0L) {
            kafkaProducerService.sendMessage(queueType)
            ResponseEntity.ok("대기열 삭제 완료")
        } else {
            val allowResult = cancelAllowUserForWaitContext(userId, queueType)
            allowResult ?: run {
                log.info { "이미 삭제가 처리된 사용자" }
                ResponseEntity.ok("이미 삭제가 처리된 사용자")
            }
        }
    }

    // 경쟁 상태 문제로 인한 별도의 로직
    private suspend fun cancelAllowUserForWaitContext(
        userId: String, queueType: String
    ): ResponseEntity<String>? {
        val allowQueueKey = "$queueType$ALLOW_QUEUE"

        val allowRemovedCount = reactiveRedisTemplate.opsForZSet()
            .remove(allowQueueKey, userId)
            .awaitSingle()

        // 승격 중 타이밍 문제로 간주
        if (allowRemovedCount == 0L) return null

        removeTtlKey(userId)
        return ResponseEntity.ok("참가열 삭제 완료")
    }

    suspend fun cancelAllowUser(
        userId: String,
        queueType: String,
    ): ResponseEntity<String> {

        try {
            val allowQueueKey = "$queueType$ALLOW_QUEUE"

            val allowRemovedCount = reactiveRedisTemplate.opsForZSet()
                .remove(allowQueueKey, userId)
                .awaitSingle()

            if (allowRemovedCount == 0L) {
                throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.USER_NOT_FOUND_IN_THE_QUEUE)
            }

            removeTtlKey(userId)
            return ResponseEntity.ok("참가열 삭제 완료")

        } catch (e: ReserveException) {
            log.error { "예약 취소 중 오류 발생" }
            throw e
        }
    }

    // TTL 키 삭제 로직
    private suspend fun removeTtlKey(userId: String) {
        val ttlRemovedCount = reactiveRedisTemplate.opsForZSet()
            .remove(TOKEN_TTL_INFO, userId)
            .awaitSingle()

        if (ttlRemovedCount == 0L) {
            log.warn { "$userId TTL 키가 존재하지 않아 삭제되지 않았습니다." }
        }
    }

    /**
     * 유효성 검사를 위한 토큰 생성
     */
    suspend fun generateAccessToken(
        userId: String,
        performanceId: Long
    ): String {

        return try {
            val digest = MessageDigest.getInstance("SHA-256")
            val raw = performanceId.toString() + ACCESS_TOKEN + userId
            val hash = digest.digest(raw.toByteArray(StandardCharsets.UTF_8))

            buildString {
                hash.forEach { append(String.format("%02x", it)) }
            }
        } catch (e: NoSuchAlgorithmException) {
            throw RuntimeException("Token 생성 실패", e)
        }
    }

    /**
     * 생성한 토큰을 쿠키에 저장
     *
     * ServerWebExchange : 요청/응답 전체를 포괄하는 컨텍스트, ServerHttpResponse : HTTP 응답 처리에만 특화된 객체
     */
    suspend fun sendCookie(
        userId: String,
        performanceId: Long,
        response: ServerHttpResponse
    ): ResponseEntity<String> {

        val encodedName = URLEncoder.encode(userId, StandardCharsets.UTF_8)
        val token = generateAccessToken(userId, performanceId)
        val cookieName = performanceId.toString() + "_user-access-cookie_$encodedName"

        val responseCookie = ResponseCookie.from(cookieName, token)
            .path("/")
            .maxAge(Duration.ofSeconds(300))
            .build()

        response.addCookie(responseCookie)

        return ResponseEntity.ok("쿠키 발급 완료")
    }

    /**
     * 토큰 유효성 검사
     */
    suspend fun isAccessTokenValid(
        userId: String,
        performanceId: Long,
        token: String
    ): Boolean {

        val ttlInfo = reactiveRedisTemplate
            .opsForZSet()
            .score(TOKEN_TTL_INFO, userId)
            .awaitFirstOrNull() ?: return false

        val expireTime = ttlInfo.toLong()
        val now = Instant.now().epochSecond

        if (expireTime < now) return false

        return if (generateAccessToken(userId, performanceId) == token) {
            log.info { "유효한 토큰입니다 !" }
            true
        } else false
    }

    /**
     * 대기열에서 새로고침 시 대기열 후순위 재등록 로직 - WAIT
     */
    suspend fun reEnterWaitQueue(
        userId: String,
        queueType: String
    ) {

        val now = Instant.now()
        val newTimestamp = now.epochSecond * 1_000_000_000L + now.nano
        val waitQueueKey = "$queueType$WAIT_QUEUE"

        val added = reactiveRedisTemplate
            .opsForZSet()
            .add(waitQueueKey, userId, newTimestamp.toDouble())
            .awaitFirstOrNull()

        if (added == true) {
            kafkaProducerService.sendMessage(queueType)
        }
    }

    /**
     * 참가열로 사용자 이동 + 토큰 TTL 저장
     */
    suspend fun allowUser(
        queueType: String,
        count: Long
    ): Long {
        val waitQueueKey = "$queueType$WAIT_QUEUE"
        val allowQueueKey = "$queueType$ALLOW_QUEUE"

        val poppedUsers = reactiveRedisTemplate
            .opsForZSet()
            .popMin(waitQueueKey, count)
            .collectList()
            .awaitSingle()

        if (poppedUsers.isEmpty()) return 0

        var movedCount = 0L

        for (user in poppedUsers) {
            val userId: String = user.value.toString()

            val now = Instant.now()
            val timestamp = now.epochSecond * 1_000_000_000L + now.nano
            val expireAt = now.plus(Duration.ofMinutes(10)).epochSecond

            // 참가열에 사용자 이동
            reactiveRedisTemplate.opsForZSet()
                .add(allowQueueKey, userId, timestamp.toDouble())
                .awaitFirstOrNull()

            // 참가열 TTL 생성
            reactiveRedisTemplate.opsForZSet()
                .add(TOKEN_TTL_INFO, userId, expireAt.toDouble())
                .awaitFirstOrNull()

            kafkaProducerService.sendMessage(queueType)
            movedCount++
        }

        return movedCount
    }

    /**
     * 대기열 → 참가열 사용자 이동 스케줄링
     */
    @OptIn(DelicateCoroutinesApi::class)
    @Scheduled(fixedDelay = 5000, initialDelay = 30000)
    fun moveUserToAllowQ() {

        val maxAllowedUsers = 3L
        val queueTypes = listOf("reserve_공연A", "reserve_공연B", "reserve_공연C")

        queueTypes.forEach { queueType ->

            // 각 다른 대기열의 스케줄링을 병렬적으로 실행
            CoroutineScope(Dispatchers.IO).launch {
                val count = allowUser(queueType, maxAllowedUsers)

                if (count > 0) {
                    log.info { "$queueType 허용열로 이동한 사용자 : $count" }
                } else {
                    log.info { "$queueType 허용열로 이동한 사용자 : 0" }
                }
            }
        }
    }
}