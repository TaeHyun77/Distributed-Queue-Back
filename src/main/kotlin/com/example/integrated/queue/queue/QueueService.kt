package com.example.integrated.queue.queue

import com.example.integrated.queue.idempotency.Idempotency
import com.example.integrated.queue.idempotency.IdempotencyRepository
import com.example.integrated.queue.kafka.KafkaProducerService
import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import com.example.integrated.util.*
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseCookie
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.NoSuchAlgorithmException
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.util.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

@Service
class QueueService (
    @Value("\${queue.validation.key}")
    private val validationKey: String,

    private val kafkaProducerService: KafkaProducerService,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
    private val idempotencyRepository: IdempotencyRepository,

    private val redisPublisher: RedisPublisher
): Loggable {

    // 대기열 등록
    suspend fun registerUserToWaitQueue(
        queueType: String,
        userId: String,
        idempotencyKey: String
    ): RegisterResult {

        try {
            // 멱등성 로직
            if (isIdempotent(queueType, userId, idempotencyKey)) {
                return RegisterResult.ALREADY_IDEMPOTENCY_EXISTS
            }

            // redis 대기열 또는 참가열에 해당 사용자가 존재하는지 확인
            validateUserNotQueued(queueType, userId)

            val timestamp: Long = generateScore()
            kafkaProducerService.sendMessage(queueType, userId, timestamp.toDouble())

            return RegisterResult.QUEUE_REGISTERED
        } catch (e: ReserveException) {
            log.error{ "대기열 등록 중 에러 발생 - ${e.message}" }
            throw e
        } catch (e: Exception) {
            log.error{ "대기열 등록 중 알 수 없는 에러 발생 - ${e.message}" }
            throw ReserveException(HttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.FAIL_TO_REGISTER)
        }
    }

    /** 멱등 로직 확인
     * 키 값으로 조회하여 있으면 false 반환하는 방법은 동시성 문제가 발생할 수 있으므로 아래와 같이 설정
     * expiresAt을 설정하여 특정 기간마다 삭제하여 정리하도록 함
     * */
    @Transactional
    suspend fun isIdempotent(
        queueType: String,
        userId: String,
        idempotencyKey: String
    ): Boolean {

        val now = LocalDateTime.now()

        val entity = Idempotency(
            idempotencyKey = idempotencyKey,
            userId = userId,
            queueType = queueType,
            expiresAt = now.plusMinutes(5)
        )

        return try {
            idempotencyRepository.save(entity)
            false
        } catch (e: DuplicateKeyException) {
            log.info { "중복된 요청입니다 - ${entity.idempotencyKey}" }
            true // 이미 처리된 요청
        }
    }

    /*
    * 대기큐 혹은 허용큐에서의 존재 여부 파악
    * 두 비동기 작업이 모두 완료된 후에야 다음 로직이 실행됨
    * */
    suspend fun validateUserNotQueued(
        queueType: String,
        userId: String
    ) {
        val (waitRank, allowRank) = coroutineScope {
            awaitAll(
                async { searchUserRanking(queueType, userId, "wait") },
                async { searchUserRanking(queueType, userId, "allow") }
            )
        }

        if (waitRank >= 0 || allowRank >= 0) {
            throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.ALREADY_REGISTERED_USER_IN_QUEUE)
        }
    }

    /**
     * 대기열 or 참가열에서 사용자 순위 조회
     * 존재하지 않는다면 -1L 반환, 존재한다면 사용자의 순위 반환
     * */
    suspend fun searchUserRanking(
        queueType: String,
        userId: String,
        queueCategory: String
    ): Long {

        val keyType = if (queueCategory == "wait") WAIT_QUEUE else ALLOW_QUEUE
        val queueKey = queueType + keyType // "reserve_공연A:user-queue:allow"와 같은 형태

        val rank = reactiveRedisTemplate.opsForZSet()
            .rank(queueKey, userId)
            .awaitFirstOrNull()
            ?.let { it + 1 }
            ?: -1L

        if (rank < 0) {
            log.info { "[$queueCategory] $userId 님이 존재하지 않습니다." }
        } else {
            log.info { "[$queueCategory] $userId 님의 현재 순위 $rank" }
        }

        return rank
    }

    /**
     * 대기열에서 사용자 삭제
     * */
    suspend fun cancelUser(
        queueType: String,
        userId: String,
        queueCategory: String
    ): Boolean {
        try {
            return when (queueCategory) {
                "wait" -> cancelWaitOrAllow(queueType, userId)
                "allow" -> cancelAllowUser(queueType, userId)
                else -> throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.INVALID_QUEUE_CATEGORY)
            }
        } catch (e: Exception) {
            log.error(" 대기열/참가열 삭제 중 에러 발생 - ${e.message}")

            throw ReserveException(HttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.REDIS_OPERATION_FAILED)
        }
    }

    /**
    *  문제 상황 발생 가능성
    *
    *  승격 로직이 wait에서 사용자를 삭제했을 때, 취소 요청이 오는 경우 대기열에는 사용자가 없기에 문제가 발생할 수 있음
    *  이러한 타이밍으로 인한 경쟁 상태를 별도로 관리하여 문제를 해결
    * */
    suspend fun cancelWaitOrAllow(
        queueType: String,
        userId: String
    ): Boolean {
        val waitQueueKey = "$queueType$WAIT_QUEUE"

        // 0 : 해당 사용자 없음 , 1 : 해당 사용자 삭제
        val isRemovedWait = reactiveRedisTemplate.opsForZSet()
            .remove(waitQueueKey, userId)
            .awaitSingle()

        if (isRemovedWait > 0) {
            log.info { "$userId 님 대기열에서 삭제 완료" }

            redisPublisher.publish(CHANNEL_NAME, queueType)
            return true
        }

        // 대기열에서 삭제 실패했다면 참가열에서 삭제 시도
        val isRemovesAllow = cancelAllowUser(queueType, userId)

        if (isRemovesAllow){
            log.info { "참가열 삭제 완료" }
        } else {
            log.info { "참가열 삭제 실패" }
        }

        return isRemovesAllow
    }

    /**
     * 허용열에서 사용자 삭제
    * */
    suspend fun cancelAllowUser(
        queueType: String,
        userId: String
    ): Boolean {
        val allowQueueKey = "$queueType$ALLOW_QUEUE"

        val isRemoved = reactiveRedisTemplate.opsForZSet()
            .remove(allowQueueKey, userId)
            .awaitSingle() == 1L

        if (isRemoved) removeTtlKey(userId)

        return isRemoved
    }

    /*
    * 인증을 위한 토큰 생성
    * */
    fun generateAccessToken(
        queueType: String,
        userId: String
    ): String {
        try {
            val mac = Mac.getInstance("HmacSHA256")
            val keySpec = SecretKeySpec(validationKey.toByteArray(StandardCharsets.UTF_8), "HmacSHA256")
            mac.init(keySpec)

            val raw = "$queueType:$userId"
            val digest = mac.doFinal(raw.toByteArray(StandardCharsets.UTF_8))

            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest)

        } catch (e: Exception) {
            log.error(e) { "토큰 생성 중 에러 발생." }

            throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.FAIL_TO_GENERATE_TOKEN)
        }
    }

    /*
    * 토큰을 쿠키에 저장
    * */
    fun sendCookie(
        queueType: String,
        userId: String,
        response: ServerHttpResponse
    ): ResponseEntity<String> {

        val encodedName = URLEncoder.encode(userId, StandardCharsets.UTF_8)
        val cookieName = "reserve_user-access-cookie_$encodedName"

        val token = generateAccessToken(queueType, userId)

        val responseCookie = ResponseCookie.from(cookieName, token)
            .path("/")
            .maxAge(Duration.ofSeconds(600))
            .build()

        response.addCookie(responseCookie)

        return ResponseEntity.ok("쿠키 발급 완료")
    }

    /*
    * 타겟 페이지에 접속하면 쿠키에 저장된 토큰과 비교하는 로직
    * */
    suspend fun isAccessTokenValid(
        queueType: String,
        userId: String,
        token: String
    ): Boolean {
        return getTtlInfo(userId)
            ?.toLong()
            ?.let { expireAt ->
                val now = Instant.now().epochSecond
                val notExpired = now < expireAt
                val tokenMatches = generateAccessToken(queueType, userId) == token

                notExpired && tokenMatches
            } ?: false
    }

    suspend fun allowUser(
        queueType: String,
        count: Long
    ): Int {
        val waitQueueKey = "$queueType$WAIT_QUEUE"
        val allowQueueKey = "$queueType$ALLOW_QUEUE"

        val poppedUsers = reactiveRedisTemplate.opsForZSet()
            .popMin(waitQueueKey, count)
            .collectList()
            .awaitSingle() // 값이 있으면 List<T> 반환, 값이 없으면 빈 List 반환

        if (poppedUsers.isEmpty()) return 0

        poppedUsers.forEach { user ->
            val userId = user.value.toString()
            val timestamp: Long = generateScore()
            val expireAt = Instant.now().plus(Duration.ofMinutes(10)).epochSecond.toDouble()

            // 참가열에 사용자 이동
            reactiveRedisTemplate.opsForZSet()
                .add(allowQueueKey, userId, timestamp.toDouble())
                .awaitSingle()

            // 참가열 TTL 생성
            reactiveRedisTemplate.opsForZSet()
                .add(TOKEN_TTL_INFO, userId, expireAt)
                .awaitSingle()
        }

        if (!poppedUsers.isEmpty()) {
            redisPublisher.publish(CHANNEL_NAME, queueType)
        }

        return poppedUsers.size
    }

    suspend fun generateScore(): Long {
        val timestampMicros = Instant.now().toEpochMilli() * 1000

        val seq = reactiveRedisTemplate.opsForValue()
            .increment("queue:seq")
            .awaitSingle()

        val seqMasked = seq and 0xFFFFF
        return (timestampMicros shl 20) or seqMasked
    }

    /*
    * TTL 키 삭제 로직
    * */
    private suspend fun removeTtlKey(userId: String) {
        val isRemove = reactiveRedisTemplate.opsForZSet()
            .remove(TOKEN_TTL_INFO, userId)
            .awaitSingle()

        require(isRemove == 1L) {"TTL 키가 존재하지 않습니다"}
    }

    private suspend fun getTtlInfo(
        userId: String
    ): Double? {
        val ttlInfo: Double? = reactiveRedisTemplate.opsForZSet()
            .score(TOKEN_TTL_INFO, userId)
            .awaitSingleOrNull()

        return ttlInfo
    }
}