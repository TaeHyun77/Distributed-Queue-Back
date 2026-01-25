package com.example.integrated.queue.queue

import com.example.integrated.queue.idempotency.IdempotencyService
import com.example.integrated.queue.kafka.KafkaProducerService
import com.example.integrated.queue.queue.dto.RegisterResult
import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import com.example.integrated.util.*
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseCookie
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.stereotype.Service
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

@Service
class QueueService (
    @Value("\${queue.validation.key}")
    private val validationKey: String,

    private val kafkaProducerService: KafkaProducerService,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
    private val idempotencyService: IdempotencyService,

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
            if (idempotencyService.checkAndSaveIdempotencyKey(queueType, userId, idempotencyKey)) {
                return RegisterResult.DUPLICATE_REQUEST
            }

            // redis 대기열 또는 참가열에 해당 사용자가 존재하는지 확인
            validateUserNotInQueue(queueType, userId)

            val timestamp: Long = generateScore()
            val isKafkaProduce = kafkaProducerService.sendMessage(queueType, userId, timestamp.toDouble())

            if (!isKafkaProduce) {
                throw ReserveException(HttpStatus.SERVICE_UNAVAILABLE, ErrorCode.KAFKA_PRODUCE_FAILED)
            }

            return RegisterResult.SUCCESS

        } catch (e: ReserveException) {
            if (e.errorCode == ErrorCode.ALREADY_REGISTERED_USER_IN_QUEUE) {
                return RegisterResult.ALREADY_REGISTERED
            }
            throw e
        } catch (e: Exception) {
            log.error{ "대기열 등록 중 알 수 없는 에러 발생 - ${e.message}" }
            throw ReserveException(HttpStatus.INTERNAL_SERVER_ERROR, ErrorCode.FAIL_TO_REGISTER)
        }
    }

    /*
    * 대기열, 참가열에서의 사용자 존재 여부
    * 두 비동기 작업이 모두 완료된 후에야 다음 로직이 실행됨
    * */
    suspend fun validateUserNotInQueue(
        queueType: String,
        userId: String
    ) = coroutineScope {

        val waitRankDeferred = async { getUserRank(queueType, userId) }
        val isAllowedDeferred = async { isAllowTokenExpired(queueType, userId) }

        val waitRank: Long = waitRankDeferred.await()
        val isAllowed: Boolean = isAllowedDeferred.await()

        if (waitRank >= 0L || isAllowed) {
            throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.ALREADY_REGISTERED_USER_IN_QUEUE)
        }
    }

    /*
     * 대기열에서 사용자 순위 조회
     * 존재하지 않는다면 -1L 반환, 존재한다면 사용자의 순위 반환
     * */
    suspend fun getUserRank(
        queueType: String,
        userId: String
    ): Long {
        val key = queueType + WAIT_QUEUE

        val rank = reactiveRedisTemplate.opsForZSet()
            .rank(key, userId)
            .awaitFirstOrNull()
            ?.let { it + 1L }
            ?: -1L

        if (rank < 0) {
            log.info { "$userId 님이 존재하지 않습니다." }
        } else {
            log.info { "$userId 님의 현재 순위 $rank" }
        }

        return rank
    }

    // 열에서의 사용자 삭제
    suspend fun cancelUser(queueType: String, userId: String): Boolean {
        val removedFromWait = removeFromWaitQueue(queueType, userId)
        val removedFromAllow = removeFromAllowQueue(queueType, userId)

        return removedFromWait || removedFromAllow
    }

    // 대기열에서 사용자 삭제
    private suspend fun removeFromWaitQueue(
        queueType: String,
        userId: String
    ): Boolean {
        val key = "$queueType$WAIT_QUEUE"

        return reactiveRedisTemplate.opsForZSet()
            .remove(key, userId)
            .awaitSingle() > 0
    }

    // 참가열에서 사용자 삭제
    private suspend fun removeFromAllowQueue(
        queueType: String,
        userId: String
    ): Boolean {
        val key = "$queueType:allow:$userId"

        return reactiveRedisTemplate.delete(key)
            .awaitSingle() > 0
    }

    // 토큰을 쿠키에 저장
    fun issueAccessTokenCookie(
        queueType: String,
        userId: String,
        response: ServerHttpResponse
    ): ResponseEntity<String> {

        val encodedName = URLEncoder.encode(userId, StandardCharsets.UTF_8)
        val cookieName = "reserve-user-access-cookie-$encodedName"

        val token = createAccessToken(queueType, userId)

        val responseCookie = ResponseCookie.from(cookieName, token)
            .path("/")
            .maxAge(Duration.ofSeconds(600))
            .build()

        response.addCookie(responseCookie)

        return ResponseEntity.ok("쿠키 발급 완료")
    }

    // 인증을 위한 토큰 생성
    fun createAccessToken(
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

    // 대기열 -> 참가열 이동 로직
    // Redis Lua Script로 “pop → add → TTL”을 하나의 원자 연산으로 묶는 방법으로 개선 고려
    suspend fun allowUser(
        queueType: String,
        count: Long
    ): Int {
        val waitQueueKey = "$queueType$WAIT_QUEUE"

        val poppedUsers = reactiveRedisTemplate.opsForZSet()
            .popMin(waitQueueKey, count)
            .collectList()
            .awaitSingle() // 값이 있으면 List<T> 반환, 값이 없으면 빈 List 반환

        if (poppedUsers.isEmpty()) return 0

        poppedUsers.forEach { user ->
            val userId = user.value.toString()
            val key = "$queueType:allow:$userId"

            // 참가열에 사용자 이동
            reactiveRedisTemplate.opsForValue()
                .set(key, "1", Duration.ofMinutes(10))
                .awaitSingle()
        }

        if (!poppedUsers.isEmpty()) {
            redisPublisher.publish(CHANNEL_NAME, queueType)
        }

        return poppedUsers.size
    }

    // 입장 시간 생성 로직
    suspend fun generateScore(): Long {
        val timestampMicros = Instant.now().toEpochMilli() * 1000

        val seq = reactiveRedisTemplate.opsForValue()
            .increment("queue:seq")
            .awaitSingle()

        val seqMasked = seq and 0xFFFFF
        return (timestampMicros shl 20) or seqMasked
    }

    // 타겟 페이지에 접속했을 때, 쿠키에 저장된 토큰의 유효성을 검증하는 로직
    suspend fun isAllowTokenValid(
        queueType: String,
        userId: String,
        token: String
    ): Boolean {
        if (!isAllowTokenExpired(queueType, userId)) return false
        if (!isAllowTokenMatched(queueType, userId, token)) return false

        return true
    }

    // 참가열에서의 사용자 TTL 만료 여부 조회
    suspend fun isAllowTokenExpired(
        queueType: String,
        userId: String
    ): Boolean {
        val key = "$queueType:allow:$userId"

        return reactiveRedisTemplate
            .hasKey(key)
            .awaitSingle()
    }

    // 토큰의 유효성 판별 로직
    private fun isAllowTokenMatched(
        queueType: String,
        userId: String,
        token: String
    ): Boolean {
        return createAccessToken(queueType, userId) == token
    }
}