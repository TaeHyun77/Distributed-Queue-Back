package com.example.integrated.queue.queue

import com.example.integrated.queue.duplication.DuplicationCheckService
import com.example.integrated.queue.kafka.service.KafkaProducerService
import com.example.integrated.queue.queue.dto.RegisterResult
import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import com.example.integrated.util.*
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.DefaultTypedTuple
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseCookie
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.stereotype.Service
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

@Service
class QueueService (
        @Value("\${queue.validation.key}")
    private val validationKey: String,

        private val kafkaProducerService: KafkaProducerService,
        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
        private val duplicationCheckService: DuplicationCheckService,

        private val redisPublisher: RedisPublisher
): Loggable {

    // 대기열 등록
    suspend fun registerUserToWaitQueue(
        queueType: String,
        userId: String,
        requestKey: String,
        requestTimestamp: Double
    ): RegisterResult {

        // Nginx 도착 시각 기반 score 생성 ( 동일 밀리초 내에서는 Redis INCR로 순서 구분 )
        val timestamp: Double = generateScore(requestTimestamp)

        // 1. 중복된 요청인지 확인
        if (duplicationCheckService.isDuplicate(requestKey)) {
            return RegisterResult.DUPLICATE_REQUEST
        }

        // 2. 이미 대기 or 참가열에 사용자가 존재하는지 확인
        if (isUserInQueue(queueType, userId)) {
            return RegisterResult.ALREADY_REGISTERED
        }

        // 3. 카프카로 대기열 삽입 이벤트 전달
        if (!kafkaProducerService.sendMessage(queueType, userId, timestamp, (requestTimestamp * 1000).toLong())) {
            throw ReserveException(HttpStatus.SERVICE_UNAVAILABLE, ErrorCode.KAFKA_PRODUCE_FAILED)
        }

        return RegisterResult.SUCCESS
    }

    // 대기열에 사용자 삽입
    suspend fun enqueueToWaitQueue(
        queueType: String,
        userId: String,
        timeStamp: Double
    ): Boolean {
        val waitKey = queueType + WAIT_QUEUE

        return reactiveRedisTemplate.opsForZSet()
            .add(waitKey, userId, timeStamp)
            .awaitSingle()
    }

    // 대기열 또는 참가열에 사용자가 존재하는지 병렬 조회
    private suspend fun isUserInQueue(
        queueType: String,
        userId: String,
    ): Boolean = coroutineScope {
        val waitRankDeferred = async { getWaitQueueRank(queueType, userId) }
        val allowRankDeferred = async { getAllowQueueRank(queueType, userId) }

        waitRankDeferred.await() >= 0L || allowRankDeferred.await() >= 0L
    }

    // 대기열에서 사용자 순위 조회 (존재하지 않으면 -1L)
    suspend fun getWaitQueueRank(queueType: String, userId: String): Long =
        getQueueRank("$queueType$WAIT_QUEUE", userId)

    // 참가열에서 사용자 순위 조회 (존재하지 않으면 -1L)
    suspend fun getAllowQueueRank(queueType: String, userId: String): Long =
        getQueueRank("$queueType$ALLOW_QUEUE", userId)

    private suspend fun getQueueRank(key: String, userId: String): Long =
        reactiveRedisTemplate.opsForZSet()
            .rank(key, userId)
            .awaitFirstOrNull()
            ?.let { it + 1L }
            ?: -1L

    // 열에서의 사용자 삭제
    suspend fun cancelUser(queueType: String, userId: String): Boolean = coroutineScope {
        val waitDeferred = async { removeFromWaitQueue(queueType, userId) }
        val allowDeferred = async { removeFromAllowQueue(queueType, userId) }

        waitDeferred.await() || allowDeferred.await()
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
        val key = "$queueType$ALLOW_QUEUE"

        return reactiveRedisTemplate.opsForZSet()
            .remove(key, userId)
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

        val allowQueueKey = "$queueType$ALLOW_QUEUE"

        val expireAt = System.currentTimeMillis() + Duration.ofMinutes(10).toMillis()

        // 개별적으로 add 하는 것이 아닌 한 번에 하도록
        val tuples = poppedUsers.map { user ->
            DefaultTypedTuple(user.value.toString(), expireAt.toDouble())
        }.toSet()

        reactiveRedisTemplate.opsForZSet()
            .addAll(allowQueueKey, tuples)
            .awaitSingle()

        redisPublisher.publish(CHANNEL_NAME, queueType)


        return poppedUsers.size
    }

    // Nginx 도착 시각 + Redis 순번으로 score 생성
    // Nginx 타임스탬프가 없는 경우 (직접 호출 등) Redis 순번만 사용
    suspend fun generateScore(requestTimestamp: Double): Double {
        val seq = reactiveRedisTemplate.opsForValue()
                .increment("queue:seq")
                .awaitSingle()

        val timestampMs = (requestTimestamp * 1000).toLong()
        return timestampMs * 1000.0 + (seq % 1000)
    }

    // 타겟 페이지에 접속했을 때, 입장 가능 기간과 쿠키에 저장된 토큰의 유효성을 검증하는 로직
    // false 반환 : 잘못된 인증
    suspend fun isAllowTokenValid(
        queueType: String,
        userId: String,
        token: String
    ): Boolean = !(isAllowTokenExpired(queueType, userId) || isTokenMismatch(queueType, userId, token))

    // 참가열에서의 사용자 TTL 만료 여부 조회
    // 만료 시 true 반환
    suspend fun isAllowTokenExpired(
        queueType: String,
        userId: String
    ): Boolean {
        val key = "$queueType$ALLOW_QUEUE"

        val score = reactiveRedisTemplate.opsForZSet()
            .score(key, userId)
            .awaitSingleOrNull()
            ?: return true // 존재하지 않으면 만료로 간주하며, true 반환

        val now = System.currentTimeMillis().toDouble()

        return score < now
    }

    // 토큰의 유효성 판별 로직
    // 유효하지 않다면 true 반환
    private fun isTokenMismatch(
        queueType: String,
        userId: String,
        token: String
    ): Boolean = createAccessToken(queueType, userId) != token
}