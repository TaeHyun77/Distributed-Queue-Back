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
import org.springframework.core.io.ClassPathResource
import org.springframework.data.redis.core.DefaultTypedTuple
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.script.RedisScript
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
        private val duplicationCheckService: DuplicationCheckService,
        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>
): Loggable {

    companion object {
        // Lua 스크립트 : 대기열/참가열 존재 확인 + 등록 진행 중 플래그 + 밀리초별 독립 Score 생성을 원자적으로 수행
        // 반환 값: -1 ( 대기열 존재 ), -2 ( 참가열 존재 ), -3 ( 등록 진행 중 ), 양수 ( 생성된 score )
        private val GENERATE_SCORE_SCRIPT: RedisScript<Long> = RedisScript.of(
            ClassPathResource("scripts/generate-score.lua"),
            Long::class.java
        )
    }

    // 대기열 등록
    suspend fun registerUserToWaitQueue(
        queueType: String,
        userId: String,
        requestKey: String,
        requestTimestamp: Double
    ): RegisterResult {
        // 1. 중복된 요청인지 확인
        if (duplicationCheckService.isDuplicate(requestKey)) {
            return RegisterResult.DUPLICATE_REQUEST
        }

        // 2. "대기열/참가열 존재 확인"과 "Score 생성"을 원자적으로 수행 ( Lua 스크립트 )
        // 원자적으로 수행함으로써 race condition을 방지
        val timestampMs = (requestTimestamp * 1000).toLong()

        return when (val score = executeScoreGeneration(queueType, userId, timestampMs)) {
            -1L, -2L, -3L -> RegisterResult.ALREADY_REGISTERED
            else -> {
                // 3. 카프카로 대기열 삽입 이벤트 전달
                kafkaProducerService.sendMessage(queueType, userId, score.toDouble(), timestampMs)
                RegisterResult.SUCCESS
            }
        }
    }

    // 대기열 or 참가열 존재 확인 + 등록 진행 중 플래그 + Score 생성 ( 원자적 진행 )
    private suspend fun executeScoreGeneration(
        queueType: String,
        userId: String,
        timestampMs: Long
    ): Long {
        val keys = listOf(
            "$queueType$WAIT_QUEUE",    // KEYS[1] : 대기열 키
            "$queueType$ALLOW_QUEUE",   // KEYS[2] : 참가열 키
            "queue:seq:$timestampMs",   // KEYS[3] : 밀리초별 카운터 키
            "registering:$queueType:$userId"    // KEYS[4] : 등록 진행 중 플래그
        )
        val args = listOf(userId, timestampMs.toString())

        return reactiveRedisTemplate.execute(GENERATE_SCORE_SCRIPT, keys, args)
            .next()
            .awaitSingle()
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

    // 대기열에서 사용자 순위 조회 ( 존재하지 않으면 -1L )
    suspend fun getWaitQueueRank(queueType: String, userId: String): Long =
        getQueueRank("$queueType$WAIT_QUEUE", userId)

    // 참가열에서 사용자 순위 조회 ( 존재하지 않으면 -1L )
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

    /* 토큰을 쿠키에 저장
    * 참가열로 이동하면 유효성 인증을 위해 토큰을 생성하여 쿠키에 저장합니다.
    * 이후 타겟 페이지로 이동했을 때, 쿠키에 저장된 토큰과 서버에서 생성한 토큰을 비교하여 유효성을 검증합니다.
    */
    suspend fun issueAccessTokenCookie(
        queueType: String,
        userId: String,
        response: ServerHttpResponse
    ): ResponseEntity<String> {
        // 참가열에 존재하는지 확인
        if (isAllowTokenExpired(queueType, userId)) {
            val encodedName = URLEncoder.encode(userId, StandardCharsets.UTF_8)
            val cookieName = "reserve-user-access-cookie-$encodedName"

            val token = createAccessToken(queueType, userId)

            val responseCookie = ResponseCookie.from(cookieName, token)
                .path("/")
                .maxAge(Duration.ofSeconds(600))
                .build()

            response.addCookie(responseCookie)

            return ResponseEntity.ok("쿠키 발급 완료")
        } else {
            throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.FAILED_TO_STORE_TOKEN_IN_COOKIE)
        }
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

    // 타겟 페이지에 접속했을 때, 입장 가능 기간과 쿠키에 저장된 토큰의 유효성을 검증하는 로직
    // false 반환 : 잘못된 인증
    suspend fun isAllowTokenValid(
        queueType: String,
        userId: String,
        token: String
    ): Boolean = !(isAllowTokenExpired(queueType, userId) || isTokenMismatch(queueType, userId, token))

    // 참가열에서의 사용자 TTL 만료 여부 조회, 만료 시 true 반환
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

    // 토큰의 유효성 판별 로직, 유효하지 않다면 true 반환
    private fun isTokenMismatch(
        queueType: String,
        userId: String,
        token: String
    ): Boolean = createAccessToken(queueType, userId) != token
}
