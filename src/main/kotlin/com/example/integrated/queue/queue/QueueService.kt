package com.example.integrated.queue.queue

import com.example.integrated.queue.kafka.service.KafkaProducerService
import com.example.integrated.queue.queue.dto.RegisterResult
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import com.example.integrated.util.*
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
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
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.util.*

@Service
class QueueService(
        @Value("\${queue.validation.key}")
        private val validationKey: String,

        private val kafkaProducerService: KafkaProducerService,
        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>
) : Loggable {

    // 대기열 등록
    suspend fun registerUserToWaitQueue(
            queueType: String,
            userId: String,
            requestTimestamp: Double
    ): RegisterResult {
        // 간단한 중복 체크 ( 최종 보장은 Consumer Lua에서 처리 )
        if (isAlreadyRegistered(queueType, userId)) {
            return RegisterResult.ALREADY_REGISTERED
        }

        kafkaProducerService.sendMessage(queueType, userId, requestTimestamp)
        return RegisterResult.SUCCESS
    }

    // 대기열 또는 참가열에 이미 존재하는지 확인
    // 경쟁 상태가 발생할 수 있지만, 최종 중복 방지는 Consumer Lua에서 원자적으로 처리하고자 합니다.
    private suspend fun isAlreadyRegistered(queueType: String, userId: String): Boolean {
        val inWait = getWaitQueueRank(queueType, userId) > 0
        val inAllow = getAllowQueueRank(queueType, userId) > 0
        return inWait || inAllow
    }

    // 대기열에서 사용자 순위 조회 ( 존재하지 않으면 -1L )
    suspend fun getWaitQueueRank(queueType: String, userId: String): Long =
            getQueueRank("$queueType$WAIT_QUEUE", userId)

    // 참가열에서 사용자 순위 조회
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

    // 참가열로 이동하면 유효성 인증을 위해 토큰을 생성하여 쿠키에 저장
    suspend fun issueAccessTokenCookie(
            queueType: String,
            userId: String,
            response: ServerHttpResponse
    ): ResponseEntity<String> {
        if (!isAllowTokenExpired(queueType, userId)) {
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

    // 타겟 페이지에 접속했을 때, 입장 가능 기간과 쿠키에 저장된 토큰의 유효성을 검증
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
                ?: return true

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