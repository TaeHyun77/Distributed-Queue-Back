package com.example.integrated.queueing.queue

import com.example.integrated.util.Loggable
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import jakarta.servlet.http.HttpServletRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.time.Instant

@RequestMapping("/queue")
@RestController
class QueueController (
    private val queueService: QueueService,

    @Value("\${SERVER_NAME}")
    private val serverName: String? = null

): Loggable {

    @PostMapping("/register/{userId}/{queueType}")
    suspend fun registerUser(
        @PathVariable("userId") userId: String,
        @PathVariable("queueType") queueType: String,
        request: ServerHttpRequest
    ): RegisterResult {

        log.info { "server name: $serverName" }

        val now = Instant.now()
        // 초 값 → 마이크로초 , 나노초 값 → 마이크로초
        val enterTimestamp = now.epochSecond * 1_000_000L + now.nano / 1_000L

        val idempotencyKey: String = request.headers["idempotencyKey"]?.firstOrNull()
            ?: throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.NOT_EXIST_IN_HEADER_IDEMPOTENCY_KEY)

        log.info { "등록 사용자 정보 , userId: $userId, queueType: $queueType enterTimestamp: $enterTimestamp idempotencyKey: $idempotencyKey" }

        return queueService.registerUserToWaitQueue(userId, queueType, enterTimestamp, idempotencyKey)
    }

    // 쿠키에 토큰 전달
    @GetMapping("/createCookie")
    suspend fun sendCookie(
        @RequestParam(name = "userId") userId: String,
        @RequestParam(name = "queueType") queueType: String,
        response: ServerHttpResponse
    ): ResponseEntity<String> {

        return queueService.sendCookie(userId, queueType, response)
    }

    // 토큰의 유효성 판단
    @GetMapping("/isValidateToken")
    suspend fun isAccessTokenValid(
        @RequestParam(name = "userId") userId: String,
        @RequestParam(name = "queueType") queueType: String,
        @RequestParam(name = "token") token: String
    ): Boolean {

        return queueService.isAccessTokenValid(userId, queueType, token)
    }

    // 대기열 or 참가열 등록 취소
    @DeleteMapping("/cancel")
    suspend fun cancelReserve(
        @RequestParam(name = "userId") userId: String,
        @RequestParam(name = "queueType") queueType: String,
        @RequestParam(name = "queueCategory") queueCategory: String
    ): Boolean {

        return queueService.cancelUser(userId, queueType.split(":")[0], queueCategory)
    }
}