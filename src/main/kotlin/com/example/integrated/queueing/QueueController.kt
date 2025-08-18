package com.example.integrated.queueing

import com.example.integrated.util.Loggable
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import com.example.integrated.util.logScope
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.web.bind.annotation.*
import java.time.Instant

@RequestMapping("/queue")
@RestController
class QueueController (
    private val queueService: QueueService
): Loggable {

    @Value("\${SERVER_NAME:unknown}")
    private val serverName: String? = null

    @PostMapping("/register/{userId}/{queueType}")
    suspend fun registerUser(
        @PathVariable("userId") userId: String,
        @PathVariable("queueType") queueType: String,
        request: ServerHttpRequest
    ): ResponseEntity<String> {

        log.info { "server : $serverName" }

        val now = Instant.now()
        val enterTimestamp = now.epochSecond * 1_000_000_000L + now.nano

        val idempotencyKey: String = request.headers["Idempotency-key"]?.firstOrNull()
            ?: throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.NOT_EXIST_IN_HEADER_IDEMPOTENCY_KEY)

        log.info { "등록 사용자 정보 , userId: $userId, queueType: $queueType enterTimestamp: $enterTimestamp idempotencyKey : $idempotencyKey" }

        val result = queueService.register(userId, queueType, enterTimestamp, idempotencyKey)

        return result
    }

    // 쿠키에 토큰 전달
    @GetMapping("/createCookie")
    suspend fun sendCookie(
        @RequestParam(name = "userId") userId: String,
        @RequestParam(name = "performanceId") performanceId: Long,
        response: ServerHttpResponse
    ): ResponseEntity<String> {

        return queueService.sendCookie(userId, performanceId, response)
    }

    // 토큰의 유효성 판단
    @GetMapping("/isValidateToken")
    suspend fun isAccessTokenValid(
        @RequestParam(name = "userId") userId: String,
        @RequestParam(name = "performanceId") performanceId: Long,
        @RequestParam(name = "token") token: String
        ): Boolean {

        return queueService.isAccessTokenValid(userId, performanceId, token)
    }

    // 대기열 or 참가열 등록 취소
    @DeleteMapping("/cancel")
    suspend fun cancelReserve(
        @RequestParam(name = "userId") userId: String,
        @RequestParam(name = "queueType") queueType: String,
        @RequestParam(name = "queueCategory") queueCategory: String
    ): ResponseEntity<String> {

        return queueService.cancelUser(userId, queueType.split(":")[0], queueCategory)
    }
}