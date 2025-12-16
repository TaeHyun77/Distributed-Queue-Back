package com.example.integrated.queue.queue

import com.example.integrated.util.Loggable
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
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
    private val queueService: QueueService,

    @Value("\${SERVER_NAME}")
    private val serverName: String? = null

): Loggable {

    // 대기열에 사용자 등록
    @PostMapping("/register/{queueType}/{userId}")
    suspend fun registerUser(
        @PathVariable("queueType") queueType: String,
        @PathVariable("userId") userId: String,
        request: ServerHttpRequest
    ): RegisterResult {

        val idempotencyKey = request.headers["idempotencyKey"]?.firstOrNull()
            ?: throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.NOT_EXIST_IN_HEADER_IDEMPOTENCY_KEY)

        log.info { "queue-server-name: $serverName" }
        log.info { "대기열 등록 사용자 정보 , userId: $userId, queueType: $queueType , idempotencyKey: $idempotencyKey" }

        return queueService.registerUserToWaitQueue(queueType, userId, idempotencyKey)
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
        return queueService.cancelUser(userId, queueType, queueCategory)
    }
}