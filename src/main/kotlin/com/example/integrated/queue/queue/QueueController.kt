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
    @PostMapping("/register/{idempotencyKey}")
    suspend fun registerUser(
        @RequestBody request: QueueRequest,
        @PathVariable("idempotencyKey") idempotencyKey: String
    ): RegisterResult {

        val queueType = request.queueType
        val userId = request.userId

        log.info { "queue-server-name: $serverName" }
        log.info { "대기열 등록 사용자 정보 , userId: $userId, queueType: $queueType , idempotencyKey: $idempotencyKey" }

        return queueService.registerUserToWaitQueue(queueType, userId, idempotencyKey)
    }

    // 쿠키에 토큰 전달
    @GetMapping("/createCookie")
    suspend fun sendCookie(
        @RequestBody request: QueueRequest,
        response: ServerHttpResponse
    ): ResponseEntity<String> = queueService.sendCookie(request.queueType, request.userId, response)

    // 토큰의 유효성 판단
    @GetMapping("/isValidateToken/{token}")
    suspend fun isAccessTokenValid(
        @RequestBody request: QueueRequest,
        @PathVariable("token") token: String
    ): Boolean = queueService.isAccessTokenValid(request.queueType, request.userId, token)

    // 대기열 or 참가열 등록 취소
    @DeleteMapping("/cancel/{queueCategory}")
    suspend fun cancelReserve(
        @RequestBody request: QueueRequest,
        @PathVariable("queueCategory") queueCategory: String
    ): Boolean = queueService.cancelUser(request.queueType, request.userId, queueCategory)
}