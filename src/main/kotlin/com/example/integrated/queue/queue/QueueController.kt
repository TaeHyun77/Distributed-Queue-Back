package com.example.integrated.queue.queue

import com.example.integrated.queue.queue.dto.QueueRequest
import com.example.integrated.queue.queue.dto.RegisterResult
import com.example.integrated.util.Loggable
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import jakarta.servlet.http.HttpServletResponse
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
    @PostMapping("/register")
    suspend fun registerUser(
        @RequestBody request: QueueRequest,
        header: ServerHttpRequest
    ): RegisterResult {

        val queueType = request.queueType
        val userId = request.userId

        val idempotencyKey = header.headers["idempotency-key"]
            ?.firstOrNull()
            ?: throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.NOT_EXIST_IN_HEADER_IDEMPOTENCY_KEY)

        log.info { "queue-server-name: $serverName" }
        log.info { "대기열 등록 사용자 정보 , userId: $userId, queueType: $queueType , idempotencyKey: $idempotencyKey" }

        return queueService.registerUserToWaitQueue(queueType, userId, idempotencyKey)
    }

    // 대기열에서의 사용자 순위 반환
    @GetMapping("/get/rank")
    suspend fun getUserRank(
        @RequestParam queueType: String,
        @RequestParam userId: String,
        @PathVariable("queueCategory") queueCategory: String
    ): Long {
        return queueService.getUserRank(queueType, userId, queueCategory)
    }


    // 쿠키에 토큰 전달
    @GetMapping("/create/cookie")
    fun issueAccessTokenCookie(
        @RequestParam queueType: String,
        @RequestParam userId: String,
        response: ServerHttpResponse
    ): ResponseEntity<String> = queueService.issueAccessTokenCookie(queueType, userId, response)

    // 토큰의 유효성 판단
    @PostMapping("/isValidateToken/{token}")
    suspend fun isAccessTokenValid(
        @RequestBody request: QueueRequest,
        @PathVariable("token") token: String
    ): Boolean =
        queueService.isAllowTokenValid(request.queueType, request.userId, token)

    // 대기열 or 참가열 등록 취소
    @PostMapping("/cancel")
    suspend fun cancelReserve(
        @RequestBody request: QueueRequest,
    ): Boolean =
        queueService.cancelUser(request.queueType, request.userId)
}