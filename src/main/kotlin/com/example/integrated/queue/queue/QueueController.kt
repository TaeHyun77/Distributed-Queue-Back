package com.example.integrated.queue.queue

import com.example.integrated.queue.queue.dto.QueueRequest
import com.example.integrated.queue.queue.dto.RegisterResult
import com.example.integrated.util.Loggable
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpRequest
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.web.bind.annotation.*

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

        // Nginx가 설정한 X-Request-Timestamp 사용, 없으면 현재 시각
        val requestTimestamp = header.headers.getFirst("X-Request-Timestamp")
                ?.toDoubleOrNull()
                ?: (System.currentTimeMillis() / 1000.0)

        log.info { "대기열 등록 요청 : server=$serverName, userId=$userId, queueType=$queueType, timestamp=$requestTimestamp" }
        return queueService.registerUserToWaitQueue(queueType, userId, requestTimestamp)
    }

    // 대기열에서의 사용자 순위 반환
    @GetMapping("/get/rank")
    suspend fun getUserRank(
        @RequestParam queueType: String,
        @RequestParam userId: String,
        @RequestParam queueCategory: String
    ): Long = when (queueCategory) {
        "wait" -> queueService.getWaitQueueRank(queueType, userId)
        "allow" -> queueService.getAllowQueueRank(queueType, userId)
        else -> -1L
    }

    // 쿠키에 토큰 전달
    @GetMapping("/create/cookie")
    suspend fun issueAccessTokenCookie(
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