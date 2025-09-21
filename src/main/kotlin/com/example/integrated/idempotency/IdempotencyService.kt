package com.example.integrated.idempotency

import com.example.integrated.util.Loggable
import com.example.integrated.reserveException.ReserveException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.reactive.*
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.withContext
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class IdempotencyService(
    private val idempotencyRepository: IdempotencyRepository,
) : Loggable {

    suspend fun execute(
        key: String,
        url: String,
        method: String,
        process: suspend () -> String
    ): ResponseEntity<String> {

        val now = LocalDateTime.now()

        val idempotency = idempotencyRepository.findByIdempotencyKey(key).awaitFirstOrNull()

        // 이미 존재하고 유효 기간이 지나지 않은 경우
        if (idempotency != null && idempotency.expires_at.isAfter(now) && idempotency.responseBody == "REGISTERED") {
            log.info { "동일한 Idempotent 요청 감지됨 - 저장된 이전 응답 반환" }
            log.info { "⇒ 이미 대기열에 등록된 사용자입니다." }

            return ResponseEntity
                .status(idempotency.statusCode)
                .body("이미 대기열에 등록된 사용자입니다.")
        }

        // 키가 없거나 유효기간 지난 경우 → 새로 처리
        return try {

            val successMessage = process()

            val successIdempotency = Idempotency(
                idempotencyKey = key,
                url = url,
                httpMethod = method,
                responseBody = successMessage,
                statusCode = 200,
                expires_at = now.plusMinutes(10)
            )

            // 단순히 save()만 호출하면 DB에 저장 요청이 날아가지 않음
            idempotencyRepository.save(successIdempotency).awaitSingleOrNull()
            log.info { "멱등성 키 저장 (성공 요청) - key: $key, message: $successMessage" }

            ResponseEntity
                .status(successIdempotency.statusCode)
                .body(successIdempotency.responseBody)

        } catch (e: ReserveException) {
            val failedIdempotency = Idempotency(
                idempotencyKey = key,
                url = url,
                httpMethod = method,
                responseBody = e.errorCode.name,
                statusCode = e.status.value(),
                expires_at = now.plusMinutes(10)
            )

            idempotencyRepository.save(failedIdempotency).awaitSingleOrNull()
            log.info { "멱등성 키 저장 (실패 요청) - key: $key, message: ${e.errorCode.name}" }

            ResponseEntity
                .status(failedIdempotency.statusCode)
                .body(failedIdempotency.responseBody)
        }
    }
}