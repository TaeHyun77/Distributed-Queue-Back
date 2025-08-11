package com.example.integrated.idempotency

import com.example.integrated.Loggable
import com.example.integrated.reserveException.ReserveException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class IdempotencyService(
    private val idempotencyRepository: IdempotencyRepository,
): Loggable {

    suspend fun execute(
        key: String,
        url: String,
        method: String,
        process: suspend () -> String
    ): ResponseEntity<String> = withContext(Dispatchers.IO) {

        val now = LocalDateTime.now()
        val idempotency = idempotencyRepository.findByIdempotencyKey(key)

        // 유효 기간이 지나지 않은 동일 요청이 있는 경우
        if (idempotency != null && idempotency.expires_at.isAfter(now)) {

            log.info { "동일한 Idempotent 요청 감지됨 - 저장된 이전 응답 반환" }
            log.info { "⇒ ${idempotency.responseBody}" }

            val idempotencyRes = IdempotencyResponse(
                statusCode = idempotency.statusCode,
                responseBody = idempotency.responseBody
            )

            return@withContext ResponseEntity
                .status(idempotencyRes.statusCode)
                .body(idempotencyRes.responseBody)
        }

        // 성공했을 때
        try {
            val successMessage = process()  // 성공 결과 메시지 or 예외

            idempotencyRepository.save(
                Idempotency(
                    idempotencyKey = key,
                    url = url,
                    httpMethod = method,
                    responseBody = successMessage,
                    statusCode = 200,
                    expires_at = now.plusMinutes(10)
                )
            )

            log.info{"멱등성 키 저장 (성공 요청) - key: $key, message: $successMessage"}

            return@withContext ResponseEntity
                .status(200)
                .body(successMessage)

        // 실패했을 때
        } catch (e: ReserveException) {

            val errorStatus = e.status.value()
            val errorCode = e.errorCode.name

            idempotencyRepository.save(
                Idempotency(
                    idempotencyKey = key,
                    url = url,
                    httpMethod = method,
                    responseBody = errorCode,
                    statusCode = errorStatus,
                    expires_at = now.plusMinutes(10)
                )
            )

            log.info{"멱등성 키 저장 (실패 요청) - key: $key, message: $errorCode"}

            return@withContext ResponseEntity
                .status(e.status)
                .body(errorCode)
        }
    }
}