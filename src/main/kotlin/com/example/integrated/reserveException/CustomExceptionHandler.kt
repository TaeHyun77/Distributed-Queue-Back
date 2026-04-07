package com.example.integrated.reserveException

import com.example.integrated.util.Loggable
import io.lettuce.core.RedisCommandTimeoutException
import io.lettuce.core.RedisConnectionException
import org.springframework.data.redis.RedisConnectionFailureException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler

@ControllerAdvice
class CustomExceptionHandler : Loggable {

    @ExceptionHandler(ReserveException::class)
    fun handleReserveException(ex: ReserveException): ResponseEntity<ErrorCodeDto> =
        ErrorCodeDto.Companion.toException(ex)

    @ExceptionHandler(
        RedisCommandTimeoutException::class,
        RedisConnectionException::class,
        RedisConnectionFailureException::class
    )
    fun handleRedisException(ex: Exception): ResponseEntity<ErrorCodeDto> {
        log.warn { "Redis 연결 실패 (failover 가능성): ${ex.message}" }

        val errorCode = ErrorCode.REDIS_UNAVAILABLE
        return ResponseEntity
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .header("Retry-After", "3")
            .body(
                ErrorCodeDto(
                    code = errorCode.errorCode,
                    message = errorCode.message,
                    detail = null
                )
            )
    }
}
