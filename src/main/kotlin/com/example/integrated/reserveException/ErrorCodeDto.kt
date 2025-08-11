package com.example.integrated.reserveException

import org.springframework.http.ResponseEntity

data class ErrorCodeDto(
    val code: String,
    val message: String,
    val detail: String?
) {
    companion object {

        fun toException(ex: ReserveException): ResponseEntity<ErrorCodeDto> {
            val errorType: ErrorCode = ex.errorCode
            val detail: String? = ex.details

            return ResponseEntity
                .status(ex.status)
                .body(
                    ErrorCodeDto(
                        code = errorType.errorCode,
                        message = errorType.message,
                        detail = detail
                    )
                )
        }
    }
}