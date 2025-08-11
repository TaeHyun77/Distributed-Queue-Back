package com.example.integrated.reserveException

import org.springframework.http.HttpStatus

class ReserveException(
    val status: HttpStatus,
    val errorCode: ErrorCode,
    val details: String? = null

): RuntimeException() {
}