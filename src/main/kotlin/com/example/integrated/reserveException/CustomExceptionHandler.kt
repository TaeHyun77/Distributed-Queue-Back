package com.example.integrated.reserveException

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler

@ControllerAdvice
class CustomExceptionHandler {

    @ExceptionHandler(ReserveException::class)
    fun handleCustom400Exception(ex: ReserveException): ResponseEntity<ErrorCodeDto> =
        ErrorCodeDto.Companion.toException(ex)
}