package com.example.integrated.util

import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseCookie
import org.springframework.http.server.reactive.ServerHttpRequest

const val WAIT_QUEUE: String = ":user-queue:wait"
const val ALLOW_QUEUE: String = ":user-queue:allow"
const val ACCESS_TOKEN: String = ":user-access:"
const val TOKEN_TTL_INFO: String = "reserve:USERS-TTL:INFO";

fun createCookie(key: String, value: String): ResponseCookie {
    return ResponseCookie.from(key, value)
        .maxAge(12 * 60 * 60)
        .path("/")
        .build()
}

fun parsingToken(request: ServerHttpRequest): String {

    val authorization = request.headers.getFirst("Authorization")
        ?: throw ReserveException(HttpStatus.UNAUTHORIZED, ErrorCode.NOT_EXIST_AUTHORIZATION_IN_HEADER)

    if (!authorization.startsWith("Bearer ")) {
        throw ReserveException(HttpStatus.UNAUTHORIZED, ErrorCode.NOT_EXIST_AUTHORIZATION_IN_HEADER)
    }

    val token = authorization.substring(7)

    return token
}

