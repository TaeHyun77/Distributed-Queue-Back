package com.example.integrated.util

import com.example.integrated.queueing.event.QueueEventPayload
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.TickerMode
import kotlinx.coroutines.channels.produce
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseCookie
import org.springframework.http.server.reactive.ServerHttpRequest
import reactor.core.publisher.Sinks
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

const val WAIT_QUEUE: String = ":user-queue:wait"
const val ALLOW_QUEUE: String = ":user-queue:allow"
const val ACCESS_TOKEN: String = ":user-access:"
const val TOKEN_TTL_INFO: String = "reserve:USERS-TTL:INFO"
const val CHANNEL_NAME = "queueing_system"

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