package com.example.integrated.util

import com.example.integrated.queueing.event.QueueEventPayload
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.TickerMode
import kotlinx.coroutines.channels.produce
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseCookie
import org.springframework.http.server.reactive.ServerHttpRequest
import reactor.core.publisher.Sinks
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.coroutineContext

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

suspend fun logScope(name: String, scope: CoroutineScope? = null) {
    val ctx = scope?.coroutineContext ?: coroutineContext

    // Job을 제외한 Context (부모 스코프 기준 확인용)
    // Job을 포함 한다면 각 코루틴은 독립적인 Job을 가지기에 항상 달라지므로 Job을 제외하고 계산해야 함
    val ctxWithoutJob = ctx.minusKey(Job)

    println(
        """
        [$name]
        Scope(hashWithoutJob): ${ctxWithoutJob.hashCode()}
        Full Scope(hash): ${ctx.hashCode()}
        Job: ${ctx[Job]}
        Dispatcher: ${ctx[ContinuationInterceptor]}
        CoroutineName: ${ctx[CoroutineName]}
        Thread: ${Thread.currentThread().name}
        """.trimIndent()
    )
}