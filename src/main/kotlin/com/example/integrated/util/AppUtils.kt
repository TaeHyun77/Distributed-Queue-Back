package com.example.integrated.util

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.coroutineContext

const val WAIT_QUEUE: String = ":user-queue:wait"
const val ALLOW_QUEUE: String = ":user-queue:allow"
const val ACCESS_TOKEN: String = ":user-access:"
const val TOKEN_TTL_INFO: String = "reserve:USERS-TTL:INFO"
const val IDEMPOTENCE_TTL: String = "idempotency-key-ttl"
const val CHANNEL_NAME = "queueing_system"

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

inline fun <reified T> ObjectMapper.readValueFromJson(json: String): T {
    return this.readValue(json, T::class.java)
}