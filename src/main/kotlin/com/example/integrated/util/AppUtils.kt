package com.example.integrated.util

import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.coroutineContext

const val WAIT_QUEUE: String = ":user-queue:wait"
const val ALLOW_QUEUE: String = ":user-queue:allow"
const val CHANNEL_NAME = "queueing_system"
const val ACTIVE_QUEUE_KEY = "active-allow-queue"
const val SCHEDULING_KEY = "scheduling-key"

inline fun <reified T> ObjectMapper.readValueFromJson(json: String): T {
    return readValue(json, T::class.java)
}