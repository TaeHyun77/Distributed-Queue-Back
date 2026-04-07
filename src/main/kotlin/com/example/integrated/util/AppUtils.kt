package com.example.integrated.util

import com.fasterxml.jackson.databind.ObjectMapper
import io.lettuce.core.RedisCommandTimeoutException
import io.lettuce.core.RedisConnectionException
import org.springframework.data.redis.RedisConnectionFailureException


const val WAIT_QUEUE: String = ":user-queue:wait"
const val ALLOW_QUEUE: String = ":user-queue:allow"
const val CHANNEL_NAME = "queueing_system"
const val ACTIVE_QUEUE_KEY = "active-allow-queue"
const val SCHEDULING_KEY = "scheduling-key"

inline fun <reified T> ObjectMapper.readValueFromJson(json: String): T {
    return readValue(json, T::class.java)
}

fun isRedisConnectionException(e: Throwable): Boolean =
    e is RedisCommandTimeoutException ||
    e is RedisConnectionException ||
    e is RedisConnectionFailureException ||
    e.cause?.let { isRedisConnectionException(it) } == true