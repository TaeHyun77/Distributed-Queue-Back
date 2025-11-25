package com.example.integrated.queue.sse.event

data class ErrorSseEvent(
    override val event: String = "error",
    val message: String
) : SseEvent()