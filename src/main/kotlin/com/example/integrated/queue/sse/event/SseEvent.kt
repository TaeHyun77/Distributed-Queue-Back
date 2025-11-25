package com.example.integrated.queue.sse.event

sealed class SseEvent {
    abstract val event: String
}