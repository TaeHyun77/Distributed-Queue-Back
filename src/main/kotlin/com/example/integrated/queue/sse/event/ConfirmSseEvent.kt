package com.example.integrated.queue.sse.event

data class ConfirmSseEvent (
    override val event: String = "confirmed",
    val userId: String

): SseEvent()