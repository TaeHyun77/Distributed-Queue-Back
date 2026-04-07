package com.example.integrated.queue.sse.event

data class RetrySseEvent(
    override val event: String = "retry",
    val message: String
) : SseEvent()
