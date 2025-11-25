package com.example.integrated.queue.sse.event

data class UpdateSseEvent (
    override val event: String = "update",
    val rank: Long
): SseEvent()