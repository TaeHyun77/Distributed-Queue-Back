package com.example.integrated.queue.queue.dto

data class QueueChangePayload(
    val queueType: String,
    val event: String,
    val ids: List<String>
)
