package com.example.integrated.queue.kafka.dto

data class KafkaMessageDto (
    val queueType: String,

    val userId: String,

    val timeStamp: Double,

    val requestedAt: Long = 0L
)