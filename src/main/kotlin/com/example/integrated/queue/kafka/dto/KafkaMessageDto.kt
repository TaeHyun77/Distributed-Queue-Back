package com.example.integrated.queue.kafka.dto

data class KafkaMessageDto (
    val queueType: String,

    val userId: String,

    val timestamp: Double,
)