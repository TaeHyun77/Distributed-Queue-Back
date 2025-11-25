package com.example.integrated.queue.kafka

data class KafkaMessage (
    val queueType: String,

    val userId: String,

    val timeStamp: Double
)