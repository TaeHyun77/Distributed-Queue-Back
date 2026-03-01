package com.example.integrated.queue.kafka

import com.example.integrated.util.Loggable
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.future.await
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class KafkaProducerService (

    @Value("\${queue.event.topic.name}")
    private var queueEventTopicName: String,

    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
): Loggable {

    suspend fun sendMessage(
        queueType: String,
        userId: String,
        timeStamp: Double,
        requestedAt: Long
    ): Boolean {
        try {
            val message = KafkaMessage(queueType, userId, timeStamp, requestedAt)
            val jsonMessage = objectMapper.writeValueAsString(message)

            kafkaTemplate.send(queueEventTopicName, jsonMessage)
                .await()

            log.info { "Kafka produce 성공" }
            return true

        } catch (e: Exception) {
            log.error(e) { "Kafka produce 실패" }
            return false
        }
    }
}