package com.example.integrated.queue.kafka

import com.example.integrated.util.Loggable
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
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

    fun sendMessage(
        queueType: String,
        userId: String,
        timeStamp: Double
    ) {
        try {
            val message = KafkaMessage(queueType, userId, timeStamp)

            val jsonMessage = objectMapper.writeValueAsString(message)

            kafkaTemplate.send(queueEventTopicName, jsonMessage).whenComplete { _, ex ->
                ex?.let {
                    log.error { "Kafka 메시지 전송 실패 - Topic: $queueEventTopicName" }
                } ?: log.info { "Kafka 메시지 전송 성공 - Topic: $queueEventTopicName" }
            }

        } catch (e: JsonProcessingException) {
            log.error {"kafka produce 직렬화 실패"}
            throw e
        }
    }
}