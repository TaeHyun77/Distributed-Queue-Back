package com.example.integrated.queueing.kafka

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

    fun sendMessage(queueType: String) {
        try {
            val message = QueueBroadcastDto(queueType)
            val json = objectMapper.writeValueAsString(message)

            kafkaTemplate.send(queueEventTopicName, queueType, json).whenComplete { _, ex ->
                if (ex == null) {
                    log.info { "Kafka 메세지 전송 성공" }
                } else {
                    log.error {"Kafka 메세지 전송 실패" }
                }
            }
        } catch (e: JsonProcessingException) {
            log.error("직렬화 실패: {}", e.message)
        }
    }
}