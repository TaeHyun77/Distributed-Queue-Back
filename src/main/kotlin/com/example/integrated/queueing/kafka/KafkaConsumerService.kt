package com.example.integrated.queueing.kafka

import com.example.integrated.Loggable
import com.example.integrated.queueing.QueueService
import com.example.integrated.queueing.event.QueueEventPayload
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService(
    private val objectMapper: ObjectMapper,
    private val queueService: QueueService
): Loggable {

    @KafkaListener(topics = ["queueing-system"], groupId = "queue-event-group")
    fun consume(message: String) {
        try {
            val messageDto: KafkaMessageDto = objectMapper.readValue(message, KafkaMessageDto::class.java)

            queueService.sink.tryEmitNext(QueueEventPayload(messageDto.queueType))
            log.info{ "Kafka 이벤트 수신 - queueType: ${messageDto.queueType}" }

        } catch (e: Exception) {
            log.error{ "Kafka 메시지 consume 실패" }
        }
    }
}