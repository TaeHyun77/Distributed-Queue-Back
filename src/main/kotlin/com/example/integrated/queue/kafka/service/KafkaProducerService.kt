package com.example.integrated.queue.kafka.service

import com.example.integrated.queue.kafka.dto.KafkaMessageDto
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import com.example.integrated.util.Loggable
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.future.await
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import kotlin.coroutines.cancellation.CancellationException

@Service
class KafkaProducerService(

        @Value("\${queue.event.topic.name}")
        private val queueTopic: String,

        private val kafkaTemplate: KafkaTemplate<String, String>,
        private val objectMapper: ObjectMapper
) : Loggable {

    suspend fun sendMessage(
            queueType: String,
            userId: String,
            timestamp: Double,
    ) {
        val message = KafkaMessageDto(queueType, userId, timestamp)
        val jsonMessage = objectMapper.writeValueAsString(message)

        try {
            kafkaTemplate.send(queueTopic, userId, jsonMessage).await()
            log.info { "Kafka produce 성공: userId=$userId, queueType=$queueType" }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error(e) { "Kafka produce 실패: userId=$userId, queueType=$queueType" }
            throw ReserveException(HttpStatus.SERVICE_UNAVAILABLE, ErrorCode.FAIL_TO_PRODUCE_KAFKA)
        }
    }
}