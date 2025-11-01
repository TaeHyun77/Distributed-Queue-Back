package com.example.integrated.queueing.kafka

import com.example.integrated.queueing.event.QueueEventPayload
import com.example.integrated.queueing.event.SseEventService
import com.example.integrated.queueing.queue.QueueService
import com.example.integrated.util.Loggable
import com.example.integrated.util.readValueFromJson
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService(
    @Value("\${SERVER_NAME}")
    private val serverName: String? = null,

    private val objectMapper: ObjectMapper
): Loggable {

    // @KafkaListener에 groupId를 명시하지 않으면, application.properties에 정의된 consumer.group-id 값이 자동으로 적용됨
    @KafkaListener(topics = ["queueing-system"])
    fun broadcastQueueEvent(message: String, record: ConsumerRecord<String, String>) {

        try {
            // objectMapper.readValue()는 Java 라이브러리인 Jackson의 메서드이기 때문에 java 객체로 변환
            val messageDto: QueueBroadcastDto = objectMapper.readValueFromJson<QueueBroadcastDto>(message)
            val queueType: String = messageDto.queueType

            log.info {"Kafka consume - queueType: $queueType, topic: ${record.topic()}, partition : ${record.partition()}, consume-server-name: $serverName"}

            SseEventService.sink.tryEmitNext(QueueEventPayload(queueType))

        } catch (e: Exception) {
            log.error(e) { "Kafka 메시지 broadcast 실패" };
        }
    }
}