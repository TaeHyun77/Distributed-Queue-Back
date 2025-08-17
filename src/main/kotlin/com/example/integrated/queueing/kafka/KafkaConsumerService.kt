package com.example.integrated.queueing.kafka

import com.example.integrated.Loggable
import com.example.integrated.redis.subscribe.RedisPublisher
import com.example.integrated.util.CHANNEL_NAME
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService(
    private val objectMapper: ObjectMapper,
    private val redisPublisher: RedisPublisher
): Loggable {

    // @KafkaListener에 groupId를 명시하지 않으면, application.properties에 정의된 consumer.group-id 값이 자동으로 적용됨
    @KafkaListener(topics = ["queueing-system"], groupId = "queue-event-group")
    fun consume(message: String, record: ConsumerRecord<String, String>) {

        val data = record.value()
        val topic = record.topic()
        val partition = record.partition()
        val offset = record.offset()

        log.info { "topic: $topic, partition: $partition, offset: $offset, data: $data" }

        try {
            val messageDto: KafkaMessageDto = objectMapper.readValue(message, KafkaMessageDto::class.java)

            log.info { "queueType : ${messageDto.queueType} , 실행" }
            redisPublisher.publish(CHANNEL_NAME, messageDto.queueType)
            // queueService.sink.tryEmitNext(QueueEventPayload(messageDto.queueType))

            log.info{ "Kafka 이벤트 수신 - queueType: ${messageDto.queueType}" }
        } catch (e: Exception) {
            log.error{ "Kafka 메시지 consume 실패" }
        }
    }
}