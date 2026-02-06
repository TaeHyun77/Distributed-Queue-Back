package com.example.integrated.queue.kafka

import com.example.integrated.queue.queue.QueueService
import com.example.integrated.queue.queue.QueueToAllowScheduler
import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.Loggable
import com.example.integrated.util.readValueFromJson
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.retrytopic.RetryTopicHeaders
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.retry.annotation.Backoff
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService(
    private val objectMapper: ObjectMapper,
    private val queueService: QueueService,
    private val queueToAllowScheduler: QueueToAllowScheduler,
    private val redisPublisher: RedisPublisher
): Loggable {

    /*
    * "queueing-system" 토픽으로 produce 된 이벤트를 consume
    * group-id를 지정하지 않으면, spring.kafka.consumer.group-id 설정 값으로 자동 적용됨
    * */
    @KafkaListener(
        topics = ["\${queue.event.topic.name}"],
        containerFactory = "kafkaListenerContainerFactory"
    )
    @RetryableTopic( // 4번까지는 재시도 후 그래도 실패한다면 DLT 토픽으로 작업을 이동
        attempts = "4",
        backoff = Backoff(delay = 3000),
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC, // 이를 통해 재시도 토픽을 한 개만 생성하도록 함
        autoCreateTopics = "true", // retry 토픽을 자동으로 생성, 이 설정이 없다면 토픽을 미리 생성해둬야 함
    )
    suspend fun consumeMessage(
        message: String,
        @Header(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS) attempt: Int
    ) {
        handleMessage(message)
    }

    private suspend fun handleMessage(message: String) {
        val consumeMessage = objectMapper.readValueFromJson<KafkaMessage>(message)

        val activated = queueService.enqueueAndActivateIfFirst(
            consumeMessage.queueType,
            consumeMessage.userId,
            consumeMessage.timeStamp
        )

        if (activated) {
            queueToAllowScheduler.addActiveQueue(consumeMessage.queueType)
        }

        redisPublisher.publish(CHANNEL_NAME, consumeMessage.queueType)
    }

    @DltHandler
    fun handleDltMessage(
        message: String,
        @Header(KafkaHeaders.RECEIVED_TOPIC) dltTopicName: String,
        @Header(KafkaHeaders.EXCEPTION_MESSAGE) errorMessage: String?,
    ) {
        try {
            val consumeMessage = objectMapper.readValueFromJson<KafkaMessage>(message)

            log.error {
                """
                DLT Topic: $dltTopicName
                Queue Type: ${consumeMessage.queueType}
                User ID: ${consumeMessage.userId}
                Timestamp: ${consumeMessage.timeStamp}
                Error Message: $errorMessage
                """.trimIndent()
            }

        } catch (e: Exception) {
            log.error(e) { "DLT 메시지 처리 중 에러 발생" }
        }
    }
}
