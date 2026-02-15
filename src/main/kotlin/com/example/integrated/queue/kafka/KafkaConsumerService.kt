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
    @RetryableTopic( // Redis failover 시간(~15초)을 커버하도록 exponential backoff 적용, 최종 실패 시 DLT로 이동
        attempts = "6", // 첫 시도 1회 + 재시도 5회
        backoff = Backoff(delay = 1000, multiplier = 2.0, maxDelay = 12000), //
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
        autoCreateTopics = "true",
    )
    suspend fun consumeMessage(
        message: String
    ) {
        handleMessage(message)
    }

    private suspend fun handleMessage(message: String) {
        val consumeMessage = objectMapper.readValueFromJson<KafkaMessage>(message)

        queueService.enqueueToWaitQueue(
            consumeMessage.queueType,
            consumeMessage.userId,
            consumeMessage.timeStamp
        )

        // set이므로 중복 추가해도 상관 없음 - 매번 호출하여 활성화 보장하도록 하기
        queueToAllowScheduler.addActiveQueue(consumeMessage.queueType)

        redisPublisher.publish(CHANNEL_NAME, consumeMessage.queueType)
    }

    @DltHandler
    fun handleDltMessage(
        @Header(KafkaHeaders.RECEIVED_TOPIC) dltTopicName: String,
        @Header(KafkaHeaders.EXCEPTION_MESSAGE) errorMessage: String?,
    ) {
        try {
            log.error {
                "DLT 토픽에 메세지 저장 , DLT Topic: $dltTopicName , Error Message: $errorMessage"
            }
        } catch (e: Exception) {
            log.error(e) { "DLT 메시지 처리 중 에러 발생" }
        }
    }
}
