package com.example.integrated.queue.kafka

import com.example.integrated.queue.queue.QueueService
import com.example.integrated.queue.queue.QueueToAllowScheduler
import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.Loggable
import com.example.integrated.util.readValueFromJson
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.runBlocking
import org.springframework.kafka.annotation.KafkaListener
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
    @KafkaListener(topics = ["\${queue.event.topic.name}"])
    fun consumeMessage(message: String) {
        runBlocking {
            handle(message)
        }
    }

    suspend fun handle(message: String) {
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
}
