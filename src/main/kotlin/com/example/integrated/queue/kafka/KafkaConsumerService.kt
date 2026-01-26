package com.example.integrated.queue.kafka

import com.example.integrated.queue.queue.QueueService
import com.example.integrated.queue.queue.QueueToAllowTrigger
import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.Loggable
import com.example.integrated.util.readValueFromJson
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService(
    private val objectMapper: ObjectMapper,
    private val queueService: QueueService,
    private val queueToAllowTrigger: QueueToAllowTrigger,
    private val redisPublisher: RedisPublisher
): Loggable {

    /*
    * "queueing-system" 토픽으로 produce 된 이벤트를 consume
    * group-id를 지정하지 않으면, spring.kafka.consumer.group-id 설정 값으로 자동 적용됨
    * */
    @KafkaListener(topics = ["\${queue.event.topic.name}"])
    suspend fun consumeMessage(message: String) {
        val consumeMessage = objectMapper.readValueFromJson<KafkaMessage>(message)

        val queueType = consumeMessage.queueType
        val userId = consumeMessage.userId
        val timeStamp = consumeMessage.timeStamp

        val activated = queueService.enqueueAndActivateIfFirst(queueType, userId, timeStamp)

        if (activated) {
            queueToAllowTrigger.addActiveQueue(queueType)
        }

        redisPublisher.publish(CHANNEL_NAME, queueType)
    }
}