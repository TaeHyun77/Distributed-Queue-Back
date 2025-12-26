package com.example.integrated.queue.kafka

import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.Loggable
import com.example.integrated.util.WAIT_QUEUE
import com.example.integrated.util.readValueFromJson
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService(
    private val objectMapper: ObjectMapper,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
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

        val key = queueType + WAIT_QUEUE

        // 삽입 성공 시 true, 실패 시 false 반환
        val isInserted = reactiveRedisTemplate.opsForZSet()
            .add(key,userId, timeStamp)
            .awaitSingle()

        if (isInserted) {
            redisPublisher.publish(CHANNEL_NAME, queueType)
        } else {
            log.warn {"consume - ZSet 삽입 실패 ⇒ userId: queueType: ${consumeMessage.queueType}, ${consumeMessage.userId}"}
        }
    }
}