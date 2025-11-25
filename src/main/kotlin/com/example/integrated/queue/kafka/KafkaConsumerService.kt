package com.example.integrated.queue.kafka

import com.example.integrated.util.Loggable
import com.example.integrated.util.WAIT_QUEUE
import com.example.integrated.util.readValueFromJson
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.awaitSingle
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService(
    @Value("\${SERVER_NAME}")
    private val serverName: String? = null,

    private val objectMapper: ObjectMapper,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
): Loggable {

    // "queueing-system" 토픽으로 produce 된 이벤트를 consume
    // group-id를 지정하지 않으면, spring.kafka.consumer.group-id 설정 값으로 자동 적용됨
    @KafkaListener(topics = ["queueing-system"])
    suspend fun broadcastQueueEvent(message: String) {
        val receiveMessage = objectMapper.readValueFromJson<KafkaMessage>(message)

        // 삽입 성공 시 true, 실패 시 false 반환
        val result = reactiveRedisTemplate.opsForZSet()
            .add(receiveMessage.queueType + WAIT_QUEUE, receiveMessage.userId, receiveMessage.timeStamp)
            .awaitSingle()

        if (!result) {
            log.warn {"consume - ZSet 삽입 실패 ⇒ userId: queueType: ${receiveMessage.queueType}, ${receiveMessage.userId}"}
        }
    }
}