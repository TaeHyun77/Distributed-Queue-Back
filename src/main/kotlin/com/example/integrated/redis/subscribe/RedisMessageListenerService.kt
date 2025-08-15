package com.example.integrated.redis.subscribe

import com.example.integrated.Loggable
import com.example.integrated.queueing.event.QueueEventPayload
import com.example.integrated.queueing.event.SseEventService
import com.example.integrated.util.CHANNEL_NAME
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.reactor.mono
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.stereotype.Service

@Service
class RedisMessageListenerService(
    private val redisMessageListenerContainer: ReactiveRedisMessageListenerContainer
): Loggable {

    @PostConstruct
    fun init() {
        receiveMessages(redisMessageListenerContainer, ChannelTopic(CHANNEL_NAME))
    }

    fun receiveMessages(
        messageListenerContainer: ReactiveRedisMessageListenerContainer,
        channel: ChannelTopic
    ) {
        val serializer = createChannelAndValueSerializer()

        // 특정 채널을 구독하고
        messageListenerContainer.receive(listOf(channel), serializer, serializer)
            .flatMap {
                val event = it.message

                mono {
                    SseEventService.sink.tryEmitNext(QueueEventPayload(event))
                }
            }
            .onErrorContinue { e, _ ->
                log.warn { "redis subscribe error: ${e.message}" }
            }
            .subscribe()
    }

    private fun createChannelAndValueSerializer(): RedisSerializationContext.SerializationPair<String> {
        return RedisSerializationContext.SerializationPair.fromSerializer(StringRedisSerializer())
    }
}