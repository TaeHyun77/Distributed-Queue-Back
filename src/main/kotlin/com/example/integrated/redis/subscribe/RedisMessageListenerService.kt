package com.example.integrated.redis.subscribe

import com.example.integrated.util.Loggable
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
        val serializer = channelAndValueSerializer()

        // receive(...)는 Redis pub/sub 채널 Listener를 Publisher로 생성하여 메시지가 들어올 때마다 Reactor 체인을 통해 지속적으로 전송
        messageListenerContainer
            .receive(listOf(channel), serializer, serializer)
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

    private fun channelAndValueSerializer(): RedisSerializationContext.SerializationPair<String> {
        return RedisSerializationContext.SerializationPair.fromSerializer(StringRedisSerializer())
    }
}