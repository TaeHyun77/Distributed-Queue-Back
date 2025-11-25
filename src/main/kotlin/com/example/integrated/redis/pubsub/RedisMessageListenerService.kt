package com.example.integrated.redis.pubsub
import com.example.integrated.queue.sse.SseEventService
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.Loggable
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
        val channelSerializer = createChannelAndValueSerializer()

        messageListenerContainer.receive(listOf(channel), channelSerializer, channelSerializer)
            .flatMap {
                val queueType = it.message

                mono {
                    SseEventService.sink.tryEmit(queueType)
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