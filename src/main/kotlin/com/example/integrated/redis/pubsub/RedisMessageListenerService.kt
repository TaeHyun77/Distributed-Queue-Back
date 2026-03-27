package com.example.integrated.redis.pubsub

import com.example.integrated.queue.sse.SseEventService
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.Loggable
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.stereotype.Service

@Service
class RedisMessageListenerService(
    private val redisMessageListenerContainer: ReactiveRedisMessageListenerContainer
): Loggable {

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    // 애플리케이션 실행 시 pub/sub channel 구독
    @PostConstruct
    fun init() {
        scope.launch {
            receiveMessages(ChannelTopic(CHANNEL_NAME))
        }
        log.info { "pub/sub channel 구독 시작" }
    }

    // 애플리케이션 종료 시 pub/sub channel 구독 해제
    @PreDestroy
    fun destroy() {
        scope.cancel()
        log.info { "pub/sub channel 구독 종료" }
    }

    // Redis Pub/Sub 채널을 구독하여 수신된 메시지를 MutableSharedFlow에 전달
    private suspend fun receiveMessages(channel: ChannelTopic) {
        val channelSerializer = createChannelAndValueSerializer()
        var attempt = 0L

        while (true) {
            try {
                redisMessageListenerContainer
                    .receive(listOf(channel), channelSerializer, channelSerializer)
                    .asFlow()
                    .collect { message ->
                        SseEventService.getSink(message.message).tryEmit(message.message)
                    }
                break
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                val delayMs = 2000L * minOf(++attempt, 5)
                log.warn { "Redis Pub/Sub 연결 끊김 (시도: $attempt), ${delayMs}ms 후 재구독: ${e.message}" }
                delay(delayMs)
            }
        }
    }

    private fun createChannelAndValueSerializer(): RedisSerializationContext.SerializationPair<String> {
        return RedisSerializationContext.SerializationPair.fromSerializer(StringRedisSerializer())
    }
}
