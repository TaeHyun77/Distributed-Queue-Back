package com.example.integrated.redis.pubsub

import com.example.integrated.queue.sse.SseEventService
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.Loggable
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.retryWhen
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

    @PostConstruct
    fun init() {
        scope.launch {
            receiveMessages(ChannelTopic(CHANNEL_NAME))
        }
        log.info { "pub/sub channel 구독 시작" }
    }

    @PreDestroy
    fun destroy() {
        scope.cancel()
        log.info { "pub/sub channel 구독 종료" }
    }

    private suspend fun receiveMessages(channel: ChannelTopic) {
        val channelSerializer = createChannelAndValueSerializer()

        redisMessageListenerContainer
            .receive(listOf(channel), channelSerializer, channelSerializer)
            .asFlow()
        .map { it.message }
        .onEach { queueType ->
            SseEventService.sink.tryEmit(queueType)
        }
        .retryWhen { cause, attempt ->
            val delayMs = 2000L * minOf(attempt + 1, 5)
            log.warn { "Redis Pub/Sub 연결 끊김 (시도: ${attempt + 1}), ${delayMs}ms 후 재구독: ${cause.message}" }
            delay(delayMs)
            true
        }
        .catch { e ->
            log.error { "Redis Pub/Sub 복구 불가 에러: ${e.message}" }
        }
        .collect()
    }

    private fun createChannelAndValueSerializer(): RedisSerializationContext.SerializationPair<String> {
        return RedisSerializationContext.SerializationPair.fromSerializer(StringRedisSerializer())
    }
}
