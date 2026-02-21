package com.example.integrated.redis.pubsub

import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component

@Component
class RedisPublisher(
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>
) {

    suspend fun publish(channel: String, message: String) {

        // 특정 채널로 메세지를 전달 ( 이 채널을 구독 중인 클라이언트에게 전달할 수 있음 )
        reactiveRedisTemplate.convertAndSend(channel, message)
            .awaitSingle()
    }
}