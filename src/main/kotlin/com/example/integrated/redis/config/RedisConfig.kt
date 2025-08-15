package com.example.integrated.redis.config

import com.example.integrated.Loggable
import com.example.integrated.queueing.QueueService
import com.example.integrated.queueing.event.QueueEventPayload
import com.example.integrated.redis.subscribe.RedisMessageListenerService
import com.example.integrated.util.CHANNEL_NAME
import kotlinx.coroutines.reactor.mono
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer

@Configuration
class RedisConfig(
    @Value("\${spring.redis.host}") val host: String,
    @Value("\${spring.redis.port}") val port: Int

): Loggable {

    @Bean
    fun lettuceConnectionFactory(): LettuceConnectionFactory {
        return LettuceConnectionFactory(host, port)
    }

    /** Reactive RedisTemplate */
    @Bean
    @Primary
    fun reactiveRedisTemplate(): ReactiveRedisTemplate<String, String> {

        val context = RedisSerializationContext
            .newSerializationContext<String, String>(StringRedisSerializer())
            .value(StringRedisSerializer())
            .build()

        return ReactiveRedisTemplate(lettuceConnectionFactory(), context)
    }

    /*
    * ReactiveRedisMessageListenerContainer : Spring Data Redis에서 제공하는 Reactive Redis pub/sub의 subscribe를 위한 메세지 리스너
    * ⇒ 특정 채널에서 비동기적으로 메세지를 수신할 수 있음
    *
    * receiveMessages : 메세지 수신 처리가 일어나는 함수
    * */
    @Bean
    fun listenerContainer(

        // 자동으로 lettuceConnectionFactory Bean이 주입됨
        connectionFactory: ReactiveRedisConnectionFactory
    )
    : ReactiveRedisMessageListenerContainer {

        return ReactiveRedisMessageListenerContainer(connectionFactory)

    }
}
