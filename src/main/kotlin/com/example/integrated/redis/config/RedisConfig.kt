package com.example.integrated.redis.config

import com.example.integrated.util.Loggable
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer

@Configuration
class RedisConfig(
    @Value("\${spring.redis.host}") val host: String,
    @Value("\${spring.redis.port}") val port: Int

): Loggable {

    // Redis와 연결을 맺어주는 팩토리 객체 생성
    @Bean
    fun lettuceConnectionFactory(): LettuceConnectionFactory {
        return LettuceConnectionFactory(host, port)
    }

    // 실제 애플리케이션에서 Redis와 데이터를 주고받는 인터페이스
    @Bean
    fun reactiveRedisTemplate(): ReactiveRedisTemplate<String, String> {

        // StringRedisSerializer()를 사용해서 key와 value 모두 String으로 직렬화 / 역직렬화하도록 설정
        val context = RedisSerializationContext
            .newSerializationContext<String, String>(StringRedisSerializer())
            .value(StringRedisSerializer())
            .build()

        return ReactiveRedisTemplate(lettuceConnectionFactory(), context)
    }

    // 레디스의 Pub/Sub 기능을 위한 리스너 컨테이너
    @Bean
    fun listenerContainer(
        // 위에서 등록한 lettuceConnectionFactory Bean이 주입됨
        lettuceConnectionFactory: ReactiveRedisConnectionFactory
    ): ReactiveRedisMessageListenerContainer {

        return ReactiveRedisMessageListenerContainer(lettuceConnectionFactory)
    }
}
