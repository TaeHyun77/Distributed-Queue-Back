package com.example.integrated.redis.config

import com.example.integrated.util.Loggable
import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config
import org.redisson.config.ReadMode
import org.redisson.config.SubscriptionMode
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.RedisSentinelConfiguration
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer

// master-slave 구조로 변경함에 따라 설정 파일 변경
@Configuration
class RedisConfig(
    @Value("\${spring.redis.sentinel.master}") val master: String,
    @Value("\${spring.redis.sentinel.nodes}") val sentinelNodes: String

): Loggable {

    // Redis와 연결을 맺어주는 팩토리 객체 생성
    @Bean
    fun lettuceConnectionFactory(): LettuceConnectionFactory {
        val sentinelConfig = RedisSentinelConfiguration().master(master)

        sentinelNodes.split(",").forEach {
            val (host, port) = it.split(":")

            sentinelConfig.sentinel(host.trim(), port.trim().toInt())
        }

        return LettuceConnectionFactory(sentinelConfig)
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

    @Bean
    fun redissonClient(): RedissonClient {
        val config = Config()

        // Sentinel 모드로 설정
        config.useSentinelServers()
            .setMasterName(master)
            .addSentinelAddress(*sentinelNodes.split(",").map { "redis://${it.trim()}" }.toTypedArray())
            .setDatabase(0)
            .setConnectTimeout(5000)
            .setReadMode(ReadMode.MASTER)
            .setSubscriptionMode(SubscriptionMode.MASTER)
            .setTimeout(10000)

        return Redisson.create(config)
    }

//    // 레디스의 Pub/Sub 기능을 위한 리스너 컨테이너
//    @Bean
//    fun listenerContainer(
//        // 위에서 등록한 lettuceConnectionFactory Bean이 주입됨
//        lettuceConnectionFactory: ReactiveRedisConnectionFactory
//    ): ReactiveRedisMessageListenerContainer {
//
//        return ReactiveRedisMessageListenerContainer(lettuceConnectionFactory)
//    }
}
