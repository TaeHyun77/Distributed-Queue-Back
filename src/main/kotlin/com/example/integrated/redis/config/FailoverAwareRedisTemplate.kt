package com.example.integrated.redis.config

import com.example.integrated.util.Loggable
import io.lettuce.core.RedisCommandExecutionException
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisCallback
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.RedisSerializationContext
import reactor.core.publisher.Flux
import reactor.util.retry.Retry
import java.time.Duration

class FailoverAwareRedisTemplate(
    private val lettuceConnectionFactory: LettuceConnectionFactory,
    serializationContext: RedisSerializationContext<String, String>
) : ReactiveRedisTemplate<String, String>(lettuceConnectionFactory, serializationContext), Loggable {

    override fun <T : Any> execute(action: ReactiveRedisCallback<T>): Flux<T> {
        return super.execute(action)
            .retryWhen( // 예외 발생 시 실행
                Retry.fixedDelay(3, Duration.ofSeconds(1))
                    .filter { e ->
                        val isReadonly = e is RedisCommandExecutionException &&
                                e.message?.contains("READONLY") == true

                        if (isReadonly) {
                            log.warn { "READONLY 감지 — 새 마스터로 연결 리셋 시도" }
                            lettuceConnectionFactory.resetConnection()
                        }

                        isReadonly
                    }
            )
    }
}
