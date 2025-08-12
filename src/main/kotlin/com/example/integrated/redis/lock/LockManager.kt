package com.example.integrated.redis.lock

import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.time.Duration

@Component
class LockManager(
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>
) {

    fun tryMutexLock(
        key: String,
        maxWaitMillis: Long = 3000,
        retryDelayMillis: Long = 100
    ): Mono<Boolean> {
        val maxAttempts = (maxWaitMillis / retryDelayMillis).toInt()

        return Mono.defer {
            reactiveRedisTemplate.opsForValue()
                .setIfAbsent(key, "lock", Duration.ofSeconds(5))
                .flatMap { acquired ->
                    if (acquired) {
                        Mono.just(true)
                    } else {
                        Mono.just(false)
                    }
                }
        }
            .repeatWhen { repeatFlux ->
                repeatFlux.delayElements(Duration.ofMillis(retryDelayMillis))
                    .take(maxAttempts.toLong()) // 최대 재시도 횟수
            }
            .filter { it } // 락 획득 성공만 통과
            .next() // 첫 번째 성공값만 반환
            .defaultIfEmpty(false)
    }

    fun unlock(key: String): Mono<Boolean> {
        return reactiveRedisTemplate.delete(key)
            .map { deletedCount -> deletedCount > 0 }
    }
}