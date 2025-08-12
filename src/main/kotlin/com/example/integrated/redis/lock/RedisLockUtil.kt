package com.example.integrated.redis.lock

import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

/*
* 특정 비즈니스 로직을 실행하기 전 락을 획득했을 때만 실행되도록 하는 코드
* */
@Component
class RedisLockUtil(
    private val manager: LockManager
) {
    private val log = LoggerFactory.getLogger(javaClass)

    suspend fun <T> acquireLockAndRun(key: String, block: suspend () -> T): T {
        if (key.isBlank()) {
            log.error("[RedisLockError] key is blank")
            return block()
        }

        val acquired = acquiredLock(key).awaitFirstOrNull() ?: false
        if (acquired) {
            return proceedWithLock(key, block)
        } else {
            throw ReserveException(HttpStatus.CONFLICT, ErrorCode.REDIS_FAILED_TO_ACQUIRED_LOCK)
        }
    }

    private fun acquiredLock(key: String): Mono<Boolean> {
        return manager.tryMutexLock(key)
            .doOnNext { if (it) log.info("Lock 획득 성공") }
            .onErrorResume { e ->
                log.error("[RedisLockError] failed to acquire lock. key: $key", e)
                Mono.just(false)
            }
    }

    private suspend fun <T> proceedWithLock(key: String, block: suspend () -> T): T {
        return try {
            block()
        } finally {
            releaseLock(key)
        }
    }

    private fun releaseLock(key: String): Mono<Boolean> {
        return manager.tryMutexLock(key)
            .doOnNext { if (it) log.info("Lock 반환 성공") }
            .onErrorResume { e ->
                log.error("[RedisLockError] failed to unlock. key: $key", e)
                Mono.just(false)
            }
    }
}