package com.example.integrated.redis

import com.example.integrated.util.Loggable
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.withContext
import org.redisson.api.RedissonClient
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.coroutines.cancellation.CancellationException

@Component
class RedisLockUtil(
        private val redissonClient: RedissonClient
): Loggable {

    @OptIn(ExperimentalCoroutinesApi::class)
    private val lockDispatcher = Dispatchers.IO.limitedParallelism(1)

    suspend fun <T> acquireLockAndRun(
            key: String,
            waitTime: Long = 0,
            leaseTime: Long = 4,
            timeUnit: TimeUnit = TimeUnit.SECONDS,
            block: suspend () -> T
    ): T? {
        return withContext(lockDispatcher) {
            val lock = redissonClient.getLock(key)

            val isLocked = try {
                lock.tryLock(waitTime, leaseTime, timeUnit)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.error { "Lock 획득 중 예외 발생 - key: $key, ${e.message}" }
                return@withContext null
            }

            if (!isLocked) {
                log.debug { "Lock 획득 실패 - key: $key" }
                return@withContext null
            }

            try {
                block()
            } finally {
                try {
                    lock.unlock()
                } catch (e: Exception) {
                    log.warn { "Lock 해제 실패 - key: $key, ${e.message}" }
                }
            }
        }
    }
}