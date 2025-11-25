package com.example.integrated.redis

import com.example.integrated.util.Loggable
import org.redisson.api.RedissonClient
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

@Component
class RedisLockUtil(
    private val redissonClient: RedissonClient
): Loggable {

    /*
    * waitTime : 락을 획득하기 위한 대기 시간
    * leaseTime : 락을 획득한 후 유지 시간
    * */
    suspend fun <T> acquireLockAndRun(
        key: String,
        waitTime: Long = 0,
        leaseTime: Long = 4,
        timeUnit: TimeUnit = TimeUnit.SECONDS,
        block: suspend () -> T
    ): T? {
        val lock = redissonClient.getLock(key)

        return try {
            val isLocked = lock.tryLock(waitTime, leaseTime, timeUnit)

            if (!isLocked) {
                log.debug { "Lock 획득 실패 - 이미 다른 인스턴스가 작업 중" }
                return null
            }

            log.info { "Lock 획득 성공" }
            block()
        } catch (e: Exception) {
            log.error { "Lock 획득 실패 : $e" }
            throw e
        }
    }
}