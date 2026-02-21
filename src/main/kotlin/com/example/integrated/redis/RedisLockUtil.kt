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
    * 0으로 설정 시, 락 획득을 시도하고, 즉시 성공/실패를 반환합니다. ( 대기하지 않음 )
    *
    * leaseTime : 락을 획득한 후 유지 시간
    * 4초로 설정 시, 락을 획득하고 4초 동안 유지합니다. ( 4초가 지나면 자동으로 풀림 )
    * */
    suspend fun <T> acquireLockAndRun(
        key: String,
        waitTime: Long = 0,
        leaseTime: Long = 4,
        timeUnit: TimeUnit = TimeUnit.SECONDS,
        block: suspend () -> T
    ): T? {
        val lock = redissonClient.getLock(key)

        val isLocked = try {
            // 특정 key에 대한 lock 획득 시도
            lock.tryLock(waitTime, leaseTime, timeUnit)
        } catch (e: Exception) {
            log.error { "Lock 획득 중 예외 발생 - key : $key ,  $e" }
            return null
        }

        // lock 획득에 실패
        if (!isLocked) {
            log.debug { "Lock 획득 실패 - 이미 다른 인스턴스가 작업 중" }
            return null
        }

        // lock 획득에 성공
        return try {
            log.info { "Lock 획득 성공" }
            block()
        } finally {
            if (lock.isHeldByCurrentThread) {
                lock.unlock()
            }
        }
    }
}