package com.example.integrated.redis

import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import com.example.integrated.util.Loggable
import org.redisson.api.RedissonClient
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

//@Component
//class RedisLockUtil(
//    private val redissonClient: RedissonClient
//): Loggable {
//
//    /**
//     * 단일 키용 락
//     */
//    suspend fun <T> acquireLockAndRun(
//        key: String,
//        waitTime: Long = 0,
//        leaseTime: Long = 4,
//        timeUnit: TimeUnit = TimeUnit.SECONDS,
//        block: suspend () -> T
//    ): T {
//        val lock = redissonClient.getLock(key)
//
//        return if (lock.tryLock(waitTime, leaseTime, timeUnit)) {
//            try {
//                log.info { "Lock 획득 성공 - key: $key" }
//                block()
//            } finally {
//                lock.unlock()
//                log.info { "Lock 해제 - key: $key" }
//            }
//        } else {
//            log.warn { "Lock 획득 실패 - key: $key" }
//            throw ReserveException(HttpStatus.CONFLICT, ErrorCode.REDIS_FAILED_TO_ACQUIRED_LOCK)
//        }
//    }
//}