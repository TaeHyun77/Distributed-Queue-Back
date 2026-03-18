package com.example.integrated.queue.queue.scheduler

import com.example.integrated.redis.RedisLockUtil
import com.example.integrated.util.ACTIVE_QUEUE_KEY
import com.example.integrated.util.Loggable
import com.example.integrated.util.SCHEDULING_KEY
import com.example.integrated.util.WAIT_QUEUE
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class QueueScheduler(
        private val queueSchedulerService: QueueSchedulerService,
        private val redisLockUtil: RedisLockUtil,
        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
) : Loggable {

    @Scheduled(
            fixedDelayString = "\${queue.allow.interval-ms}",
            initialDelay = 5000
    )
    suspend fun scheduling() {
        val activeQueues = getActiveQueue()
        if (activeQueues.isEmpty()) return

        redisLockUtil.acquireLockAndRun(SCHEDULING_KEY) {
            activeQueues.forEach { queueType ->
                try {
                    // Lua 스크립트 하나로 만료 정리 + 빈 자리 계산 + 승격을 원자적으로 처리
                    val count = queueSchedulerService.promoteUsers(queueType)
                    log.info { "$queueType 허용열로 이동한 사용자 : $count" }

                    if (count == 0L && getWaitQueueSize(queueType) == 0L) {
                        removeActiveQueue(queueType)
                    }
                } catch (e: Exception) {
                    log.error(e) { "스케줄링 중 예외 발생 - ${e.message}" }
                }
            }
        }
    }

    suspend fun getActiveQueue(): List<String> {
        return reactiveRedisTemplate.opsForSet()
                .members(ACTIVE_QUEUE_KEY)
                .collectList()
                .awaitSingleOrNull()
                ?: emptyList()
    }

    private suspend fun getWaitQueueSize(queueType: String): Long {
        return reactiveRedisTemplate.opsForZSet()
                .size("$queueType$WAIT_QUEUE")
                .awaitSingle()
    }

    suspend fun addActiveQueue(queueType: String) {
        reactiveRedisTemplate.opsForSet()
                .add(ACTIVE_QUEUE_KEY, queueType)
                .awaitSingle()
    }

    suspend fun removeActiveQueue(queueType: String) {
        reactiveRedisTemplate.opsForSet()
                .remove(ACTIVE_QUEUE_KEY, queueType)
                .awaitSingle()
    }
}