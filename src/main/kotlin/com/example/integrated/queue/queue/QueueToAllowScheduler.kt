package com.example.integrated.queue.queue

import com.example.integrated.redis.RedisLockUtil
import com.example.integrated.util.ACTIVE_QUEUE_KEY
import com.example.integrated.util.Loggable
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class QueueToAllowScheduler(
    private val queueService: QueueService,
    private val redisLockUtil: RedisLockUtil,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,

    @Value("\${queue.allow.max-capacity}")
    private val maxCapacity: Long,
) : Loggable {

    @Scheduled(
        fixedDelayString = "\${queue.allow.interval-ms}",
        initialDelay = 5000
    )
    suspend fun scheduling() {
        val activeQueues = activeQueue()
        if (activeQueues.isEmpty()) return

        redisLockUtil.acquireLockAndRun("scheduling_key") {
            activeQueues.forEach { queueType ->
                try {
                    val availableAllowSize = maxCapacity - queueService.getAllowQueueSize(queueType)

                    val count = queueService.allowUser(queueType, availableAllowSize)
                    log.info { "$queueType 허용열로 이동한 사용자 : $count" }

                    if (count == 0) removeActiveQueue(queueType)

                } catch (e: Exception) {
                    log.error(e) { "스케줄링 중 예외 발생 - ${e.message}" }
                }
            }
        }
    }

    suspend fun activeQueue(): List<String> {
        return reactiveRedisTemplate.opsForSet()
            .members(ACTIVE_QUEUE_KEY)
            .collectList()
            .awaitSingleOrNull()
            ?: emptyList()
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