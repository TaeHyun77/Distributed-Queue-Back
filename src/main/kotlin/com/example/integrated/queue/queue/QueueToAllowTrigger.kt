package com.example.integrated.queue.queue

import com.example.integrated.redis.RedisLockUtil
import com.example.integrated.util.ACTIVE_QUEUE_KEY
import com.example.integrated.util.Loggable
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component

@Component
class QueueToAllowTrigger(
    private val queueService: QueueService,

    @Value("\${queue.allow.interval-ms}")
    private val allowSchedulerInterval: Long,

    @Value("\${queue.allow.max-capacity}")
    private val maxCapacity: Long,

    private val redisLockUtil: RedisLockUtil,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
): Loggable {

    fun tickerFlow(period: Long, initialDelay: Long = 0) = flow {
        delay(initialDelay)

        while (true) {
            val activeQueues = reactiveRedisTemplate.opsForSet()
                .members(ACTIVE_QUEUE_KEY)
                .collectList()
                .awaitSingleOrNull()
                ?.takeIf { it.isNotEmpty() } ?: emptyList()

            if (activeQueues.isNotEmpty()) emit(activeQueues)

            delay(period)
        }
    }

    private val tickerScope = CoroutineScope(Dispatchers.Default)

    @PostConstruct
    fun schedulingStart() {
        tickerScope.launch {
            tickerFlow(allowSchedulerInterval, 5_000)
                .collect { activeQueues ->
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
        }
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

    @PreDestroy
    fun stop() {
        println("스케줄링 종료")
        tickerScope.cancel()
    }
}