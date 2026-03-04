package com.example.integrated.queue.queue.scheduler

import com.example.integrated.redis.RedisLockUtil
import com.example.integrated.util.ACTIVE_QUEUE_KEY
import com.example.integrated.util.ALLOW_QUEUE
import com.example.integrated.util.Loggable
import com.example.integrated.util.SCHEDULING_KEY
import com.example.integrated.util.WAIT_QUEUE
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.Range
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class QueueScheduler(
        private val queueSchedulerService: QueueSchedulerService,
        private val redisLockUtil: RedisLockUtil,
        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,

        @Value("\${queue.allow.max-capacity}")
    private val maxCapacity: Long,
) : Loggable {

    // fixedDelayString 마다 모든 인스턴스가 락 획득을 시도합니다.
    // 이때 락을 획득한 서버만 스케줄링을 진행합니다.
    @Scheduled(
        fixedDelayString = "\${queue.allow.interval-ms}",
        initialDelay = 5000 // 애플리케이션 시작 후 5초 후부터 진행
    )
    suspend fun scheduling() {
        val activeQueues = getActiveQueue()
        if (activeQueues.isEmpty()) return

        redisLockUtil.acquireLockAndRun(SCHEDULING_KEY) {
            activeQueues.forEach { queueType ->
                try {
                    // 1. 참가열에서 만료된 사용자 정리
                    cleanExpiredAllowQueueMembers(queueType)

                    // 2. 참가열의 여유 공간 계산
                    val allowQueueSize = getQueueSize(queueType, ALLOW_QUEUE)
                    val availableAllowSize = maxOf(0, maxCapacity - allowQueueSize)

                    // 3. 대기열에서 여유분만큼 참가열로 승격
                    val count = queueSchedulerService.allowUser(queueType, availableAllowSize)
                    log.info { "$queueType 허용열로 이동한 사용자 : $count" }

                    // 대기열이 비어있을 때만 비활성화 ( 참가열이 가득 찬 경우는 유지 )
                    if (count == 0 && getQueueSize(queueType, WAIT_QUEUE) == 0L) {
                        removeActiveQueue(queueType)
                    }
                } catch (e: Exception) {
                    log.error(e) { "스케줄링 중 예외 발생 - ${e.message}" }
                }
            }
        }
    }

    // 스케줄링 대상 큐 리스트 반환
    suspend fun getActiveQueue(): List<String> {
        return reactiveRedisTemplate.opsForSet()
            .members(ACTIVE_QUEUE_KEY)
            .collectList()
            .awaitSingleOrNull()
            ?: emptyList()
    }

    // 특정 queueType의 대기열 or 참가열 사이즈 반환
    suspend fun getQueueSize(queueType: String, queueSuffix: String): Long {
        val key = "$queueType$queueSuffix"

        return reactiveRedisTemplate.opsForZSet()
                .size(key)
                .awaitSingle()
    }

    // 참가열에서 만료된 사용자 정리 ( score가 만료된 사용자 )
    private suspend fun cleanExpiredAllowQueueMembers(queueType: String): Long {
        val key = "$queueType$ALLOW_QUEUE"
        val now = System.currentTimeMillis().toDouble()

        return reactiveRedisTemplate.opsForZSet()
            .removeRangeByScore(key, Range.closed(0.0, now))
            .awaitSingle()
    }

    // 스케줄링 대상 큐 목록에 삽입
    suspend fun addActiveQueue(queueType: String) {
        reactiveRedisTemplate.opsForSet()
            .add(ACTIVE_QUEUE_KEY, queueType)
            .awaitSingle()
    }

    // 스케줄링 대상 큐 목록에서 삭제
    suspend fun removeActiveQueue(queueType: String) {
        reactiveRedisTemplate.opsForSet()
            .remove(ACTIVE_QUEUE_KEY, queueType)
            .awaitSingle()
    }
}