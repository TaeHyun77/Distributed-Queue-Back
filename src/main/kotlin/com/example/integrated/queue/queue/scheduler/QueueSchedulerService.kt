package com.example.integrated.queue.queue.scheduler

import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.util.ALLOW_QUEUE
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.WAIT_QUEUE
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.ClassPathResource
import org.springframework.data.redis.core.DefaultTypedTuple
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.core.script.RedisScript
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class QueueSchedulerService(
        @Value("\${queue.allow.max-capacity}")
        private val maxCapacity: Long,

        private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
        private val redisPublisher: RedisPublisher
) {

    companion object {
        // Lua 스크립트 : 만료 사용자 정리 + 참가열 여유 확인 후 참가열 직접 삽입 or 대기열 삽입을 원자적으로 수행
        // 반환 값: 1 ( 참가열 직접 삽입 ), 0 ( 대기열 삽입 )
        private val ENQUEUE_OR_ALLOW_SCRIPT: RedisScript<Long> = RedisScript.of(
                ClassPathResource("scripts/enqueue-or-allow.lua"),
                Long::class.java
        )
    }

    // 대기열 -> 참가열 승격 로직
    suspend fun allowUser(
            queueType: String,
            count: Long
    ): Int {
        val waitQueueKey = "$queueType$WAIT_QUEUE"

        val poppedUsers = reactiveRedisTemplate.opsForZSet()
                .popMin(waitQueueKey, count)
                .collectList()
                .awaitSingle() // 값이 있으면 List<T> 반환, 값이 없으면 빈 List 반환

        if (poppedUsers.isEmpty()) return 0

        val allowQueueKey = "$queueType$ALLOW_QUEUE"

        val expireAt = System.currentTimeMillis() + Duration.ofMinutes(10).toMillis()

        // 개별적으로 add 하는 것이 아닌 한 번에 하도록
        val tuples = poppedUsers.map { user ->
            DefaultTypedTuple(user.value.toString(), expireAt.toDouble())
        }.toSet()

        reactiveRedisTemplate.opsForZSet()
                .addAll(allowQueueKey, tuples)
                .awaitSingle()

        redisPublisher.publish(CHANNEL_NAME, queueType)

        return poppedUsers.size
    }

    // 하이브리드 승격 : 참가열 여유 시 직접 삽입, 아니면 대기열 삽입 (원자적으로 수행)
    // 반환 값: 1 ( 참가열 직접 삽입 ), 0 ( 대기열 삽입 )
    suspend fun enqueueOrAllow(
            queueType: String,
            userId: String,
            waitScore: Double
    ): Long {
        val nowMs = System.currentTimeMillis()
        val expireAt = nowMs + Duration.ofMinutes(10).toMillis()

        val keys = listOf(
                "$queueType$ALLOW_QUEUE",   // KEYS[1] : 참가열 키
                "$queueType$WAIT_QUEUE",    // KEYS[2] : 대기열 키
                "registering:$queueType:$userId"    // KEYS[3] : 등록 진행 중 플래그
        )

        val args = listOf(
                userId,                     // ARGV[1] : userId
                maxCapacity.toString(),     // ARGV[2] : 참가열 최대 수용 인원
                nowMs.toString(),           // ARGV[3] : 현재 시각 (만료 판단용)
                expireAt.toString(),        // ARGV[4 ]: 참가열 score (expireAt)
                waitScore.toString()        // ARGV[5] : 대기열 score
        )

        return reactiveRedisTemplate.execute(ENQUEUE_OR_ALLOW_SCRIPT, keys, args)
                .next()
                .awaitSingle()
    }
}