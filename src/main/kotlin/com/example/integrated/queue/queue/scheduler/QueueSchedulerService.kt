package com.example.integrated.queue.queue.scheduler

import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.util.ALLOW_QUEUE
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.WAIT_QUEUE
import com.example.integrated.util.Loggable
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.ClassPathResource
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
) : Loggable {

    companion object {
        // Consumer용 : 중복 체크 + score 생성 + 참가열/대기열 삽입을 원자적으로 수행
        private val ENQUEUE_OR_ALLOW_SCRIPT: RedisScript<Long> = RedisScript.of(
                ClassPathResource("scripts/enqueue-or-allow.lua"),
                Long::class.java
        )

        // 스케줄러용 : 만료 정리 + 빈 자리 계산 + 승격을 원자적으로 수행
        private val SCHEDULE_PROMOTE_SCRIPT: RedisScript<Long> = RedisScript.of(
                ClassPathResource("scripts/schedule-promote.lua"),
                Long::class.java
        )
    }

    // 스케줄러 : 만료 정리 + 빈 자리 계산 + 승격을 원자적으로 처리
    suspend fun promoteUsers(queueType: String): Long {
        val nowMs = System.currentTimeMillis()
        val expireAt = nowMs + Duration.ofMinutes(10).toMillis()

        val keys = listOf(
                "$queueType$ALLOW_QUEUE",   // KEYS[1] : 참가열 키
                "$queueType$WAIT_QUEUE"     // KEYS[2] : 대기열 키
        )

        val args = listOf(
                maxCapacity.toString(),     // ARGV[1] : 참가열 최대 수용 인원
                nowMs.toString(),           // ARGV[2] : 현재 시각 ( 만료 판단 )
                expireAt.toString()         // ARGV[3] : 참가열 score
        )

        val promoted = reactiveRedisTemplate.execute(SCHEDULE_PROMOTE_SCRIPT, keys, args)
                .next()
                .awaitSingle()

        if (promoted > 0) {
            redisPublisher.publish(CHANNEL_NAME, queueType)
        }

        return promoted
    }

    /**
     * 중복 체크 + score 생성 + 대기열/참가열 삽입을 원자적으로 처리
     * return -1 (대기열 존재), -2 (참가열 존재), 1 (참가열 삽입), 0 (대기열 삽입)
     */
    suspend fun enqueueOrAllow(
            queueType: String,
            userId: String,
            timestamp: Double
    ): Long {
        val timestampMs = (timestamp * 1000).toLong()
        val nowMs = System.currentTimeMillis()
        val expireAt = nowMs + Duration.ofMinutes(10).toMillis()

        val keys = listOf(
                "$queueType$ALLOW_QUEUE",       // KEYS[1] : 참가열 키
                "$queueType$WAIT_QUEUE",        // KEYS[2] : 대기열 키
                "queue:seq:$timestampMs"        // KEYS[3] : 밀리초별 카운터 키 ( score 생성용 )
        )

        val args = listOf(
                userId,                         // ARGV[1] : userId
                maxCapacity.toString(),         // ARGV[2] : 참가열 최대 수용 인원
                nowMs.toString(),               // ARGV[3] : 현재 시각
                expireAt.toString(),            // ARGV[4] : 참가열 score
                timestampMs.toString()          // ARGV[5] : 요청 도착 시각
        )

        return reactiveRedisTemplate.execute(ENQUEUE_OR_ALLOW_SCRIPT, keys, args)
                .next()
                .awaitSingle()
    }
}