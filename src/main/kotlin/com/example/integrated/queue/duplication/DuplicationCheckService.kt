package com.example.integrated.queue.duplication

import com.example.integrated.util.Loggable
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class DuplicationCheckService(
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>
): Loggable {

    // Redis SET NX로 중복 요청 감지 ( 키가 없을 때만 저장 + TTL 5분 )
    suspend fun isDuplicate(requestKey: String): Boolean {
        val key = "duplication:$requestKey"

        val isFirst = reactiveRedisTemplate.opsForValue()
            .setIfAbsent(key, "1", Duration.ofMinutes(5))
            .awaitSingle()

        return !isFirst
    }
}