package com.example.integrated.queue.sse

import com.example.integrated.queue.sse.event.ConfirmSseEvent
import com.example.integrated.queue.sse.event.RetrySseEvent
import com.example.integrated.util.Loggable
import com.example.integrated.util.isRedisConnectionException
import com.example.integrated.queue.queue.QueueService
import com.example.integrated.queue.sse.event.ErrorSseEvent
import com.example.integrated.queue.sse.event.UpdateSseEvent
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onStart
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

@Service
class SseEventService(
    private val queueService: QueueService,
    private val objectMapper: ObjectMapper,
): Loggable {

    // 특정 queueType 별로 이벤트를 독립적으로 전달하기 위해 Flow를 분리
    companion object {
        private val sinks = ConcurrentHashMap<String, MutableSharedFlow<String>>()

        fun getSink(queueType: String): MutableSharedFlow<String> {
            return sinks.computeIfAbsent(queueType) {
                MutableSharedFlow(extraBufferCapacity = 64)
            }
        }
    }

    fun streamQueueEvents(
        queueType: String,
        userId: String
    ): Flow<ServerSentEvent<String>> {
        return getSink(queueType)
            .onStart { emit(queueType) } // 연결 즉시 현재 상태를 먼저 전송
            .map { buildSseEvent(queueType, userId) }
            .catch { e ->
                log.error { "SSE 처리 중 에러: ${e.message}" }
                emit(
                    ServerSentEvent.builder(
                        objectMapper.writeValueAsString(
                            ErrorSseEvent(message = "SSE 이벤트 전송 오류 발생")
                        )
                    ).event("error").build()
                )
            }
    }

    private suspend fun buildSseEvent(
        queueType: String,
        userId: String,
    ): ServerSentEvent<String> {
        return try {

            // 1. 대기열 순위 확인 ( 대부분의 사용자는 대기열에 존재하기 때문 )
            val rank = queueService.getWaitQueueRank(queueType, userId)

            if (rank > 0) {
                ServerSentEvent.builder(
                    objectMapper.writeValueAsString(UpdateSseEvent(rank = rank))
                ).event("update").build()
            } else {
                // 2. 대기열에 없으면 참가열 확인 ( Lua 원자성으로 대기열에서 빠진 사용자는 반드시 참가열에 존재 )
                val isInAllowQueue = !queueService.isAllowTokenExpired(queueType, userId)

                if (isInAllowQueue) {
                    ServerSentEvent.builder(
                        objectMapper.writeValueAsString(ConfirmSseEvent(userId = userId))
                    ).event("confirmed").build()
                } else {
                    ServerSentEvent.builder(
                        objectMapper.writeValueAsString(ErrorSseEvent(message = "대기열 조회 오류 발생"))
                    ).event("error").build()
                }
            }
        } catch (e: Exception) {
            if (isRedisConnectionException(e)) {
                log.warn { "Redis 연결 실패 (failover 가능성), retry 이벤트 전송: ${e.message}" }
                ServerSentEvent.builder(
                    objectMapper.writeValueAsString(RetrySseEvent(message = "서버 일시 장애입니다. 잠시만 기다려주세요."))
                ).event("retry").build()
            } else {
                log.error { "SSE 이벤트 생성 중 에러: ${e.message}" }
                ServerSentEvent.builder(
                    objectMapper.writeValueAsString(ErrorSseEvent(message = "SSE 이벤트 생성 실패"))
                ).event("error").build()
            }
        }
    }
}