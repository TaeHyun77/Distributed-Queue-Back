package com.example.integrated.queue.sse

import com.example.integrated.queue.sse.event.ConfirmSseEvent
import com.example.integrated.util.Loggable
import com.example.integrated.queue.queue.QueueService
import com.example.integrated.queue.sse.event.ErrorSseEvent
import com.example.integrated.queue.sse.event.UpdateSseEvent
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Service

@Service
class SseEventService(
    private val queueService: QueueService,
    private val objectMapper: ObjectMapper,
): Loggable {

    companion object {
        val sink = MutableSharedFlow<String>(replay = 1)
    }

    fun streamQueueEvents(
        queueType: String,
        userId: String
    ): Flow<ServerSentEvent<String>> {
        return sink
            .filter { it == queueType }
            .map {
                ServerSentEvent.builder(buildEvent(queueType, userId))
                    .build()
            }
            .catch { e ->
                log.error { "SSE 처리 중 에러: ${e.message}" }
                val err = objectMapper.writeValueAsString(
                    ErrorSseEvent(message = "sse 이벤트 전송 오류 발생")
                )

                emit(ServerSentEvent.builder(err)
                    .build()
                )
            }
    }

    private suspend fun buildEvent(
        queueType: String,
        userId: String,
    ): String {
        return try {
            val allowed = queueService.searchUserRanking(queueType, userId,"allow")

            // 참가열에 존재한다면
            if (allowed != -1L) {
                objectMapper.writeValueAsString(
                    ConfirmSseEvent(userId = userId)
                )
            // 대기열에 존재한다면
            } else {
                val rank = queueService.searchUserRanking(queueType, userId,"wait")

                log.info{"queueType : $queueType , userId : $userId"}
                log.info { "sse rank : $rank" }

                if (rank > 0) {
                    objectMapper.writeValueAsString(UpdateSseEvent(rank = rank))
                } else {
                    objectMapper.writeValueAsString(
                        ErrorSseEvent(message = "대기열 조회 오류 발생")
                    )
                }
            }
        } catch (e: Exception) {
            log.error { "sse 이벤트 생성 중 에러: ${e.message}" }

            objectMapper.writeValueAsString(
                ErrorSseEvent(message = "sse 이벤트 생성 실패")
            )
        }
    }
}