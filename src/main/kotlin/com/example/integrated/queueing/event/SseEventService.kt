package com.example.integrated.queueing.event

import com.example.integrated.util.Loggable
import com.example.integrated.queueing.QueueService
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactive.asFlow
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Service
import reactor.core.publisher.Sinks

@Service
class SseEventService(
    private val queueService: QueueService,
    private val objectMapper: ObjectMapper,
): Loggable {

    companion object {
        val sink: Sinks.Many<QueueEventPayload> = Sinks.many().replay().limit(1)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun streamQueueEvents(
        userId: String,
        queueType: String
    ): Flow<ServerSentEvent<String>> {

        return sink.asFlux().asFlow()
            .flatMapConcat {
                flow {
                    try {
                        log.info{ "sink 이벤트 연결 !" }
                        val queueType = queueType.split(":")[0]

                        val isAllowed = queueService.searchUserRanking(userId, queueType, "allow")

                        // 참가열에 존재한다면 → confirmed 이벤트 전송 → 타겟 페이지 이동
                        if (isAllowed != -1L) {
                            log.info{ "참가열 존재 !" }

                            val json = objectMapper.writeValueAsString(
                                mapOf("event" to "confirmed", "user_id" to userId)
                            )
                            emit(ServerSentEvent.builder(json).build())

                            // 대기열에 존재한다면 순위 전송
                        } else {
                            log.info{ "대기열 존재 !" }

                            val rank = queueService.searchUserRanking(userId, queueType, "wait")

                            // 대기열에 존재하지 않는 경우
                            val json = if (rank <= 0) {
                                objectMapper.writeValueAsString(
                                    mapOf("event" to "error", "message" to "해당 사용자는 대기열에 존재하지 않습니다.")
                                )

                                // 대기열에 존재하는 경우 → 사용자의 순위 반환
                            } else {
                                objectMapper.writeValueAsString(
                                    mapOf("event" to "update", "rank" to rank)
                                )
                            }
                            emit(ServerSentEvent.builder(json).build())
                        }
                    } catch (ex: Exception) {
                        val errorJson = objectMapper.writeValueAsString(
                            mapOf("event" to "error", "message" to "서버 오류 발생")
                        )
                        emit(ServerSentEvent.builder(errorJson).build())
                    }
                }
            }
    }
}