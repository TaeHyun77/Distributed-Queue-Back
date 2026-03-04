package com.example.integrated.queue.kafka.service

import com.example.integrated.queue.kafka.dto.KafkaMessageDto
import com.example.integrated.queue.queue.scheduler.QueueScheduler
import com.example.integrated.queue.queue.scheduler.QueueSchedulerService
import com.example.integrated.redis.pubsub.RedisPublisher
import com.example.integrated.util.CHANNEL_NAME
import com.example.integrated.util.Loggable
import com.example.integrated.util.readValueFromJson
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService(
        private val objectMapper: ObjectMapper,
        private val queueSchedulerService: QueueSchedulerService,
        private val queueScheduler: QueueScheduler,
        private val redisPublisher: RedisPublisher,
        private val kafkaTemplate: KafkaTemplate<String, String>,
        @Value("\${queue.event.topic.name}") private val topicName: String
): Loggable {

    companion object {
        private const val MAX_RETRIES = 5 // 재시도 횟수
        private val BACKOFF_DELAYS = longArrayOf(1000, 2000, 4000, 8000, 12000) // 재시도 간 간격
    }

    @KafkaListener(
            topics = ["\${queue.event.topic.name}"],
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "1"
    )
    fun consumeBatch(
            messages: List<ConsumerRecord<String, String>>,
            acknowledgment: Acknowledgment
    ) {
        runBlocking {
            coroutineScope {
                messages.map { record ->
                    async { processWithRetry(record) }
                }.awaitAll()
            }
        }
        acknowledgment.acknowledge() // 이를 통해 offset 커밋
    }

    private suspend fun processWithRetry(record: ConsumerRecord<String, String>) {
        var lastException: Exception? = null

        for (attempt in 0..MAX_RETRIES) {
            try {
                handleMessage(record.value())
                return
            } catch (e: Exception) {
                lastException = e
                if (attempt < MAX_RETRIES) {
                    val delayMs = BACKOFF_DELAYS[attempt]
                    log.warn { "메시지 처리 실패 (시도 ${attempt + 1}/${MAX_RETRIES + 1}), ${delayMs}ms 후 재시도: ${e.message}" }
                    delay(delayMs)
                }
            }
        }

        log.error(lastException) { "최대 재시도 초과, DLT로 전송: key=${record.key()}" }
        sendToDlt(record)
    }

    private fun sendToDlt(record: ConsumerRecord<String, String>) {
        try {
            kafkaTemplate.send("$topicName-dlt", record.key(), record.value())
            log.info { "DLT 전송 완료: topic=$topicName-dlt, key=${record.key()}" }
        } catch (e: Exception) {
            log.error(e) { "DLT 전송 실패: key=${record.key()}" }
        }
    }

    private suspend fun handleMessage(message: String) {
        val consumeMessage = objectMapper.readValueFromJson<KafkaMessageDto>(message)

        // 하이브리드 승격 : 참가열 여유 시 직접 삽입, 아니면 대기열 삽입
        val result = queueSchedulerService.enqueueOrAllow(
                consumeMessage.queueType,
                consumeMessage.userId,
                consumeMessage.timeStamp
        )

        if (consumeMessage.requestedAt > 0) {
            val e2eDuration = System.currentTimeMillis() - consumeMessage.requestedAt
            log.info { "E2E completed duration=${e2eDuration}ms userId=${consumeMessage.userId}" }
        }

        if (result == 1L) {
            // 참가열 직접 삽입 → 클라이언트에 즉시 입장 알림
            log.info { "참가열 직접 삽입: userId=${consumeMessage.userId}, queueType=${consumeMessage.queueType}" }
        } else {
            // 대기열 삽입 → 스케줄러가 이후에 승격
            queueScheduler.addActiveQueue(consumeMessage.queueType)
        }

        redisPublisher.publish(CHANNEL_NAME, consumeMessage.queueType)
    }
}