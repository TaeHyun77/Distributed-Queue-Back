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
import kotlinx.coroutines.future.await
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
) : Loggable {

    companion object {
        private const val MAX_RETRIES = 3
        private val BACKOFF_DELAYS = longArrayOf(500, 1000, 2000) // 최대 3.5초
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
        acknowledgment.acknowledge()
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

    private suspend fun sendToDlt(record: ConsumerRecord<String, String>) {
        try {
            kafkaTemplate.send("$topicName-dlt", record.key(), record.value()).await()
            log.info { "DLT 전송 완료: topic=$topicName-dlt, key=${record.key()}" }
        } catch (e: Exception) {
            log.error(e) { "DLT 전송 실패: key=${record.key()}" }
        }
    }

    private suspend fun handleMessage(message: String) {
        val consumeMessage = objectMapper.readValueFromJson<KafkaMessageDto>(message)

        // 단일 Lua 스크립트로 중복 체크 + score 생성 + 대기열/참가열 삽입을 원자적으로 처리
        val result = queueSchedulerService.enqueueOrAllow(
                consumeMessage.queueType,
                consumeMessage.userId,
                consumeMessage.timestamp
        )

        // E2E latency 측정
        if (consumeMessage.timestamp > 0) {
            val requestedAtMs = (consumeMessage.timestamp * 1000).toLong()
            val e2eDuration = System.currentTimeMillis() - requestedAtMs
            log.info { "E2E completed duration=${e2eDuration}ms userId=${consumeMessage.userId}" }
        }

        when (result) {
            -1L, -2L -> {
                log.info { "이미 등록된 유저 (중복 무시): userId=${consumeMessage.userId}" }
            }
            1L -> {
                log.info { "참가열 직접 삽입: userId=${consumeMessage.userId}, queueType=${consumeMessage.queueType}" }
            }
            else -> {
                queueScheduler.addActiveQueue(consumeMessage.queueType)
            }
        }

        redisPublisher.publish(CHANNEL_NAME, consumeMessage.queueType)
    }
}