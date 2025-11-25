package com.example.integrated.queue.kafka

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

@Configuration
class KafkaProducerConfig(
    private val env: Environment
) {

    fun producerConfig(): Map<String, Any> {
        return mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to env.getProperty("spring.kafka.producer.bootstrap-servers")!!,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,

            // 안정성 설정
            ProducerConfig.ACKS_CONFIG to "all", // 모든 ISR 브로커 확인 후 ACK
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true, // 중복 방지 + 순서 보장
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 1, // 순서 보장
            ProducerConfig.RETRIES_CONFIG to Int.MAX_VALUE, // 무제한 재시도
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to 120000, // 재시도 포함 전송 타임아웃 (기본 2분)
            ProducerConfig.RETRY_BACKOFF_MS_CONFIG to 100L // 재시도 간격
        )
    }

    fun producerFactory(): ProducerFactory<String, String> {
        return DefaultKafkaProducerFactory(producerConfig())
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(producerFactory())
    }
}