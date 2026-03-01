package com.example.integrated.queue.kafka.config

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

            // 모든 브로커가 produce 성공 여부 확인 후 ACK
            // 이를 통해 produce가 성공적으로 되었는지 확인 가능
            ProducerConfig.ACKS_CONFIG to "all",

            // 브로커가 메세지를 받아서 저장은 했지만, ack 응답을 내려주지 못한 경우 중복 produce가 발생할 수 있음
            // 따라서 ENABLE_IDEMPOTENCE을 true로 하여 메세지 중복 producing을 방지
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,

            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 5, // 순서 보장
            ProducerConfig.LINGER_MS_CONFIG to 5,
            ProducerConfig.BATCH_SIZE_CONFIG to 32768,
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