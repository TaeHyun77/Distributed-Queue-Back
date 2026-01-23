package com.example.integrated.queue.kafka.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

@EnableKafka
@Configuration
class KafkaConsumerConfig(private val env: Environment) {

    @Bean
    fun consumerConfig(): Map<String, Any> {
        return mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to env.getProperty("spring.kafka.consumer.bootstrap-servers")!!,
            ConsumerConfig.GROUP_ID_CONFIG to env.getProperty("spring.kafka.consumer.group-id")!!,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to env.getProperty("spring.kafka.consumer.auto-offset-reset")!!,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,

            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,

            // 한 번에 가져올 레코드 수 (배치 처리 시)
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 100,

            // Poll 간격 제한 (Consumer가 살아있는지 확인)
            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 300000, // 5분

            // Session timeout (Consumer가 죽었다고 판단하는 시간)
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to 30000 // 30초
        )
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> {
        return DefaultKafkaConsumerFactory(consumerConfig())
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        return ConcurrentKafkaListenerContainerFactory<String, String>().apply {
            consumerFactory = consumerFactory()
        }
    }
}