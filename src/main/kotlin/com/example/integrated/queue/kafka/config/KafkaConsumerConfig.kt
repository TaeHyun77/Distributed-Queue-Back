package com.example.integrated.queue.kafka.config

import com.example.integrated.util.Loggable
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties

@Configuration
class KafkaConsumerConfig(
        private val env: Environment,
) {

    @Bean
    fun consumerConfig(): Map<String, Any> {
        return mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to env.getProperty("spring.kafka.consumer.bootstrap-servers")!!,
                ConsumerConfig.GROUP_ID_CONFIG to env.getProperty("spring.kafka.consumer.group-id")!!,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to env.getProperty("spring.kafka.consumer.auto-offset-reset")!!,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,

                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false, // 자동 commit 설정 false, 오프셋 커밋의 제어권을 Spring Kafka에 넘김
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 100, // 한 번에 100개의 record를 가져옴
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 300000, // 5분 동안 poll()을 호출하지 않으면 리밸런싱 ( default : 5분 )
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to 30000
        )
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> {
        return DefaultKafkaConsumerFactory(consumerConfig())
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> =
            ConcurrentKafkaListenerContainerFactory<String, String>().apply {
                consumerFactory = consumerFactory()
                isBatchListener = true
                containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
            }
}
