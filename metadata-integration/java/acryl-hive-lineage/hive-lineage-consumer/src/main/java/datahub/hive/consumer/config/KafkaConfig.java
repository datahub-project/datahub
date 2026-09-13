package datahub.hive.consumer.config;

import datahub.client.kafka.KafkaEmitter;
import datahub.client.kafka.KafkaEmitterConfig;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for Kafka consumer and producer.
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;
    
    @Value("${spring.kafka.consumer.concurrency:7}")
    private int consumerConcurrency;

    @Value("${spring.kafka.consumer.retry.max-attempts:3}")
    private int maxRetryAttempts;

    @Value("${spring.kafka.consumer.retry.initial-delay:1000}")
    private int delay;

    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Bean for KafkaEmitter with Avro serialization used to send messages to the MCP topic.
     */
    @Bean
    public KafkaEmitter kafkaEmitter() throws IOException {

        String bootstrapServers = String.join(",", kafkaProperties.getProducer().getBootstrapServers());
        String schemaRegistryUrl = kafkaProperties.getProducer().getProperties().get("schema.registry.url");
        String mcpTopic = kafkaProperties.getProducer().getProperties().get("mcp.topic");

        Map<String, String> producerConfig = new HashMap<>();

        producerConfig.put("security.protocol", kafkaProperties.getSecurity().getProtocol());
        try {
            producerConfig.put("ssl.truststore.location", kafkaProperties.getSsl().getTrustStoreLocation().getFile().getAbsolutePath());
            producerConfig.put("ssl.keystore.location", kafkaProperties.getSsl().getKeyStoreLocation().getFile().getAbsolutePath());
        } catch (IOException e) {
            throw new IOException("Error getting SSL store locations", e);
        }
        producerConfig.put("ssl.truststore.password", kafkaProperties.getSsl().getTrustStorePassword());
        producerConfig.put("ssl.keystore.password", kafkaProperties.getSsl().getKeyStorePassword());

        producerConfig.putAll(kafkaProperties.getProducer().getProperties());

        KafkaEmitterConfig config = KafkaEmitterConfig.builder()
                .bootstrap(bootstrapServers)
                .schemaRegistryUrl(schemaRegistryUrl)
                .producerConfig(producerConfig)
                .build();

        return new KafkaEmitter(config, mcpTopic);
    }

    /**
     * Bean for Kafka listener container factory with record acknowledgment.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setConcurrency(consumerConcurrency);

        // The first parameter is the interval between retries in milliseconds
        // The second parameter is the maximum number of attempts (UNLIMITED = -1)
        FixedBackOff fixedBackOff = new FixedBackOff(delay, maxRetryAttempts - 1);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(fixedBackOff);
        
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
