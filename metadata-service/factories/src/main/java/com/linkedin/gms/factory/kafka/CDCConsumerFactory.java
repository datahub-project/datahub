package com.linkedin.gms.factory.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.CDC_EVENT_CONSUMER_NAME;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Slf4j
@Configuration
@DependsOn("configurationProvider")
@ConditionalOnProperty(
    name = "mclProcessing.cdcSource.enabled",
    havingValue = "true",
    matchIfMissing = true)
public class CDCConsumerFactory {

  @Bean(name = CDC_EVENT_CONSUMER_NAME)
  @com.google.common.annotations.VisibleForTesting
  KafkaListenerContainerFactory<?> createCdcConsumerFactory(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      KafkaProperties properties) {
    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    KafkaProperties.Consumer consumerProps = properties.getConsumer();

    // Configure (de)serializers for CDC messages - both key and value are strings
    consumerProps.setKeyDeserializer(StringDeserializer.class);
    consumerProps.setValueDeserializer(StringDeserializer.class);

    // Records will be flushed every 10 seconds.
    consumerProps.setEnableAutoCommit(true);
    consumerProps.setAutoCommitInterval(Duration.ofSeconds(10));

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    String bootstrapServers =
        StringUtils.isNotBlank(kafkaConfiguration.getConsumer().getBootstrapServers())
            ? kafkaConfiguration.getConsumer().getBootstrapServers()
            : kafkaConfiguration.getBootstrapServers();
    if (StringUtils.isNotBlank(bootstrapServers)) {
      consumerProps.setBootstrapServers(Arrays.asList(bootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> customizedProperties = properties.buildConsumerProperties(null);
    customizedProperties.put(
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        kafkaConfiguration.getConsumer().getMaxPartitionFetchBytes());

    // customizedProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Create factory using GenericRecord typing to match ThreadPoolContainerCustomizer
    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setContainerCustomizer(new ThreadPoolContainerCustomizer());
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(customizedProperties));
    factory.setAutoStartup(false);

    log.info("CDC KafkaListenerContainerFactory built successfully for JSON CDC messages");

    return factory;
  }
}
