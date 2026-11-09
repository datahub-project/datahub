package com.linkedin.gms.factory.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.CDC_EVENT_CONSUMER_NAME;

import com.linkedin.gms.factory.aws.AwsClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Slf4j
@Configuration
@Import(AwsClientFactory.class)
@DependsOn({"configurationProvider", "defaultAwsCredentialsProvider"})
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
    // Overrides go into the built map, never into the shared KafkaProperties bean: mutating it
    // would leak these settings into every other client built from it afterwards.
    Map<String, Object> customizedProperties = properties.buildConsumerProperties();

    // Configure (de)serializers for CDC messages - both key and value are strings
    customizedProperties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    customizedProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    // Records will be flushed every 10 seconds.
    customizedProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    customizedProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10_000);

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    String bootstrapServers =
        StringUtils.isNotBlank(kafkaConfiguration.getConsumer().getBootstrapServers())
            ? kafkaConfiguration.getConsumer().getBootstrapServers()
            : kafkaConfiguration.getBootstrapServers();
    if (StringUtils.isNotBlank(bootstrapServers)) {
      customizedProperties.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(bootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    String securityProtocol =
        StringUtils.isNotBlank(kafkaConfiguration.getConsumer().getSecurityProtocol())
            ? kafkaConfiguration.getConsumer().getSecurityProtocol()
            : null;
    if (StringUtils.isNotBlank(securityProtocol)) {
      customizedProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    }
    // Spring's per-client raw properties map was applied after the overrides above when they were
    // set on KafkaProperties; keep it winning so precedence is unchanged.
    customizedProperties.putAll(properties.getConsumer().getProperties());

    customizedProperties.put(
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        kafkaConfiguration.getConsumer().getMaxPartitionFetchBytes());
    KafkaMskIamAuth.configure(customizedProperties);

    // customizedProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Create factory using GenericRecord typing to match ThreadPoolContainerCustomizer
    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setContainerCustomizer(new ThreadPoolContainerCustomizer());
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(customizedProperties));
    factory.setAutoStartup(false);
    int authRetrySeconds = kafkaConfiguration.getConsumer().getAuthExceptionRetryIntervalSeconds();
    if (authRetrySeconds > 0) {
      factory
          .getContainerProperties()
          .setAuthExceptionRetryInterval(Duration.ofSeconds(authRetrySeconds));
    }

    log.info("CDC KafkaListenerContainerFactory built successfully for JSON CDC messages");

    return factory;
  }
}
