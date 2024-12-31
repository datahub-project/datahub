package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
@DependsOn("configurationProvider")
public class DataHubKafkaProducerFactory {

  @Bean(name = "kafkaProducer")
  protected Producer<String, IndexedRecord> createInstance(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      final KafkaProperties properties,
      @Qualifier("schemaRegistryConfig")
          final KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig) {
    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    return new KafkaProducer<>(
        buildProducerProperties(schemaRegistryConfig, kafkaConfiguration, properties));
  }

  public static Map<String, Object> buildProducerProperties(
      KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig,
      KafkaConfiguration kafkaConfiguration,
      KafkaProperties properties) {
    KafkaProperties.Producer producerProps = properties.getProducer();

    producerProps.setKeySerializer(StringSerializer.class);
    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaConfiguration.getBootstrapServers() != null
        && kafkaConfiguration.getBootstrapServers().length() > 0) {
      producerProps.setBootstrapServers(
          Arrays.asList(kafkaConfiguration.getBootstrapServers().split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> props = properties.buildProducerProperties(null);
    props.putAll(
        kafkaConfiguration.getSerde().getEvent().getProducerProperties(schemaRegistryConfig));

    props.put(ProducerConfig.RETRIES_CONFIG, kafkaConfiguration.getProducer().getRetryCount());
    props.put(
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
        kafkaConfiguration.getProducer().getDeliveryTimeout());
    props.put(
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
        kafkaConfiguration.getProducer().getRequestTimeout());
    props.put(
        ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
        kafkaConfiguration.getProducer().getBackoffTimeout());
    props.put(
        ProducerConfig.COMPRESSION_TYPE_CONFIG,
        kafkaConfiguration.getProducer().getCompressionType());
    props.put(
        ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
        kafkaConfiguration.getProducer().getMaxRequestSize());

    // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
    props.putAll(kafkaConfiguration.getSerde().getEvent().getProperties(schemaRegistryConfig));

    return props;
  }
}
