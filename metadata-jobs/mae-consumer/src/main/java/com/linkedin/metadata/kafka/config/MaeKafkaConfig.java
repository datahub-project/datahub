package com.linkedin.metadata.kafka.config;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;


@Slf4j
@Configuration
public class MaeKafkaConfig {
  @Value("${KAFKA_BOOTSTRAP_SERVER:http://localhost:9092}")
  private String kafkaBootstrapServer;
  @Value("${KAFKA_SCHEMAREGISTRY_URL:http://localhost:8081}")
  private String kafkaSchemaRegistryUrl;

  @Bean(name = "avroSerializedKafkaListener")
  public KafkaListenerContainerFactory<?> avroSerializedKafkaListenerContainerFactory(KafkaProperties properties) {
    return createKafkaListenerContainerFactory(properties, KafkaAvroDeserializer.class);
  }

  @Bean(name = "stringSerializedKafkaListener")
  public KafkaListenerContainerFactory<?> stringSerializedKafkaListenerContainerFactory(KafkaProperties properties) {
    return createKafkaListenerContainerFactory(properties, StringDeserializer.class);
  }

  public KafkaListenerContainerFactory<?> createKafkaListenerContainerFactory(KafkaProperties properties, Class<?> valueDeserializer) {
    KafkaProperties.Consumer consumerProps = properties.getConsumer();

    // Specify (de)serializers for record keys and for record values.
    consumerProps.setKeyDeserializer(StringDeserializer.class);
    consumerProps.setValueDeserializer(valueDeserializer);
    // Records will be flushed every 10 seconds.
    consumerProps.setEnableAutoCommit(true);
    consumerProps.setAutoCommitInterval(Duration.ofSeconds(10));

    Map<String, Object> props = properties.buildConsumerProperties();

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaBootstrapServer != null && kafkaBootstrapServer.length() > 0) {
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(kafkaBootstrapServer.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);

    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));

    log.info("KafkaListenerContainerFactory built successfully");

    return factory;
  }
}
