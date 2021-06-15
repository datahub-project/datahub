package com.linkedin.metadata.kafka.config;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ErrorHandler;


@Slf4j
@Configuration
public class MceKafkaConfig {

  @Value("${KAFKA_BOOTSTRAP_SERVER:http://localhost:9092}")
  private String kafkaBootstrapServers;
  @Value("${KAFKA_SCHEMAREGISTRY_URL:http://localhost:8081}")
  private String kafkaSchemaRegistryUrl;

  @Bean(name = "mceKafkaContainerFactory")
  public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(KafkaProperties properties) {
    KafkaProperties.Consumer consumerProps = properties.getConsumer();

    // Specify (de)serializers for record keys and for record values.
    consumerProps.setKeyDeserializer(StringDeserializer.class);
    consumerProps.setValueDeserializer(KafkaAvroDeserializer.class);
    // Records will be flushed every 10 seconds.
    consumerProps.setEnableAutoCommit(true);
    consumerProps.setAutoCommitInterval(Duration.ofSeconds(10));

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaBootstrapServers != null && kafkaBootstrapServers.length() > 0) {
      consumerProps.setBootstrapServers(Arrays.asList(kafkaBootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> props = properties.buildConsumerProperties();
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);

    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));

    log.info("KafkaListenerContainerFactory built successfully");

    return factory;
  }

  @Bean
  public KafkaTemplate<String, GenericRecord> kafkaTemplate(KafkaProperties properties) {
    KafkaProperties.Producer producerProps = properties.getProducer();

    producerProps.setKeySerializer(StringSerializer.class);
    producerProps.setValueSerializer(KafkaAvroSerializer.class);

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaBootstrapServers != null && kafkaBootstrapServers.length() > 0) {
      producerProps.setBootstrapServers(Arrays.asList(kafkaBootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> props = properties.buildProducerProperties();
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);

    KafkaTemplate<String, GenericRecord> template =
        new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));

    log.info("KafkaTemplate built successfully");

    return template;
  }

  @Bean
  public ErrorHandler errorHandler() {
    return (e, r) -> log.error("Exception caught during Deserialization, topic: {}, partition: {},  offset: {}",
        r.topic(), r.partition(), r.offset(), e);
  }
}
