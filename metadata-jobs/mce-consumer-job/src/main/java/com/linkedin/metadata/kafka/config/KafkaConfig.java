package com.linkedin.metadata.kafka.config;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;


@Slf4j
@Configuration
public class KafkaConfig {
  @Value("${KAFKA_BOOTSTRAP_SERVER:localhost:9092}")
  private String kafkaBootstrapServer;
  @Value("${KAFKA_SCHEMAREGISTRY_URL:http://localhost:8081}")
  private String kafkaSchemaRegistryUrl;

  @Bean
  public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(KafkaProperties properties) {
    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaBootstrapServer != null && kafkaBootstrapServer.length() > 0) {
      properties.setBootstrapServers(Arrays.asList(kafkaBootstrapServer.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> consumerProps = properties.buildConsumerProperties();
    consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);

    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps));

    log.info("KafkaListenerContainerFactory built successfully");

    return factory;
  }

  @Bean
  public KafkaTemplate<String, GenericRecord> kafkaTemplate(KafkaProperties properties) {
    KafkaTemplate<String, GenericRecord>  template = new KafkaTemplate<>(
        new DefaultKafkaProducerFactory<>(properties.buildProducerProperties()));
    log.info("KafkaTemplate built successfully");
    return template;
  }

}
