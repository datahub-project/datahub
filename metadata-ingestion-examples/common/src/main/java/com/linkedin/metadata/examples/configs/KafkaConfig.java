package com.linkedin.metadata.examples.configs;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class KafkaConfig {
  @Value("${KAFKA_BOOTSTRAP_SERVER:localhost:9092}")
  private String kafkaBootstrapServers;

  @Value("${KAFKA_SCHEMAREGISTRY_URL:http://localhost:8081}")
  private String kafkaSchemaRegistryUrl;

  @Bean(name = "kafkaProducer")
  public Producer<String, GenericRecord> kafkaProducerFactory(KafkaProperties properties) {
    KafkaProperties.Producer producerProps = properties.getProducer();

    producerProps.setKeySerializer(StringSerializer.class);
    producerProps.setValueSerializer(KafkaAvroSerializer.class);

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaBootstrapServers != null && kafkaBootstrapServers.length() > 0) {
      producerProps.setBootstrapServers(Arrays.asList(kafkaBootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> props = properties.buildProducerProperties();
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);

    return new KafkaProducer<>(props);
  }
}
