package com.linkedin.metadata.examples.cli.config;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class KafkaConsumerConfig {
  @Value("${KAFKA_BOOTSTRAP_SERVER:localhost:9092}")
  private String kafkaBootstrapServers;

  @Value("${KAFKA_SCHEMAREGISTRY_URL:http://localhost:8081}")
  private String kafkaSchemaRegistryUrl;

  @Bean(name = "kafkaEventConsumer")
  public Consumer<String, GenericRecord> kafkaConsumerFactory(KafkaProperties properties) {
    KafkaProperties.Consumer consumerProps = properties.getConsumer();

    consumerProps.setKeyDeserializer(StringDeserializer.class);
    consumerProps.setValueDeserializer(KafkaAvroDeserializer.class);
    consumerProps.setGroupId("mce-cli");

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaBootstrapServers != null && kafkaBootstrapServers.length() > 0) {
      consumerProps.setBootstrapServers(Arrays.asList(kafkaBootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> props = properties.buildConsumerProperties();
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);

    return new KafkaConsumer<>(props);
  }
}
