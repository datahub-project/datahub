package com.linkedin.gms.factory.common;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaEventProducerFactory {

  @Value("${KAFKA_BOOTSTRAP_SERVER:http://localhost:9092}")
  private String kafkaBootstrapServers;

  @Value("${KAFKA_SCHEMAREGISTRY_URL:http://localhost:8081}")
  private String kafkaSchemaRegistryUrl;

  @Bean(name = "kafkaEventProducer")
  protected Producer<String, IndexedRecord> createInstance(KafkaProperties properties) {
    KafkaProperties.Producer producerProps = properties.getProducer();

    producerProps.setKeySerializer(StringSerializer.class);
    producerProps.setValueSerializer(KafkaAvroSerializer.class);

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaBootstrapServers != null && kafkaBootstrapServers.length() > 0) {
      producerProps.setBootstrapServers(Arrays.asList(kafkaBootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> props = properties.buildProducerProperties();
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);

    return new KafkaProducer(props);
  }
}
