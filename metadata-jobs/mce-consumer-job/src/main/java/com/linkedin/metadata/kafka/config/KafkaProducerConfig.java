package com.linkedin.metadata.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;

@Configuration
@EnableKafka
public class KafkaProducerConfig {

    @Value("${KAFKA_BOOTSTRAP_SERVER:localhost:9092}")
    private String kafkaBootstrapServer;
    @Value("${KAFKA_SCHEMAREGISTRY_URL:http://localhost:8081}")
    private String kafkaSchemaRegistryUrl;

    /**
     * KafkaProducer Properties to produce FailedMetadataChangeEvent
     *
     * @return Properties producerConfig
     */
    @Bean
    public Map<String, Object> getProducerConfig() {
        final Map<String, Object> producerConfigMap = new HashMap<>();

        producerConfigMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            com.linkedin.util.Configuration.getEnvironmentVariable("KAFKA_BOOTSTRAP_SERVER", kafkaBootstrapServer));
        producerConfigMap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            com.linkedin.util.Configuration.getEnvironmentVariable("KAFKA_SCHEMAREGISTRY_URL", kafkaSchemaRegistryUrl));
        producerConfigMap.put(ProducerConfig.CLIENT_ID_CONFIG, "failed-mce-producer");
        producerConfigMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);

        return producerConfigMap;
    }

    @Bean
    public ProducerFactory<String, GenericRecord> producerFactory() {
        return new DefaultKafkaProducerFactory<>(getProducerConfig());
    }

    @Bean
    public KafkaProducer<String, GenericRecord> kafkaProducer() {
        return new KafkaProducer<>(getProducerConfig());
    }
}
