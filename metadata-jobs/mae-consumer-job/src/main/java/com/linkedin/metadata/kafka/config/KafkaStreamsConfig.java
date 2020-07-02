package com.linkedin.metadata.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import com.linkedin.metadata.kafka.MetadataAuditEventsProcessor;
import com.linkedin.mxe.Topics;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Value("${KAFKA_BOOTSTRAP_SERVER:localhost:9092}")
    private String kafkaBootstrapServer;
    @Value("${KAFKA_SCHEMAREGISTRY_URL:http://localhost:8081}")
    private String kafkaSchemaRegistryUrl;
    @Value("${KAFKA_TOPIC_NAME:" + Topics.METADATA_AUDIT_EVENT + "}")
    private String kafkaTopicName;

    private final MetadataAuditEventsProcessor eventsProcessor;

    public KafkaStreamsConfig(MetadataAuditEventsProcessor eventsProcessor) {
        this.eventsProcessor = eventsProcessor;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mae-consumer-job");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "mae-consumer-job-client");
        // Where to find Kafka broker(s).
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        // Specify default (de)serializers for record keys and for record values.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);
        // Records will be flushed every 10 seconds.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
        // Disable record caches.
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, GenericRecord> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, GenericRecord> messages = kStreamBuilder.stream(kafkaTopicName);
        messages.foreach((k, v) -> eventsProcessor.processSingleMAE(v));
        return messages;
    }

}
