package com.linkedin.metadata.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import com.linkedin.metadata.kafka.MetadataChangeEventsProcessor;
import com.linkedin.mxe.Topics;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {

    @Value("${KAFKA_BOOTSTRAP_SERVER:localhost:9092}")
    private String kafkaBootstrapServer;
    @Value("${KAFKA_SCHEMAREGISTRY_URL:http://localhost:8081}")
    private String kafkaSchemaRegistryUrl;
    @Value("${KAFKA_MCE_TOPIC_NAME:" + Topics.METADATA_CHANGE_EVENT + "}")
    private String mceTopicName;

    private final MetadataChangeEventsProcessor eventsProcessor;

    public KafkaStreamsConfig(MetadataChangeEventsProcessor eventsProcessor) {
        this.eventsProcessor = eventsProcessor;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mce-consuming-job");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "mce-consuming-job-client");
        // Where to find Kafka broker(s).
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        // Specify default (de)serializers for record keys and for record values.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafkaSchemaRegistryUrl);
        // Continue handling events after exception
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        // Records will be flushed every 10 seconds.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
        // Disable record caches.
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, GenericRecord> kStream(StreamsBuilder kStreamBuilder) {
        final KStream<String, GenericRecord> messages = kStreamBuilder.stream(mceTopicName);
        messages.foreach((k, v) -> eventsProcessor.processSingleMCE(v));
        return messages;
    }

}
