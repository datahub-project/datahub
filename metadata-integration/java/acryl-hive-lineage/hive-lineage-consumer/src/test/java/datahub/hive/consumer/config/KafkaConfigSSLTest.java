package datahub.hive.consumer.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for KafkaConfig focusing on kafkaEmitter() method coverage.
 */
public class KafkaConfigSSLTest {

    /**
     * Tests kafkaEmitter() method with multiple bootstrap servers.
     */
    @Test
    void testKafkaEmitter_MultipleBootstrapServers() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        // Multiple bootstrap servers
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList(
            "broker1:9092", "broker2:9092", "broker3:9092"
        ));
        
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("schema.registry.url", "http://localhost:8081");
        producerProps.put("mcp.topic", "MetadataChangeProposal_v1");
        kafkaProperties.getProducer().getProperties().putAll(producerProps);
        
        kafkaProperties.getSecurity().setProtocol("PLAINTEXT");
        
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }

    /**
     * Tests kafkaEmitter() method with single bootstrap server.
     */
    @Test
    void testKafkaEmitter_SingleBootstrapServer() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList("single-broker:9092"));
        
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("schema.registry.url", "http://localhost:8081");
        producerProps.put("mcp.topic", "MetadataChangeProposal_v1");
        kafkaProperties.getProducer().getProperties().putAll(producerProps);
        
        kafkaProperties.getSecurity().setProtocol("PLAINTEXT");
        
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }

    /**
     * Tests kafkaEmitter() method with different schema registry URLs.
     */
    @Test
    void testKafkaEmitter_DifferentSchemaRegistryUrl() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList("localhost:9092"));
        
        // Different schema registry URL
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("schema.registry.url", "http://schema-registry:8081");
        producerProps.put("mcp.topic", "MetadataChangeProposal_v1");
        kafkaProperties.getProducer().getProperties().putAll(producerProps);
        
        kafkaProperties.getSecurity().setProtocol("PLAINTEXT");
        
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }

    /**
     * Tests kafkaEmitter() method with different MCP topic.
     */
    @Test
    void testKafkaEmitter_DifferentMcpTopic() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList("localhost:9092"));
        
        // Different MCP topic
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("schema.registry.url", "http://localhost:8081");
        producerProps.put("mcp.topic", "CustomMetadataChangeProposal");
        kafkaProperties.getProducer().getProperties().putAll(producerProps);
        
        kafkaProperties.getSecurity().setProtocol("PLAINTEXT");
        
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }

    /**
     * Tests kafkaEmitter() method with additional producer properties.
     */
    @Test
    void testKafkaEmitter_AdditionalProducerProperties() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList("localhost:9092"));
        
        // Multiple producer properties
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("schema.registry.url", "http://localhost:8081");
        producerProps.put("mcp.topic", "MetadataChangeProposal_v1");
        producerProps.put("acks", "all");
        producerProps.put("retries", "3");
        producerProps.put("batch.size", "16384");
        kafkaProperties.getProducer().getProperties().putAll(producerProps);
        
        kafkaProperties.getSecurity().setProtocol("PLAINTEXT");
        
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }

    /**
     * Tests kafkaEmitter() method with SASL_SSL protocol.
     */
    @Test
    void testKafkaEmitter_SASLSSLProtocol() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList("localhost:9092"));
        
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("schema.registry.url", "http://localhost:8081");
        producerProps.put("mcp.topic", "MetadataChangeProposal_v1");
        kafkaProperties.getProducer().getProperties().putAll(producerProps);
        
        // SASL_SSL protocol
        kafkaProperties.getSecurity().setProtocol("SASL_SSL");
        
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }
}
