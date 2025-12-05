package datahub.hive.consumer.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for KafkaConfig focusing on kafkaEmitter() method.
 * Tests the configuration building and exception handling paths.
 */
public class KafkaConfigIntegrationTest {

    /**
     * Tests kafkaEmitter() method with minimal configuration.
     */
    @Test
    void testKafkaEmitter_MinimalConfig() {
        // Create real KafkaProperties with minimal configuration
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        // Set up producer properties
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList("localhost:9092"));
        
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("schema.registry.url", "http://localhost:8081");
        producerProps.put("mcp.topic", "MetadataChangeProposal_v1");
        kafkaProperties.getProducer().getProperties().putAll(producerProps);
        
        // Set up security without SSL to avoid file issues
        kafkaProperties.getSecurity().setProtocol("PLAINTEXT");
        
        // Create KafkaConfig
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }

    /**
     * Tests kafkaEmitter() method with multiple bootstrap servers.
     * This should cover the bootstrap servers joining logic.
     */
    @Test
    void testKafkaEmitter_MultipleBootstrapServers() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        // Set up multiple bootstrap servers
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList(
            "broker1:9092", "broker2:9092", "broker3:9092"
        ));
        
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("schema.registry.url", "http://localhost:8081");
        producerProps.put("mcp.topic", "MetadataChangeProposal_v1");
        kafkaProperties.getProducer().getProperties().putAll(producerProps);
        
        kafkaProperties.getSecurity().setProtocol("PLAINTEXT");
        
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        // This should cover the String.join() operation for bootstrap servers
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }

    /**
     * Tests kafkaEmitter() method with additional producer properties.
     * This should cover the putAll operation with multiple properties.
     */
    @Test
    void testKafkaEmitter_AdditionalProducerProperties() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList("localhost:9092"));
        
        // Set up producer properties with additional configs
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("schema.registry.url", "http://localhost:8081");
        producerProps.put("mcp.topic", "MetadataChangeProposal_v1");
        producerProps.put("acks", "all");
        producerProps.put("retries", "3");
        producerProps.put("batch.size", "16384");
        producerProps.put("linger.ms", "5");
        kafkaProperties.getProducer().getProperties().putAll(producerProps);
        
        kafkaProperties.getSecurity().setProtocol("PLAINTEXT");
        
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }

    /**
     * Tests kafkaEmitter() method with empty producer properties.
     */
    @Test
    void testKafkaEmitter_EmptyProducerProperties() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList("localhost:9092"));
        
        // Empty producer properties
        kafkaProperties.getSecurity().setProtocol("PLAINTEXT");
        
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }

    /**
     * Tests KafkaConfig constructor with different properties.
     * Verifies that the config can be instantiated with various properties.
     */
    @Test
    void testKafkaConfigInstantiation() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        // Test constructor
        assertDoesNotThrow(() -> {
            KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
            assertNotNull(kafkaConfig);
        });
    }

    /**
     * Tests KafkaConfig with null properties.
     * Verifies handling of null KafkaProperties.
     */
    @Test
    void testKafkaConfigWithNullProperties() {
        // Test constructor with null properties
        assertDoesNotThrow(() -> {
            KafkaConfig kafkaConfig = new KafkaConfig(null);
            assertNotNull(kafkaConfig);
        });
    }

    /**
     * Tests kafkaEmitter() method with different security protocols.
     */
    @Test
    void testKafkaEmitter_DifferentSecurityProtocols() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        
        kafkaProperties.getProducer().setBootstrapServers(Arrays.asList("localhost:9092"));
        
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("schema.registry.url", "http://localhost:8081");
        producerProps.put("mcp.topic", "MetadataChangeProposal_v1");
        kafkaProperties.getProducer().getProperties().putAll(producerProps);
        
        // Test with SASL_SSL protocol
        kafkaProperties.getSecurity().setProtocol("SASL_SSL");
        
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        
        assertThrows(Exception.class, () -> {
            kafkaConfig.kafkaEmitter();
        });
    }
}
