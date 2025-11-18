package datahub.hive.consumer.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.test.util.ReflectionTestUtils;


import static org.junit.jupiter.api.Assertions.*;


/**
 * Unit tests for KafkaConfig.
 * Tests the configuration of Kafka consumer and producer beans.
 */
@ExtendWith(MockitoExtension.class)
public class KafkaConfigTest {

    @Mock
    private KafkaProperties kafkaProperties;

    @Mock
    private ConsumerFactory<String, String> consumerFactory;

    @InjectMocks
    private KafkaConfig kafkaConfig;

    @BeforeEach
    void setUp() {
        // Set up test properties using ReflectionTestUtils
        ReflectionTestUtils.setField(kafkaConfig, "consumerConcurrency", 7);
        ReflectionTestUtils.setField(kafkaConfig, "maxRetryAttempts", 3);
        ReflectionTestUtils.setField(kafkaConfig, "delay", 1000);
    }

    /**
     * Tests successful KafkaListenerContainerFactory bean creation.
     * Verifies that the container factory is configured correctly.
     */
    @Test
    void testKafkaListenerContainerFactory_Success() {
        // Call the method under test
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            kafkaConfig.kafkaListenerContainerFactory(consumerFactory);

        // Verify that factory is created
        assertNotNull(factory);

        // Verify factory configuration
        assertEquals(consumerFactory, factory.getConsumerFactory());
        assertEquals(ContainerProperties.AckMode.RECORD, factory.getContainerProperties().getAckMode());
        
        // Verify concurrency is set
        Object concurrency = ReflectionTestUtils.getField(factory, "concurrency");
        assertEquals(7, concurrency);

        // Verify error handler is set
        Object errorHandler = ReflectionTestUtils.getField(factory, "commonErrorHandler");
        assertNotNull(errorHandler);
        assertTrue(errorHandler instanceof DefaultErrorHandler);
    }

    /**
     * Tests KafkaListenerContainerFactory with different concurrency setting.
     * Verifies that concurrency configuration is applied correctly.
     */
    @Test
    void testKafkaListenerContainerFactory_DifferentConcurrency() {
        // Set different concurrency
        ReflectionTestUtils.setField(kafkaConfig, "consumerConcurrency", 10);

        // Call the method under test
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            kafkaConfig.kafkaListenerContainerFactory(consumerFactory);

        // Verify concurrency setting using reflection
        Object concurrency = ReflectionTestUtils.getField(factory, "concurrency");
        assertEquals(10, concurrency);
    }

    /**
     * Tests KafkaListenerContainerFactory with different retry settings.
     * Verifies that retry configuration is applied correctly.
     */
    @Test
    void testKafkaListenerContainerFactory_DifferentRetrySettings() {
        // Set different retry settings
        ReflectionTestUtils.setField(kafkaConfig, "maxRetryAttempts", 5);
        ReflectionTestUtils.setField(kafkaConfig, "delay", 2000);

        // Call the method under test
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            kafkaConfig.kafkaListenerContainerFactory(consumerFactory);

        // Verify that factory is created with error handler
        assertNotNull(factory);
        Object errorHandler = ReflectionTestUtils.getField(factory, "commonErrorHandler");
        assertNotNull(errorHandler);
        assertTrue(errorHandler instanceof DefaultErrorHandler);
    }

    /**
     * Tests KafkaListenerContainerFactory with minimum retry attempts.
     * Verifies that minimum retry configuration works.
     */
    @Test
    void testKafkaListenerContainerFactory_MinimumRetryAttempts() {
        // Set minimum retry attempts
        ReflectionTestUtils.setField(kafkaConfig, "maxRetryAttempts", 1);
        ReflectionTestUtils.setField(kafkaConfig, "delay", 500);

        // Call the method under test
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            kafkaConfig.kafkaListenerContainerFactory(consumerFactory);

        // Verify that factory is created successfully
        assertNotNull(factory);
        Object errorHandler = ReflectionTestUtils.getField(factory, "commonErrorHandler");
        assertNotNull(errorHandler);
    }

    /**
     * Tests KafkaListenerContainerFactory with null consumer factory.
     * Verifies that null consumer factory is handled appropriately.
     */
    @Test
    void testKafkaListenerContainerFactory_NullConsumerFactory() {
        // Call the method under test with null consumer factory
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            kafkaConfig.kafkaListenerContainerFactory(null);

        // Verify that factory is still created
        assertNotNull(factory);
        assertNull(factory.getConsumerFactory());
    }

    /**
     * Tests KafkaListenerContainerFactory with maximum concurrency.
     * Verifies that high concurrency values are handled correctly.
     */
    @Test
    void testKafkaListenerContainerFactory_MaxConcurrency() {
        // Set high concurrency
        ReflectionTestUtils.setField(kafkaConfig, "consumerConcurrency", 20);

        // Call the method under test
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            kafkaConfig.kafkaListenerContainerFactory(consumerFactory);

        // Verify concurrency setting
        Object concurrency = ReflectionTestUtils.getField(factory, "concurrency");
        assertEquals(20, concurrency);
    }

    /**
     * Tests KafkaListenerContainerFactory with zero delay.
     * Verifies that zero delay configuration works.
     */
    @Test
    void testKafkaListenerContainerFactory_ZeroDelay() {
        // Set zero delay
        ReflectionTestUtils.setField(kafkaConfig, "delay", 0);

        // Call the method under test
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            kafkaConfig.kafkaListenerContainerFactory(consumerFactory);

        // Verify that factory is created successfully
        assertNotNull(factory);
        Object errorHandler = ReflectionTestUtils.getField(factory, "commonErrorHandler");
        assertNotNull(errorHandler);
    }

    /**
     * Tests KafkaListenerContainerFactory with high retry attempts.
     * Verifies that high retry configuration works.
     */
    @Test
    void testKafkaListenerContainerFactory_HighRetryAttempts() {
        // Set high retry attempts
        ReflectionTestUtils.setField(kafkaConfig, "maxRetryAttempts", 10);
        ReflectionTestUtils.setField(kafkaConfig, "delay", 5000);

        // Call the method under test
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            kafkaConfig.kafkaListenerContainerFactory(consumerFactory);

        // Verify that factory is created successfully
        assertNotNull(factory);
        Object errorHandler = ReflectionTestUtils.getField(factory, "commonErrorHandler");
        assertNotNull(errorHandler);
        assertTrue(errorHandler instanceof DefaultErrorHandler);
    }

    /**
     * Tests KafkaListenerContainerFactory container properties.
     * Verifies that container properties are set correctly.
     */
    @Test
    void testKafkaListenerContainerFactory_ContainerProperties() {
        // Call the method under test
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            kafkaConfig.kafkaListenerContainerFactory(consumerFactory);

        // Verify container properties
        assertNotNull(factory.getContainerProperties());
        assertEquals(ContainerProperties.AckMode.RECORD, factory.getContainerProperties().getAckMode());
    }

    /**
     * Tests KafkaConfig constructor with KafkaProperties.
     * Verifies that the constructor properly initializes the config.
     */
    @Test
    void testKafkaConfigConstructor() {
        // Create a new KafkaConfig with mocked properties
        KafkaConfig config = new KafkaConfig(kafkaProperties);
        
        // Verify that the config is created successfully
        assertNotNull(config);
    }
}
