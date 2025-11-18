package datahub.hive.producer;

import datahub.hive.producer.HiveLineageLogger.KafkaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for KafkaProducerService class.
 */
@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    private KafkaProducerConfig config;
    
    @Mock
    private KafkaProducer<String, String> mockProducer;

    @BeforeEach
    void setUp() {
        config = new KafkaProducerConfig(
                "localhost:9092",
                "/path/to/truststore",
                "truststore-password",
                "/path/to/keystore",
                "keystore-password",
                "test-topic",
                3,
                100,
                true,
                3000,
                3000,
                5000,
                30000
        );
    }

    @AfterEach
    void tearDown() {
        // Clean up after each test
    }

    @Test
    void testSendMessage_success() throws Exception {
        // Given
        String key = "test-key";
        String value = "{\"test\":\"value\"}";
        
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("test-topic", 0),
                0L,
                0L,
                System.currentTimeMillis(),
                0L,
                0,
                0
        );
        
        Future<RecordMetadata> future = CompletableFuture.completedFuture(metadata);
        
        try (MockedConstruction<KafkaProducer> mockedProducer = mockConstruction(KafkaProducer.class,
                (mock, context) -> {
                    when(mock.send(any(ProducerRecord.class))).thenReturn(future);
                })) {
            
            // When
            KafkaProducerService service = new KafkaProducerService(config);
            service.sendMessage(key, value);
            
            // Then
            KafkaProducer<String, String> producer = mockedProducer.constructed().get(0);
            verify(producer, times(1)).send(any(ProducerRecord.class));
        }
    }

    @Test
    void testSendMessage_failure() throws Exception {
        // Given
        String key = "test-key";
        String value = "{\"test\":\"value\"}";
        
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Kafka send failed"));
        
        try (MockedConstruction<KafkaProducer> mockedProducer = mockConstruction(KafkaProducer.class,
                (mock, context) -> {
                    when(mock.send(any(ProducerRecord.class))).thenReturn(future);
                })) {
            
            // When
            KafkaProducerService service = new KafkaProducerService(config);
            service.sendMessage(key, value);
            
            // Then - should not throw exception, just log warning
            KafkaProducer<String, String> producer = mockedProducer.constructed().get(0);
            verify(producer, times(1)).send(any(ProducerRecord.class));
        }
    }

    @Test
    void testClose_success() throws Exception {
        // Given
        try (MockedConstruction<KafkaProducer> mockedProducer = mockConstruction(KafkaProducer.class,
                (mock, context) -> {
                    doNothing().when(mock).close(any());
                })) {
            
            // When
            KafkaProducerService service = new KafkaProducerService(config);
            service.close();
            
            // Then
            KafkaProducer<String, String> producer = mockedProducer.constructed().get(0);
            verify(producer, times(1)).close(any());
        }
    }

    @Test
    void testClose_withException() throws Exception {
        // Given
        try (MockedConstruction<KafkaProducer> mockedProducer = mockConstruction(KafkaProducer.class,
                (mock, context) -> {
                    doThrow(new RuntimeException("Close failed")).when(mock).close(any());
                })) {
            
            // When
            KafkaProducerService service = new KafkaProducerService(config);
            service.close();
            
            // Then - should not throw exception, just log warning
            KafkaProducer<String, String> producer = mockedProducer.constructed().get(0);
            verify(producer, times(1)).close(any());
        }
    }

    @Test
    void testMultipleSendMessages() throws Exception {
        // Given
        String key1 = "key1";
        String value1 = "{\"test\":\"value1\"}";
        String key2 = "key2";
        String value2 = "{\"test\":\"value2\"}";
        
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("test-topic", 0),
                0L,
                0L,
                System.currentTimeMillis(),
                0L,
                0,
                0
        );
        
        Future<RecordMetadata> future = CompletableFuture.completedFuture(metadata);
        
        try (MockedConstruction<KafkaProducer> mockedProducer = mockConstruction(KafkaProducer.class,
                (mock, context) -> {
                    when(mock.send(any(ProducerRecord.class))).thenReturn(future);
                })) {
            
            // When
            KafkaProducerService service = new KafkaProducerService(config);
            service.sendMessage(key1, value1);
            service.sendMessage(key2, value2);
            
            // Then
            KafkaProducer<String, String> producer = mockedProducer.constructed().get(0);
            verify(producer, times(2)).send(any(ProducerRecord.class));
        }
    }

    @Test
    void testKafkaProducerConfig_values() {
        // Given & When
        KafkaProducerConfig testConfig = new KafkaProducerConfig(
                "broker1:9092,broker2:9092",
                "/custom/truststore",
                "custom-trust-pass",
                "/custom/keystore",
                "custom-key-pass",
                "custom-topic",
                5,
                200,
                false,
                5000,
                5000,
                10000,
                60000
        );
        
        // Then
        assert testConfig.bootstrapServers().equals("broker1:9092,broker2:9092");
        assert testConfig.truststoreLocation().equals("/custom/truststore");
        assert testConfig.truststorePassword().equals("custom-trust-pass");
        assert testConfig.keystoreLocation().equals("/custom/keystore");
        assert testConfig.keystorePassword().equals("custom-key-pass");
        assert testConfig.kafkaTopic().equals("custom-topic");
        assert testConfig.retries() == 5;
        assert testConfig.retryBackoffMs() == 200;
        assert !testConfig.enableIdempotence();
        assert testConfig.maxBlockMs() == 5000;
        assert testConfig.requestTimeoutMs() == 5000;
        assert testConfig.deliveryTimeoutMs() == 10000;
        assert testConfig.closeTimeoutMs() == 60000;
    }
}
