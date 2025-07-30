package com.linkedin.metadata.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.kafka.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.kafka.elasticsearch.JsonElasticEvent;
import com.linkedin.metadata.kafka.transformer.DataHubUsageEventTransformer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubUsageEventsProcessorTest {

  private static final String TEST_INDEX_NAME = "datahub_usage_event_test";
  private static final String TEST_TOPIC = "DataHubUsageEvent_v1";
  private static final String TEST_KEY = "test-key";
  private static final String TEST_EVENT_ID = "event-123";
  private static final long TEST_OFFSET = 12345L;
  private static final long TEST_TIMESTAMP = System.currentTimeMillis();

  @Mock private ElasticsearchConnector elasticsearchConnector;

  @Mock private DataHubUsageEventTransformer dataHubUsageEventTransformer;

  @Mock private ConsumerRecord<String, String> mockRecord;

  private OperationContext systemOperationContext;

  @Mock private MetricUtils metricUtils;

  private DataHubUsageEventsProcessor processor;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    systemOperationContext =
        TestOperationContexts.Builder.builder()
            .systemTelemetryContextSupplier(
                () ->
                    SystemTelemetryContext.builder()
                        .tracer(SystemTelemetryContext.TEST.getTracer())
                        .metricUtils(metricUtils)
                        .build())
            .buildSystemContext();

    processor =
        new DataHubUsageEventsProcessor(
            elasticsearchConnector,
            dataHubUsageEventTransformer,
            systemOperationContext.getSearchContext().getIndexConvention(),
            systemOperationContext);
  }

  @Test
  public void testConsumeSuccessfulEvent() {
    // Given
    String eventJson = "{\"type\":\"PageViewEvent\",\"timestamp\":1234567890}";
    String transformedDocument = "{\"transformed\":\"data\"}";

    when(mockRecord.key()).thenReturn(TEST_KEY);
    when(mockRecord.topic()).thenReturn(TEST_TOPIC);
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(TEST_OFFSET);
    when(mockRecord.timestamp()).thenReturn(TEST_TIMESTAMP);
    when(mockRecord.serializedValueSize()).thenReturn(100);
    when(mockRecord.value()).thenReturn(eventJson);

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(TEST_EVENT_ID, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // When
    processor.consume(mockRecord);

    // Then
    verify(metricUtils).histogram(eq(DataHubUsageEventsProcessor.class), eq("kafkaLag"), anyLong());

    ArgumentCaptor<JsonElasticEvent> eventCaptor = ArgumentCaptor.forClass(JsonElasticEvent.class);
    verify(elasticsearchConnector).feedElasticEvent(eventCaptor.capture());

    JsonElasticEvent capturedEvent = eventCaptor.getValue();
    assertEquals(capturedEvent.getIndex(), "datahub_usage_event");
    assertEquals(capturedEvent.getActionType(), ChangeType.CREATE);
    assertEquals(capturedEvent.getId(), "event-123_12345"); // event ID + last 5 digits of offset
  }

  @Test
  public void testConsumeWithFailedTransformation() {
    // Given
    String eventJson = "{\"invalid\":\"event\"}";

    when(mockRecord.key()).thenReturn(TEST_KEY);
    when(mockRecord.topic()).thenReturn(TEST_TOPIC);
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(TEST_OFFSET);
    when(mockRecord.timestamp()).thenReturn(TEST_TIMESTAMP);
    when(mockRecord.serializedValueSize()).thenReturn(50);
    when(mockRecord.value()).thenReturn(eventJson);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.empty());

    // When
    processor.consume(mockRecord);

    // Then
    verify(metricUtils).histogram(eq(DataHubUsageEventsProcessor.class), eq("kafkaLag"), anyLong());
    verify(elasticsearchConnector, never()).feedElasticEvent(any());
  }

  @Test
  public void testConsumeWithNoMetrics() {
    // Given
    systemOperationContext =
        TestOperationContexts.Builder.builder()
            .systemTelemetryContextSupplier(
                () ->
                    systemOperationContext.getSystemTelemetryContext().toBuilder()
                        .metricUtils(null)
                        .build())
            .buildSystemContext();

    processor =
        new DataHubUsageEventsProcessor(
            elasticsearchConnector,
            dataHubUsageEventTransformer,
            systemOperationContext.getSearchContext().getIndexConvention(),
            systemOperationContext);

    String eventJson = "{\"type\":\"SearchEvent\"}";
    String transformedDocument = "{\"search\":\"data\"}";

    when(mockRecord.key()).thenReturn(TEST_KEY);
    when(mockRecord.topic()).thenReturn(TEST_TOPIC);
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(TEST_OFFSET);
    when(mockRecord.timestamp()).thenReturn(TEST_TIMESTAMP);
    when(mockRecord.serializedValueSize()).thenReturn(75);
    when(mockRecord.value()).thenReturn(eventJson);

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(TEST_EVENT_ID, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // When
    processor.consume(mockRecord);

    // Then
    verify(metricUtils, never()).histogram(any(), any(), anyLong());
    verify(elasticsearchConnector).feedElasticEvent(any());
  }

  @Test
  public void testDocumentIdGenerationWithLargeOffset() {
    // Given
    String eventJson = "{\"type\":\"Event\"}";
    String transformedDocument = "{\"data\":\"value\"}";
    long largeOffset = 9876543210L; // Last 5 digits should be 43210

    when(mockRecord.key()).thenReturn(TEST_KEY);
    when(mockRecord.topic()).thenReturn(TEST_TOPIC);
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(largeOffset);
    when(mockRecord.timestamp()).thenReturn(TEST_TIMESTAMP);
    when(mockRecord.serializedValueSize()).thenReturn(50);
    when(mockRecord.value()).thenReturn(eventJson);

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(TEST_EVENT_ID, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // When
    processor.consume(mockRecord);

    // Then
    verify(elasticsearchConnector)
        .feedElasticEvent(argThat(event -> event.getId().equals("event-123_43210")));
  }

  @Test
  public void testDocumentIdGenerationWithSmallOffset() {
    // Given
    String eventJson = "{\"type\":\"Event\"}";
    String transformedDocument = "{\"data\":\"value\"}";
    long smallOffset = 42L; // Should be padded to 00042

    when(mockRecord.key()).thenReturn(TEST_KEY);
    when(mockRecord.topic()).thenReturn(TEST_TOPIC);
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(smallOffset);
    when(mockRecord.timestamp()).thenReturn(TEST_TIMESTAMP);
    when(mockRecord.serializedValueSize()).thenReturn(50);
    when(mockRecord.value()).thenReturn(eventJson);

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(TEST_EVENT_ID, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // When
    processor.consume(mockRecord);

    // Then
    verify(elasticsearchConnector)
        .feedElasticEvent(argThat(event -> event.getId().equals("event-123_00042")));
  }

  @Test
  public void testDocumentIdGenerationWithSpecialCharacters() {
    // Given
    String eventJson = "{\"type\":\"Event\"}";
    String transformedDocument = "{\"data\":\"value\"}";
    String eventIdWithSpecialChars = "event/123+456";

    when(mockRecord.key()).thenReturn(TEST_KEY);
    when(mockRecord.topic()).thenReturn(TEST_TOPIC);
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(TEST_OFFSET);
    when(mockRecord.timestamp()).thenReturn(TEST_TIMESTAMP);
    when(mockRecord.serializedValueSize()).thenReturn(50);
    when(mockRecord.value()).thenReturn(eventJson);

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(
            eventIdWithSpecialChars, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // When
    processor.consume(mockRecord);

    // Then
    verify(elasticsearchConnector)
        .feedElasticEvent(argThat(event -> event.getId().equals("event%2F123%2B456_12345")));
  }

  @Test
  public void testConsumeWithNullRecord() {
    // Given
    when(mockRecord.key()).thenReturn(TEST_KEY);
    when(mockRecord.topic()).thenReturn(TEST_TOPIC);
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(TEST_OFFSET);
    when(mockRecord.timestamp()).thenReturn(TEST_TIMESTAMP);
    when(mockRecord.serializedValueSize()).thenReturn(0);
    when(mockRecord.value()).thenReturn(null);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(null))
        .thenReturn(Optional.empty());

    // When
    processor.consume(mockRecord);

    // Then
    verify(elasticsearchConnector, never()).feedElasticEvent(any());
  }

  @Test
  public void testConsumeVerifyElasticEventProperties() {
    // Given
    String eventJson = "{\"type\":\"ViewEvent\"}";
    String transformedDocument = "{\"view\":\"data\"}";

    when(mockRecord.key()).thenReturn(TEST_KEY);
    when(mockRecord.topic()).thenReturn(TEST_TOPIC);
    when(mockRecord.partition()).thenReturn(2); // partition 2
    when(mockRecord.offset()).thenReturn(TEST_OFFSET);
    when(mockRecord.timestamp()).thenReturn(TEST_TIMESTAMP);
    when(mockRecord.serializedValueSize()).thenReturn(100); // serialized value size
    when(mockRecord.value()).thenReturn(eventJson);

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(TEST_EVENT_ID, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // When
    processor.consume(mockRecord);

    // Then
    ArgumentCaptor<JsonElasticEvent> eventCaptor = ArgumentCaptor.forClass(JsonElasticEvent.class);
    verify(elasticsearchConnector).feedElasticEvent(eventCaptor.capture());

    JsonElasticEvent capturedEvent = eventCaptor.getValue();
    assertEquals(capturedEvent.getIndex(), "datahub_usage_event");
    assertEquals(capturedEvent.getActionType(), ChangeType.CREATE);
  }

  @Test
  public void testKafkaLagCalculation() {
    // Given
    String eventJson = "{\"type\":\"Event\"}";
    long recordTimestamp = System.currentTimeMillis() - 5000; // 5 seconds ago

    when(mockRecord.key()).thenReturn(TEST_KEY);
    when(mockRecord.topic()).thenReturn(TEST_TOPIC);
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(TEST_OFFSET);
    when(mockRecord.timestamp()).thenReturn(recordTimestamp);
    when(mockRecord.serializedValueSize()).thenReturn(50);
    when(mockRecord.value()).thenReturn(eventJson);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.empty());

    // When
    processor.consume(mockRecord);

    // Then
    ArgumentCaptor<Long> lagCaptor = ArgumentCaptor.forClass(Long.class);
    verify(metricUtils)
        .histogram(eq(DataHubUsageEventsProcessor.class), eq("kafkaLag"), lagCaptor.capture());

    Long capturedLag = lagCaptor.getValue();
    assertTrue(capturedLag >= 5000L); // Should be at least 5 seconds
  }

  @Test
  public void testMicrometerKafkaQueueTimeMetric() {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(metricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set the consumer group ID via reflection
    setConsumerGroupId(processor, "datahub-usage-event-consumer-job-client");

    // Setup test data
    String eventJson = "{\"type\":\"PageViewEvent\",\"timestamp\":1234567890}";
    String transformedDocument = "{\"transformed\":\"data\"}";

    // Set timestamp to simulate queue time
    long messageTimestamp = System.currentTimeMillis() - 3000; // 3 seconds ago
    when(mockRecord.timestamp()).thenReturn(messageTimestamp);
    when(mockRecord.topic()).thenReturn("DataHubUsageEvent_v1");
    when(mockRecord.value()).thenReturn(eventJson);

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(TEST_EVENT_ID, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // Execute
    processor.consume(mockRecord);

    // Verify timer was recorded
    Timer timer =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "DataHubUsageEvent_v1",
            "consumer.group",
            "datahub-usage-event-consumer-job-client");

    assertNotNull(timer);
    assertEquals(timer.count(), 1);
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 2500); // At least 2.5 seconds
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) <= 3500); // At most 3.5 seconds

    // Verify the dropwizard histogram method was also called
    verify(metricUtils).histogram(eq(DataHubUsageEventsProcessor.class), eq("kafkaLag"), anyLong());

    // Verify successful processing
    verify(elasticsearchConnector).feedElasticEvent(any());
  }

  @Test
  public void testMicrometerKafkaQueueTimeWithDifferentTopics() {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(metricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set the consumer group ID
    setConsumerGroupId(processor, "datahub-usage-event-consumer-job-client");

    // Setup test data
    String eventJson = "{\"type\":\"SearchEvent\"}";
    String transformedDocument = "{\"search\":\"data\"}";

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(TEST_EVENT_ID, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // Test with first topic
    long now = System.currentTimeMillis();
    when(mockRecord.timestamp()).thenReturn(now - 2000);
    when(mockRecord.topic()).thenReturn("DataHubUsageEvent_v1");
    when(mockRecord.value()).thenReturn(eventJson);
    processor.consume(mockRecord);

    // Create second consumer record for different topic
    ConsumerRecord<String, String> mockRecord2 = mock(ConsumerRecord.class);
    when(mockRecord2.key()).thenReturn("test-key-2");
    when(mockRecord2.topic()).thenReturn("DataHubUsageEvent_v2");
    when(mockRecord2.partition()).thenReturn(0);
    when(mockRecord2.offset()).thenReturn(123L);
    when(mockRecord2.timestamp()).thenReturn(now - 5000);
    when(mockRecord2.serializedValueSize()).thenReturn(100);
    when(mockRecord2.value()).thenReturn(eventJson);

    // Test with second topic
    processor.consume(mockRecord2);

    // Verify separate timers for different topics
    Timer timer1 =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "DataHubUsageEvent_v1",
            "consumer.group",
            "datahub-usage-event-consumer-job-client");

    Timer timer2 =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "DataHubUsageEvent_v2",
            "consumer.group",
            "datahub-usage-event-consumer-job-client");

    assertEquals(timer1.count(), 1);
    assertEquals(timer2.count(), 1);

    // Verify different queue times
    assertTrue(timer1.totalTime(TimeUnit.MILLISECONDS) >= 1500);
    assertTrue(timer1.totalTime(TimeUnit.MILLISECONDS) <= 2500);

    assertTrue(timer2.totalTime(TimeUnit.MILLISECONDS) >= 4500);
    assertTrue(timer2.totalTime(TimeUnit.MILLISECONDS) <= 5500);
  }

  @Test
  public void testMicrometerMetricsWithFailedTransformation() {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(metricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set the consumer group ID
    setConsumerGroupId(processor, "datahub-usage-event-consumer-job-client");

    // Setup test data
    String eventJson = "{\"invalid\":\"event\"}";

    // Set timestamp
    long messageTimestamp = System.currentTimeMillis() - 4000; // 4 seconds ago
    when(mockRecord.timestamp()).thenReturn(messageTimestamp);
    when(mockRecord.topic()).thenReturn("DataHubUsageEvent_v1");
    when(mockRecord.value()).thenReturn(eventJson);

    // Transformation fails
    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.empty());

    // Execute
    processor.consume(mockRecord);

    // Verify timer was still recorded despite transformation failure
    Timer timer =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "DataHubUsageEvent_v1",
            "consumer.group",
            "datahub-usage-event-consumer-job-client");

    assertEquals(timer.count(), 1);
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 3500);
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) <= 4500);

    // Verify Elasticsearch was not called
    verify(elasticsearchConnector, never()).feedElasticEvent(any());
  }

  @Test
  public void testMicrometerMetricsAbsentWhenRegistryNotPresent() {
    // Configure the mock metricUtils to return empty Optional (no registry)
    when(metricUtils.getRegistry()).thenReturn(Optional.empty());

    // Set the consumer group ID
    setConsumerGroupId(processor, "datahub-usage-event-consumer-job-client");

    // Setup test data
    String eventJson = "{\"type\":\"ViewEvent\"}";
    String transformedDocument = "{\"view\":\"data\"}";

    when(mockRecord.timestamp()).thenReturn(System.currentTimeMillis() - 1000);
    when(mockRecord.value()).thenReturn(eventJson);

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(TEST_EVENT_ID, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // Execute - should not throw exception
    processor.consume(mockRecord);

    // Verify the histogram method was still called (for dropwizard metrics)
    verify(metricUtils).histogram(eq(DataHubUsageEventsProcessor.class), eq("kafkaLag"), anyLong());

    // Verify processing completed successfully despite no registry
    verify(elasticsearchConnector).feedElasticEvent(any());
  }

  @Test
  public void testMicrometerKafkaQueueTimeWithCustomConsumerGroup() {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(metricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set a custom consumer group ID
    String customConsumerGroup = "custom-usage-event-consumer";
    setConsumerGroupId(processor, customConsumerGroup);

    // Setup test data
    String eventJson = "{\"type\":\"Event\"}";
    String transformedDocument = "{\"data\":\"value\"}";

    when(mockRecord.timestamp()).thenReturn(System.currentTimeMillis() - 1500);
    when(mockRecord.topic()).thenReturn("DataHubUsageEvent_v1");
    when(mockRecord.value()).thenReturn(eventJson);

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(TEST_EVENT_ID, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // Execute
    processor.consume(mockRecord);

    // Verify timer was recorded with custom consumer group
    Timer timer =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "DataHubUsageEvent_v1",
            "consumer.group",
            customConsumerGroup);

    assertNotNull(timer);
    assertEquals(timer.count(), 1);
  }

  @Test
  public void testMicrometerKafkaQueueTimeAccuracy() {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(metricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set the consumer group ID
    setConsumerGroupId(processor, "datahub-usage-event-consumer-job-client");

    // Setup test data
    String eventJson = "{\"type\":\"Event\"}";
    String transformedDocument = "{\"data\":\"value\"}";

    DataHubUsageEventTransformer.TransformedDocument transformedDoc =
        new DataHubUsageEventTransformer.TransformedDocument(TEST_EVENT_ID, transformedDocument);

    when(dataHubUsageEventTransformer.transformDataHubUsageEvent(eventJson))
        .thenReturn(Optional.of(transformedDoc));

    // Test multiple queue times
    long[] queueTimes = {100, 500, 1000, 2000, 5000}; // milliseconds

    for (int i = 0; i < queueTimes.length; i++) {
      // Create new consumer record for each test
      ConsumerRecord<String, String> testRecord = mock(ConsumerRecord.class);
      when(testRecord.key()).thenReturn("test-key-" + i);
      when(testRecord.topic()).thenReturn("DataHubUsageEvent_v1");
      when(testRecord.partition()).thenReturn(0);
      when(testRecord.offset()).thenReturn((long) (TEST_OFFSET + i));
      when(testRecord.timestamp()).thenReturn(System.currentTimeMillis() - queueTimes[i]);
      when(testRecord.serializedValueSize()).thenReturn(100);
      when(testRecord.value()).thenReturn(eventJson);

      processor.consume(testRecord);
    }

    // Verify timer statistics
    Timer timer =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "DataHubUsageEvent_v1",
            "consumer.group",
            "datahub-usage-event-consumer-job-client");

    assertEquals(timer.count(), queueTimes.length);

    // Verify mean is reasonable (should be around (100+500+1000+2000+5000)/5 = 1720ms)
    double mean = timer.mean(TimeUnit.MILLISECONDS);
    assertTrue(mean >= 1500);
    assertTrue(mean <= 2000);

    // Verify max recorded time
    assertTrue(timer.max(TimeUnit.MILLISECONDS) >= 4500);
    assertTrue(timer.max(TimeUnit.MILLISECONDS) <= 5500);

    // Verify histogram was called for each record
    verify(metricUtils, times(queueTimes.length))
        .histogram(eq(DataHubUsageEventsProcessor.class), eq("kafkaLag"), anyLong());
  }

  // Helper method to set consumer group ID via reflection
  private void setConsumerGroupId(DataHubUsageEventsProcessor processor, String consumerGroupId) {
    try {
      java.lang.reflect.Field field =
          DataHubUsageEventsProcessor.class.getDeclaredField("datahubUsageEventConsumerGroupId");
      field.setAccessible(true);
      field.set(processor, consumerGroupId);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set datahubUsageEventConsumerGroupId field", e);
    }
  }
}
