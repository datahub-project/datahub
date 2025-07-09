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
import java.util.Optional;
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
}
