package com.linkedin.metadata.kafka.listener.mcl;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.trace.TraceServiceImpl;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.context.TraceIdGenerator;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.slf4j.MDC;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MCLKafkaListenerTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,test.dataset,PROD)";
  private static final String TEST_ASPECT_NAME = "datasetProperties";
  private static final String TEST_ENTITY_TYPE = "dataset";
  private static final String TEST_TOPIC = "MetadataChangeLog_v1";
  private static final String TEST_CONSUMER_GROUP = "mcl-consumer-group";
  private static final long TEST_TIMESTAMP = System.currentTimeMillis();

  @Mock private MetadataChangeLogHook mockHook1;
  @Mock private MetadataChangeLogHook mockHook2;
  @Mock private ConsumerRecord<String, GenericRecord> mockConsumerRecord;
  @Mock private GenericRecord mockGenericRecord;
  @Mock private MetricUtils metricUtils;
  @Mock private SystemMetadata mockSystemMetadata;

  private MCLKafkaListener listener;
  private OperationContext systemOperationContext;
  private SimpleMeterRegistry meterRegistry;

  @BeforeMethod
  public void setUp() throws URISyntaxException {
    MockitoAnnotations.openMocks(this);
    MDC.clear();

    mockSystemMetadata = spy(SystemMetadataUtils.createDefaultSystemMetadata());
    meterRegistry = new SimpleMeterRegistry();
    when(metricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    systemOperationContext =
        TestOperationContexts.Builder.builder()
            .systemTelemetryContextSupplier(
                () ->
                    SystemTelemetryContext.builder()
                        .tracer(SystemTelemetryContext.TEST.getTracer())
                        .metricUtils(metricUtils)
                        .build())
            .buildSystemContext();

    listener = new MCLKafkaListener();

    // Setup hooks
    mockHook1 = spy(new TestHook1());
    mockHook2 = spy(new TestHook2());

    // Initialize with two hooks
    Map<String, Set<String>> aspectsToDrop = new HashMap<>();
    aspectsToDrop.put("dataset", new HashSet<>(Arrays.asList("deprecation")));
    aspectsToDrop.put("*", new HashSet<>(Arrays.asList("status")));

    listener.init(
        systemOperationContext,
        TEST_CONSUMER_GROUP,
        Arrays.asList(mockHook1, mockHook2),
        true, // fineGrainedLoggingEnabled
        aspectsToDrop);

    // Setup mock consumer record
    when(mockConsumerRecord.topic()).thenReturn(TEST_TOPIC);
    when(mockConsumerRecord.partition()).thenReturn(0);
    when(mockConsumerRecord.offset()).thenReturn(12345L);
    when(mockConsumerRecord.timestamp()).thenReturn(TEST_TIMESTAMP - 2000); // 2 seconds ago
    when(mockConsumerRecord.key()).thenReturn("test-key");
    when(mockConsumerRecord.serializedValueSize()).thenReturn(1024);
    when(mockConsumerRecord.value()).thenReturn(mockGenericRecord);
  }

  @AfterMethod
  public void tearDown() {
    MDC.clear();
  }

  @Test
  public void testConsumeSuccessfulEvent() throws Exception {
    // Given
    MetadataChangeLog event = createTestMCL(ChangeType.UPSERT);

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);

      // When
      listener.consume(mockConsumerRecord);

      // Then
      verify(mockHook1).invoke(eq(event));
      verify(mockHook2).invoke(eq(event));

      // Verify metrics
      verify(metricUtils, times(1))
          .histogram(eq(MCLKafkaListener.class), eq("kafkaLag"), anyLong());
      verify(metricUtils, times(1))
          .increment(
              eq(MCLKafkaListener.class),
              eq(TEST_CONSUMER_GROUP + "_received_event_count"),
              eq(1d));
      verify(metricUtils, times(1))
          .increment(
              eq(MCLKafkaListener.class),
              eq(TEST_CONSUMER_GROUP + "_consumed_event_count"),
              eq(1d));

      // Verify no failures
      verify(metricUtils, never())
          .increment(eq(MCLKafkaListener.class), contains("failure"), anyInt());
    }
  }

  @Test
  public void testUpdateMetricsWithTraceId() throws IOException, URISyntaxException {
    // Given
    long requestEpochMillis = TEST_TIMESTAMP - 5000; // 5 seconds ago
    String traceId = new TraceIdGenerator().generateTraceId(requestEpochMillis);

    when(mockSystemMetadata.getProperties())
        .thenReturn(new StringMap(Collections.singletonMap("traceId", traceId)));

    MetadataChangeLog event = createTestMCL(ChangeType.CREATE);
    event.setSystemMetadata(mockSystemMetadata);

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class);
        MockedStatic<TraceServiceImpl> traceService = mockStatic(TraceServiceImpl.class)) {

      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);
      traceService
          .when(() -> TraceServiceImpl.extractTraceIdEpochMillis(mockSystemMetadata))
          .thenReturn(requestEpochMillis);

      // When
      listener.consume(mockConsumerRecord);

      // Then
      // Verify timer was recorded with correct hook names
      Timer timer1 =
          meterRegistry.timer(MetricUtils.DATAHUB_REQUEST_HOOK_QUEUE_TIME, "hook", "TestHook1");
      Timer timer2 =
          meterRegistry.timer(MetricUtils.DATAHUB_REQUEST_HOOK_QUEUE_TIME, "hook", "TestHook2");

      assertEquals(timer1.count(), 1);
      assertEquals(timer2.count(), 1);

      // Queue time should be at least 5 seconds
      assertTrue(timer1.totalTime(TimeUnit.MILLISECONDS) >= 5000);
      assertTrue(timer2.totalTime(TimeUnit.MILLISECONDS) >= 5000);
    }
  }

  @Test
  public void testUpdateMetricsWithoutTraceId() throws Exception {
    // Given
    MetadataChangeLog event = createTestMCL(ChangeType.DELETE);
    event.setSystemMetadata(mockSystemMetadata);

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class);
        MockedStatic<TraceServiceImpl> traceService = mockStatic(TraceServiceImpl.class)) {

      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);
      traceService
          .when(() -> TraceServiceImpl.extractTraceIdEpochMillis(any()))
          .thenReturn(null); // No trace ID

      // When
      listener.consume(mockConsumerRecord);

      // Then
      // Verify hooks were called
      verify(mockHook1).invoke(event);
      verify(mockHook2).invoke(event);

      // Verify no timer was recorded
      Timer timer1 =
          meterRegistry.timer(MetricUtils.DATAHUB_REQUEST_HOOK_QUEUE_TIME, "hook", "TestHook1");
      Timer timer2 =
          meterRegistry.timer(MetricUtils.DATAHUB_REQUEST_HOOK_QUEUE_TIME, "hook", "TestHook2");

      assertEquals(timer1.count(), 0);
      assertEquals(timer2.count(), 0);
    }
  }

  @Test
  public void testMDCContext() throws IOException, URISyntaxException {
    // Given
    MetadataChangeLog event = createTestMCL(ChangeType.RESTATE);

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);

      // When
      listener.consume(mockConsumerRecord);

      // Then - MDC should be cleared after processing
      assertNull(MDC.get(Constants.MDC_ENTITY_URN));
      assertNull(MDC.get(Constants.MDC_ASPECT_NAME));
      assertNull(MDC.get(Constants.MDC_ENTITY_TYPE));
      assertNull(MDC.get(Constants.MDC_CHANGE_TYPE));
    }
  }

  @Test
  public void testShouldSkipProcessing() throws Exception {
    // Given - event with aspect that should be dropped
    MetadataChangeLog event = createTestMCL(ChangeType.UPSERT);
    event.setAspectName("deprecation"); // This aspect is in the drop list for dataset

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);

      // When
      listener.consume(mockConsumerRecord);

      // Then - hooks should not be invoked
      verify(mockHook1, never()).invoke(any());
      verify(mockHook2, never()).invoke(any());

      // Verify metrics still recorded for received event
      verify(metricUtils)
          .increment(
              eq(MCLKafkaListener.class),
              eq(TEST_CONSUMER_GROUP + "_received_event_count"),
              eq(1d));

      // But not for consumed event
      verify(metricUtils, never())
          .increment(
              eq(MCLKafkaListener.class),
              eq(TEST_CONSUMER_GROUP + "_consumed_event_count"),
              anyInt());
    }
  }

  @Test
  public void testShouldSkipProcessingWildcard() throws Exception {
    // Given - event with aspect that should be dropped for all entity types
    MetadataChangeLog event = createTestMCL(ChangeType.UPSERT);
    event.setEntityType("chart"); // Different entity type
    event.setAspectName("status"); // This aspect is in the wildcard drop list

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);

      // When
      listener.consume(mockConsumerRecord);

      // Then - hooks should not be invoked
      verify(mockHook1, never()).invoke(any());
      verify(mockHook2, never()).invoke(any());
    }
  }

  @Test
  public void testConversionFailure() throws Exception {
    // Given
    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils
          .when(() -> EventUtils.avroToPegasusMCL(any()))
          .thenThrow(new IOException("Conversion failed"));

      // When
      listener.consume(mockConsumerRecord);

      // Then
      verify(metricUtils)
          .increment(
              eq(MCLKafkaListener.class), eq(TEST_CONSUMER_GROUP + "_conversion_failure"), eq(1d));
      verify(mockHook1, never()).invoke(any());
      verify(mockHook2, never()).invoke(any());
    }
  }

  @Test
  public void testHookFailure() throws Exception {
    // Given
    MetadataChangeLog event = createTestMCL(ChangeType.UPSERT);

    // First hook throws exception
    doThrow(new RuntimeException("Hook failed")).when(mockHook1).invoke(any());

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);

      // When
      listener.consume(mockConsumerRecord);

      // Then - second hook should still be invoked
      verify(mockHook1).invoke(event);
      verify(mockHook2).invoke(event);

      // Verify failure metric for first hook
      verify(metricUtils).increment(eq(MCLKafkaListener.class), eq("TestHook1_failure"), eq(1d));

      // But overall consumed event count should still increment
      verify(metricUtils)
          .increment(
              eq(MCLKafkaListener.class),
              eq(TEST_CONSUMER_GROUP + "_consumed_event_count"),
              eq(1d));
    }
  }

  @Test
  public void testFineGrainedLoggingAttributes() throws IOException, URISyntaxException {
    // Given
    MetadataChangeLog event = createTestMCL(ChangeType.PATCH);

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);

      // When
      listener.consume(mockConsumerRecord);

      // Then
      List<String> attributes = listener.getFineGrainedLoggingAttributes(event);

      assertTrue(attributes.contains(MetricUtils.ASPECT_NAME));
      assertTrue(attributes.contains(TEST_ASPECT_NAME));
      assertTrue(attributes.contains(MetricUtils.ENTITY_TYPE));
      assertTrue(attributes.contains(TEST_ENTITY_TYPE));
      assertTrue(attributes.contains(MetricUtils.CHANGE_TYPE));
      assertTrue(attributes.contains(ChangeType.PATCH.name()));
    }
  }

  @Test
  public void testFineGrainedLoggingDisabled() throws IOException, URISyntaxException {
    // Given - reinitialize with fine-grained logging disabled
    listener.init(
        systemOperationContext,
        TEST_CONSUMER_GROUP,
        Arrays.asList(mockHook1, mockHook2),
        false, // fineGrainedLoggingEnabled = false
        new HashMap<>());

    MetadataChangeLog event = createTestMCL(ChangeType.UPSERT);

    // When
    List<String> attributes = listener.getFineGrainedLoggingAttributes(event);

    // Then
    assertTrue(attributes.isEmpty());
  }

  @Test
  public void testGetEventDisplayString() throws URISyntaxException {
    // Given
    MetadataChangeLog event = createTestMCL(ChangeType.CREATE);

    // When
    String displayString = listener.getEventDisplayString(event);

    // Then
    assertTrue(displayString.contains(TEST_ENTITY_URN));
    assertTrue(displayString.contains(TEST_ASPECT_NAME));
    assertTrue(displayString.contains(TEST_ENTITY_TYPE));
    assertTrue(displayString.contains(ChangeType.CREATE.toString()));
  }

  @Test
  public void testGetEventDisplayStringWithNullFields() throws URISyntaxException {
    // Given
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(Urn.createFromString(TEST_ENTITY_URN));
    // Don't set other fields

    // When
    String displayString = listener.getEventDisplayString(event);

    // Then
    assertTrue(displayString.contains(TEST_ENTITY_URN));
    assertTrue(displayString.contains("null"));
  }

  @Test
  public void testKafkaQueueTimeMetric() throws IOException, URISyntaxException {
    // Given
    MetadataChangeLog event = createTestMCL(ChangeType.UPSERT);

    long queueTime = 3000; // 3 seconds
    when(mockConsumerRecord.timestamp()).thenReturn(System.currentTimeMillis() - queueTime);

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);

      // When
      listener.consume(mockConsumerRecord);

      // Then
      Timer timer =
          meterRegistry.timer(
              MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
              "topic",
              TEST_TOPIC,
              "consumer.group",
              TEST_CONSUMER_GROUP);

      assertEquals(timer.count(), 1);
      assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 2500);
      assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) <= 3500);
    }
  }

  @Test
  public void testMultipleEventsMetrics() throws IOException, URISyntaxException {
    // Given
    long[] queueTimes = {1000, 2000, 3000, 4000, 5000};

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class);
        MockedStatic<TraceServiceImpl> traceService = mockStatic(TraceServiceImpl.class)) {

      for (int i = 0; i < queueTimes.length; i++) {
        MetadataChangeLog event = createTestMCL(ChangeType.UPSERT);

        // Set up trace ID for some events (indices 0, 2, 4)
        if (i % 2 == 0) {
          long requestTime = TEST_TIMESTAMP - queueTimes[i] - 1000; // Add extra delay
          SystemMetadata systemMetadataWithTrace =
              spy(SystemMetadataUtils.createDefaultSystemMetadata());
          event.setSystemMetadata(systemMetadataWithTrace);

          // Configure the mock to return the request time for this specific system metadata
          traceService
              .when(() -> TraceServiceImpl.extractTraceIdEpochMillis(systemMetadataWithTrace))
              .thenReturn(requestTime);
        } else {
          // For odd indices, ensure no trace ID is extracted
          SystemMetadata systemMetadataNoTrace =
              spy(SystemMetadataUtils.createDefaultSystemMetadata());
          event.setSystemMetadata(systemMetadataNoTrace);
          traceService
              .when(() -> TraceServiceImpl.extractTraceIdEpochMillis(systemMetadataNoTrace))
              .thenReturn(null);
        }

        eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);

        ConsumerRecord<String, GenericRecord> record = mock(ConsumerRecord.class);
        when(record.topic()).thenReturn(TEST_TOPIC);
        when(record.partition()).thenReturn(0);
        when(record.offset()).thenReturn(12345L + i);
        when(record.timestamp()).thenReturn(System.currentTimeMillis() - queueTimes[i]);
        when(record.key()).thenReturn("test-key-" + i);
        when(record.serializedValueSize()).thenReturn(1024);
        when(record.value()).thenReturn(mockGenericRecord);

        // When
        listener.consume(record);
      }

      // Then
      Timer kafkaTimer =
          meterRegistry.timer(
              MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
              "topic",
              TEST_TOPIC,
              "consumer.group",
              TEST_CONSUMER_GROUP);

      assertEquals(kafkaTimer.count(), queueTimes.length);

      // Check request queue time metrics (only for events with trace IDs)
      Timer requestTimer1 =
          meterRegistry.timer(MetricUtils.DATAHUB_REQUEST_HOOK_QUEUE_TIME, "hook", "TestHook1");
      Timer requestTimer2 =
          meterRegistry.timer(MetricUtils.DATAHUB_REQUEST_HOOK_QUEUE_TIME, "hook", "TestHook2");

      assertEquals(requestTimer1.count(), 3); // Events at index 0, 2, 4
      assertEquals(requestTimer2.count(), 3);
    }
  }

  @Test
  public void testNoMetricsWhenMetricUtilsNotPresent() throws Exception {
    // Given - create context without metrics
    systemOperationContext =
        TestOperationContexts.Builder.builder()
            .systemTelemetryContextSupplier(
                () ->
                    SystemTelemetryContext.builder()
                        .tracer(SystemTelemetryContext.TEST.getTracer())
                        .metricUtils(null)
                        .build())
            .buildSystemContext();

    listener = new MCLKafkaListener();
    listener.init(
        systemOperationContext,
        TEST_CONSUMER_GROUP,
        Arrays.asList(mockHook1, mockHook2),
        true,
        new HashMap<>());

    MetadataChangeLog event = createTestMCL(ChangeType.UPSERT);

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(event);

      // When
      listener.consume(mockConsumerRecord);

      // Then - hooks should still be called
      verify(mockHook1).invoke(event);
      verify(mockHook2).invoke(event);

      // But no metrics interactions
      verify(metricUtils, never()).histogram(any(), any(), anyLong());
      verify(metricUtils, never()).increment(any(), any(), anyInt());
    }
  }

  // Helper method to create test MCL
  private MetadataChangeLog createTestMCL(ChangeType changeType) throws URISyntaxException {
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(Urn.createFromString(TEST_ENTITY_URN));
    mcl.setAspectName(TEST_ASPECT_NAME);
    mcl.setEntityType(TEST_ENTITY_TYPE);
    mcl.setChangeType(changeType);
    mcl.setSystemMetadata(new SystemMetadata());
    return mcl;
  }

  // Test hook classes for mocking
  static class TestHook1 implements MetadataChangeLogHook {
    @Nonnull
    @Override
    public String getConsumerGroupSuffix() {
      return "";
    }

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public void invoke(MetadataChangeLog event) {}
  }

  static class TestHook2 implements MetadataChangeLogHook {
    @Nonnull
    @Override
    public String getConsumerGroupSuffix() {
      return "";
    }

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public void invoke(MetadataChangeLog event) {}
  }
}
