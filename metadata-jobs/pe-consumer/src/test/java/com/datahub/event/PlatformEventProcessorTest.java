package com.datahub.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.event.hook.PlatformEventHook;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.PlatformEvent;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PlatformEventProcessorTest {

  private OperationContext mockOperationContext;
  private MetricUtils mockMetricUtils;
  private PlatformEventHook mockHook1;
  private PlatformEventHook mockHook2;
  private PlatformEventHook mockDisabledHook;
  private PlatformEventProcessor processor;
  private GenericRecord mockGenericRecord;
  private PlatformEvent mockPlatformEvent;
  private ConsumerRecord<String, GenericRecord> mockConsumerRecord;

  @BeforeMethod
  public void setup() {
    // Create real operation context
    Authentication auth =
        new Authentication(new Actor(ActorType.USER, "testUser"), "test:credentials");
    OperationContext realOperationContext =
        TestOperationContexts.userContextNoSearchAuthorization(auth);

    // Create a spy of the real operation context to inject mock MetricUtils
    mockOperationContext = Mockito.spy(realOperationContext);

    // Create mock MetricUtils
    mockMetricUtils = mock(MetricUtils.class);
    when(mockOperationContext.getMetricUtils()).thenReturn(Optional.of(mockMetricUtils));

    // Create mock hooks
    mockHook1 = mock(PlatformEventHook.class);
    when(mockHook1.isEnabled()).thenReturn(true);

    mockHook2 = mock(PlatformEventHook.class);
    when(mockHook2.isEnabled()).thenReturn(true);

    mockDisabledHook = mock(PlatformEventHook.class);
    when(mockDisabledHook.isEnabled()).thenReturn(false);

    // Create mock generic record and platform event
    mockGenericRecord = mock(GenericRecord.class);
    mockPlatformEvent = mock(PlatformEvent.class);
    when(mockPlatformEvent.getName()).thenReturn("TestEvent");

    // Create mock consumer record
    mockConsumerRecord = mock(ConsumerRecord.class);
    when(mockConsumerRecord.value()).thenReturn(mockGenericRecord);
    when(mockConsumerRecord.key()).thenReturn("test-key");
    when(mockConsumerRecord.topic()).thenReturn("platform_event_topic");
    when(mockConsumerRecord.partition()).thenReturn(0);
    when(mockConsumerRecord.offset()).thenReturn(100L);
    when(mockConsumerRecord.timestamp()).thenReturn(1234567890L);
    when(mockConsumerRecord.serializedValueSize()).thenReturn(1024);

    processor =
        new PlatformEventProcessor(mockOperationContext, Arrays.asList(mockHook1, mockHook2));
  }

  @Test
  public void testConstructorFiltersEnabledHooks() {
    // Create fresh mocks for this test to avoid counting init() calls from setup()
    PlatformEventHook testHook1 = mock(PlatformEventHook.class);
    when(testHook1.isEnabled()).thenReturn(true);

    PlatformEventHook testHook2 = mock(PlatformEventHook.class);
    when(testHook2.isEnabled()).thenReturn(true);

    PlatformEventHook testDisabledHook = mock(PlatformEventHook.class);
    when(testDisabledHook.isEnabled()).thenReturn(false);

    // Create processor with mixed enabled/disabled hooks
    List<PlatformEventHook> allHooks = Arrays.asList(testHook1, testDisabledHook, testHook2);
    PlatformEventProcessor testProcessor =
        new PlatformEventProcessor(mockOperationContext, allHooks);

    // Verify only enabled hooks are kept
    assertEquals(testProcessor.getHooks().size(), 2);
    assertTrue(testProcessor.getHooks().contains(testHook1));
    assertTrue(testProcessor.getHooks().contains(testHook2));
    assertTrue(!testProcessor.getHooks().contains(testDisabledHook));

    // Verify init is called only on enabled hooks
    verify(testHook1, times(1)).init();
    verify(testHook2, times(1)).init();
    verify(testDisabledHook, never()).init();
  }

  @Test
  public void testConstructorWithNoHooks() {
    processor = new PlatformEventProcessor(mockOperationContext, Collections.emptyList());
    assertEquals(processor.getHooks().size(), 0);
  }

  @Test
  public void testConsumeSuccessful() throws Exception {
    // Setup
    List<PlatformEventHook> hooks = Arrays.asList(mockHook1, mockHook2);
    processor = new PlatformEventProcessor(mockOperationContext, hooks);

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(mockGenericRecord))
          .thenReturn(mockPlatformEvent);

      // Execute
      processor.consume(mockConsumerRecord);

      // Verify metrics
      verify(mockMetricUtils, times(1))
          .histogram(eq(PlatformEventProcessor.class), eq("kafkaLag"), anyLong());
      verify(mockMetricUtils, times(1))
          .increment(eq(PlatformEventProcessor.class), eq("received_pe_count"), eq(1d));
      verify(mockMetricUtils, times(1))
          .increment(eq(PlatformEventProcessor.class), eq("consumed_pe_count"), eq(1d));

      // Verify hooks were invoked
      verify(mockHook1, times(1)).invoke(any(OperationContext.class), eq(mockPlatformEvent));
      verify(mockHook2, times(1)).invoke(any(OperationContext.class), eq(mockPlatformEvent));
    }
  }

  @Test
  public void testConsumeWithAvroConversionFailure() throws Exception {
    // Setup
    List<PlatformEventHook> hooks = Arrays.asList(mockHook1);
    processor = new PlatformEventProcessor(mockOperationContext, hooks);

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(mockGenericRecord))
          .thenThrow(new RuntimeException("Conversion failed"));

      // Execute
      processor.consume(mockConsumerRecord);

      // Verify metrics
      verify(mockMetricUtils, times(1))
          .increment(
              eq(PlatformEventProcessor.class), eq("avro_to_pegasus_conversion_failure"), eq(1d));

      // Verify hook was NOT invoked due to conversion failure
      verify(mockHook1, never()).invoke(any(OperationContext.class), any(PlatformEvent.class));

      // Verify consumed_pe_count was NOT incremented
      verify(mockMetricUtils, never())
          .increment(eq(PlatformEventProcessor.class), eq("consumed_pe_count"), eq(1d));
    }
  }

  @Test
  public void testConsumeWithHookFailure() throws Exception {
    // Setup
    List<PlatformEventHook> hooks = Arrays.asList(mockHook1, mockHook2);
    processor = new PlatformEventProcessor(mockOperationContext, hooks);

    // Make first hook throw exception
    doThrow(new RuntimeException("Hook 1 failed"))
        .when(mockHook1)
        .invoke(any(OperationContext.class), any(PlatformEvent.class));

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(mockGenericRecord))
          .thenReturn(mockPlatformEvent);

      // Execute
      processor.consume(mockConsumerRecord);

      // Verify failure metric for hook1
      ArgumentCaptor<String> metricNameCaptor = ArgumentCaptor.forClass(String.class);
      verify(mockMetricUtils, times(3))
          .increment(eq(PlatformEventProcessor.class), metricNameCaptor.capture(), eq(1d));

      List<String> capturedMetrics = metricNameCaptor.getAllValues();
      assertTrue(capturedMetrics.contains("received_pe_count"));
      assertTrue(capturedMetrics.stream().anyMatch(m -> m.contains("_failure")));
      assertTrue(capturedMetrics.contains("consumed_pe_count"));

      // Verify both hooks were invoked (failure in hook1 doesn't stop hook2)
      verify(mockHook1, times(1)).invoke(any(OperationContext.class), eq(mockPlatformEvent));
      verify(mockHook2, times(1)).invoke(any(OperationContext.class), eq(mockPlatformEvent));
    }
  }

  @Test
  public void testConsumeWithAllHooksFailing() throws Exception {
    // Setup
    List<PlatformEventHook> hooks = Arrays.asList(mockHook1, mockHook2);
    processor = new PlatformEventProcessor(mockOperationContext, hooks);

    // Make both hooks throw exceptions
    doThrow(new RuntimeException("Hook 1 failed"))
        .when(mockHook1)
        .invoke(any(OperationContext.class), any(PlatformEvent.class));
    doThrow(new RuntimeException("Hook 2 failed"))
        .when(mockHook2)
        .invoke(any(OperationContext.class), any(PlatformEvent.class));

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(mockGenericRecord))
          .thenReturn(mockPlatformEvent);

      // Execute
      processor.consume(mockConsumerRecord);

      // Verify failure metrics for both hooks
      ArgumentCaptor<String> metricNameCaptor = ArgumentCaptor.forClass(String.class);
      verify(mockMetricUtils, times(4))
          .increment(eq(PlatformEventProcessor.class), metricNameCaptor.capture(), eq(1d));

      List<String> capturedMetrics = metricNameCaptor.getAllValues();
      assertTrue(capturedMetrics.contains("received_pe_count"));
      assertTrue(capturedMetrics.stream().filter(m -> m.contains("_failure")).count() == 2);
      assertTrue(capturedMetrics.contains("consumed_pe_count"));

      // Verify both hooks were still invoked
      verify(mockHook1, times(1)).invoke(any(OperationContext.class), eq(mockPlatformEvent));
      verify(mockHook2, times(1)).invoke(any(OperationContext.class), eq(mockPlatformEvent));
    }
  }

  @Test
  public void testConsumeWithNullMetricUtils() throws Exception {
    // Setup operation context without metric utils
    when(mockOperationContext.getMetricUtils()).thenReturn(Optional.empty());

    List<PlatformEventHook> hooks = Arrays.asList(mockHook1);
    processor = new PlatformEventProcessor(mockOperationContext, hooks);

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(mockGenericRecord))
          .thenReturn(mockPlatformEvent);

      // Execute - should not throw even without metrics
      processor.consume(mockConsumerRecord);

      // Verify hook was still invoked
      verify(mockHook1, times(1)).invoke(any(OperationContext.class), eq(mockPlatformEvent));
    }
  }

  @Test
  public void testConsumeVerifiesKafkaRecordDetails() throws Exception {
    // Setup
    List<PlatformEventHook> hooks = Arrays.asList(mockHook1);
    processor = new PlatformEventProcessor(mockOperationContext, hooks);

    // Set up specific consumer record values
    String expectedKey = "test-key-123";
    String expectedTopic = "platform_event_topic_v2";
    int expectedPartition = 3;
    long expectedOffset = 999L;
    long expectedTimestamp = 1234567890L;
    int expectedValueSize = 2048;

    ConsumerRecord<String, GenericRecord> specificMockRecord = mock(ConsumerRecord.class);
    when(specificMockRecord.value()).thenReturn(mockGenericRecord);
    when(specificMockRecord.key()).thenReturn(expectedKey);
    when(specificMockRecord.topic()).thenReturn(expectedTopic);
    when(specificMockRecord.partition()).thenReturn(expectedPartition);
    when(specificMockRecord.offset()).thenReturn(expectedOffset);
    when(specificMockRecord.timestamp()).thenReturn(expectedTimestamp);
    when(specificMockRecord.serializedValueSize()).thenReturn(expectedValueSize);

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(mockGenericRecord))
          .thenReturn(mockPlatformEvent);

      // Execute
      processor.consume(specificMockRecord);

      // Verify lag calculation uses the correct timestamp
      ArgumentCaptor<Long> lagCaptor = ArgumentCaptor.forClass(Long.class);
      verify(mockMetricUtils)
          .histogram(eq(PlatformEventProcessor.class), eq("kafkaLag"), lagCaptor.capture());

      // Lag should be calculated as current time - record timestamp
      long capturedLag = lagCaptor.getValue();
      assertTrue(capturedLag >= 0, "Lag should be non-negative");

      // Verify that the consumer record methods were called
      // Note: some methods may be called multiple times (e.g., for logging and metrics)
      verify(specificMockRecord, times(1)).key();
      verify(specificMockRecord, times(1)).topic();
      verify(specificMockRecord, times(1)).partition();
      verify(specificMockRecord, times(1)).offset();
      verify(specificMockRecord, times(2))
          .timestamp(); // Called twice: once for lag calculation, once for logging
      verify(specificMockRecord, times(1)).serializedValueSize();
      verify(specificMockRecord, times(1)).value(); // Called to get the GenericRecord
    }
  }

  @Test
  public void testMultipleEventsProcessedSequentially() throws Exception {
    // Setup
    List<PlatformEventHook> hooks = Arrays.asList(mockHook1);
    processor = new PlatformEventProcessor(mockOperationContext, hooks);

    PlatformEvent event1 = mock(PlatformEvent.class);
    when(event1.getName()).thenReturn("Event1");

    PlatformEvent event2 = mock(PlatformEvent.class);
    when(event2.getName()).thenReturn("Event2");

    GenericRecord record1 = mock(GenericRecord.class);
    GenericRecord record2 = mock(GenericRecord.class);

    ConsumerRecord<String, GenericRecord> consumerRecord1 = mock(ConsumerRecord.class);
    when(consumerRecord1.value()).thenReturn(record1);
    when(consumerRecord1.key()).thenReturn("key1");
    when(consumerRecord1.topic()).thenReturn("platform_event_topic");
    when(consumerRecord1.partition()).thenReturn(0);
    when(consumerRecord1.offset()).thenReturn(100L);
    when(consumerRecord1.timestamp()).thenReturn(1234567890L);
    when(consumerRecord1.serializedValueSize()).thenReturn(512);

    ConsumerRecord<String, GenericRecord> consumerRecord2 = mock(ConsumerRecord.class);
    when(consumerRecord2.value()).thenReturn(record2);
    when(consumerRecord2.key()).thenReturn("key2");
    when(consumerRecord2.topic()).thenReturn("platform_event_topic");
    when(consumerRecord2.partition()).thenReturn(0);
    when(consumerRecord2.offset()).thenReturn(101L);
    when(consumerRecord2.timestamp()).thenReturn(1234567891L);
    when(consumerRecord2.serializedValueSize()).thenReturn(768);

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils.when(() -> EventUtils.avroToPegasusPE(record1)).thenReturn(event1);
      mockedEventUtils.when(() -> EventUtils.avroToPegasusPE(record2)).thenReturn(event2);

      // Execute
      processor.consume(consumerRecord1);
      processor.consume(consumerRecord2);

      // Verify both events were processed
      verify(mockHook1, times(1)).invoke(any(OperationContext.class), eq(event1));
      verify(mockHook1, times(1)).invoke(any(OperationContext.class), eq(event2));

      // Verify metrics were incremented twice
      verify(mockMetricUtils, times(2))
          .increment(eq(PlatformEventProcessor.class), eq("received_pe_count"), eq(1d));
      verify(mockMetricUtils, times(2))
          .increment(eq(PlatformEventProcessor.class), eq("consumed_pe_count"), eq(1d));
    }
  }

  @Test
  public void testConsumeWithNullGenericRecord() throws Exception {
    // Setup
    List<PlatformEventHook> hooks = Arrays.asList(mockHook1);
    processor = new PlatformEventProcessor(mockOperationContext, hooks);

    ConsumerRecord<String, GenericRecord> nullValueRecord = mock(ConsumerRecord.class);
    when(nullValueRecord.value()).thenReturn(null);
    when(nullValueRecord.key()).thenReturn("test-key");
    when(nullValueRecord.topic()).thenReturn("platform_event_topic");
    when(nullValueRecord.partition()).thenReturn(0);
    when(nullValueRecord.offset()).thenReturn(100L);
    when(nullValueRecord.timestamp()).thenReturn(1234567890L);
    when(nullValueRecord.serializedValueSize()).thenReturn(0);

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(null))
          .thenThrow(new NullPointerException("Record is null"));

      // Execute
      processor.consume(nullValueRecord);

      // Verify conversion failure metric
      verify(mockMetricUtils, times(1))
          .increment(eq(PlatformEventProcessor.class), eq("null_record"), eq(1d));

      // Verify hook was NOT invoked
      verify(mockHook1, never()).invoke(any(OperationContext.class), any(PlatformEvent.class));
    }
  }

  @Test
  public void testMicrometerKafkaQueueTimeMetric() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set the consumer group ID via reflection
    setConsumerGroupId(processor, "generic-platform-event-job-client");

    // Set timestamp to simulate queue time
    long messageTimestamp = System.currentTimeMillis() - 3000; // 3 seconds ago
    when(mockConsumerRecord.timestamp()).thenReturn(messageTimestamp);
    when(mockConsumerRecord.topic()).thenReturn("PlatformEvent_v1");

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(mockGenericRecord))
          .thenReturn(mockPlatformEvent);

      // Execute
      processor.consume(mockConsumerRecord);

      // Verify timer was recorded
      Timer timer =
          meterRegistry.timer(
              MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
              "topic",
              "PlatformEvent_v1",
              "consumer.group",
              "generic-platform-event-job-client");

      assertNotNull(timer);
      assertEquals(timer.count(), 1);
      assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 2500); // At least 2.5 seconds
      assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) <= 3500); // At most 3.5 seconds

      // Verify the dropwizard histogram was also called
      verify(mockMetricUtils)
          .histogram(eq(PlatformEventProcessor.class), eq("kafkaLag"), anyLong());

      // Verify successful processing
      verify(mockHook1).invoke(any(OperationContext.class), eq(mockPlatformEvent));
    }
  }

  @Test
  public void testMicrometerKafkaQueueTimeWithDifferentTopics() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set the consumer group ID
    setConsumerGroupId(processor, "generic-platform-event-job-client");

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(any(GenericRecord.class)))
          .thenReturn(mockPlatformEvent);

      // Test with first topic
      long now = System.currentTimeMillis();
      when(mockConsumerRecord.timestamp()).thenReturn(now - 2000);
      when(mockConsumerRecord.topic()).thenReturn("PlatformEvent_v1");
      processor.consume(mockConsumerRecord);

      // Create second consumer record for different topic
      ConsumerRecord<String, GenericRecord> mockConsumerRecord2 = mock(ConsumerRecord.class);
      GenericRecord mockGenericRecord2 = mock(GenericRecord.class);
      when(mockConsumerRecord2.value()).thenReturn(mockGenericRecord2);
      when(mockConsumerRecord2.key()).thenReturn("test-key-2");
      when(mockConsumerRecord2.topic()).thenReturn("PlatformEvent_v2");
      when(mockConsumerRecord2.partition()).thenReturn(0);
      when(mockConsumerRecord2.offset()).thenReturn(101L);
      when(mockConsumerRecord2.timestamp()).thenReturn(now - 5000);
      when(mockConsumerRecord2.serializedValueSize()).thenReturn(1024);

      // Test with second topic
      processor.consume(mockConsumerRecord2);

      // Verify separate timers for different topics
      Timer timer1 =
          meterRegistry.timer(
              MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
              "topic",
              "PlatformEvent_v1",
              "consumer.group",
              "generic-platform-event-job-client");

      Timer timer2 =
          meterRegistry.timer(
              MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
              "topic",
              "PlatformEvent_v2",
              "consumer.group",
              "generic-platform-event-job-client");

      assertEquals(timer1.count(), 1);
      assertEquals(timer2.count(), 1);

      // Verify different queue times
      assertTrue(timer1.totalTime(TimeUnit.MILLISECONDS) >= 1500);
      assertTrue(timer1.totalTime(TimeUnit.MILLISECONDS) <= 2500);

      assertTrue(timer2.totalTime(TimeUnit.MILLISECONDS) >= 4500);
      assertTrue(timer2.totalTime(TimeUnit.MILLISECONDS) <= 5500);
    }
  }

  @Test
  public void testMicrometerMetricsWithProcessingFailure() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set the consumer group ID
    setConsumerGroupId(processor, "generic-platform-event-job-client");

    // Make hook throw exception
    doThrow(new RuntimeException("Hook failed"))
        .when(mockHook1)
        .invoke(any(OperationContext.class), any(PlatformEvent.class));

    // Set timestamp
    long messageTimestamp = System.currentTimeMillis() - 4000; // 4 seconds ago
    when(mockConsumerRecord.timestamp()).thenReturn(messageTimestamp);
    when(mockConsumerRecord.topic()).thenReturn("PlatformEvent_v1");

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(mockGenericRecord))
          .thenReturn(mockPlatformEvent);

      // Execute
      processor.consume(mockConsumerRecord);

      // Verify timer was still recorded despite hook failure
      Timer timer =
          meterRegistry.timer(
              MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
              "topic",
              "PlatformEvent_v1",
              "consumer.group",
              "generic-platform-event-job-client");

      assertEquals(timer.count(), 1);
      assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 3500);
      assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) <= 4500);

      // Verify hook was invoked (and failed)
      verify(mockHook1).invoke(any(OperationContext.class), eq(mockPlatformEvent));
    }
  }

  @Test
  public void testMicrometerMetricsAbsentWhenRegistryNotPresent() throws Exception {
    // Configure the mock metricUtils to return empty Optional (no registry)
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.empty());

    // Set the consumer group ID
    setConsumerGroupId(processor, "generic-platform-event-job-client");

    when(mockConsumerRecord.timestamp()).thenReturn(System.currentTimeMillis() - 1000);

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(mockGenericRecord))
          .thenReturn(mockPlatformEvent);

      // Execute - should not throw exception
      processor.consume(mockConsumerRecord);

      // Verify the histogram method was still called (for dropwizard metrics)
      verify(mockMetricUtils)
          .histogram(eq(PlatformEventProcessor.class), eq("kafkaLag"), anyLong());

      // Verify processing completed successfully despite no registry
      verify(mockHook1).invoke(any(OperationContext.class), eq(mockPlatformEvent));
    }
  }

  @Test
  public void testMicrometerKafkaQueueTimeWithCustomConsumerGroup() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set a custom consumer group ID
    String customConsumerGroup = "custom-platform-event-consumer";
    setConsumerGroupId(processor, customConsumerGroup);

    when(mockConsumerRecord.timestamp()).thenReturn(System.currentTimeMillis() - 1500);
    when(mockConsumerRecord.topic()).thenReturn("PlatformEvent_v1");

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(mockGenericRecord))
          .thenReturn(mockPlatformEvent);

      // Execute
      processor.consume(mockConsumerRecord);

      // Verify timer was recorded with custom consumer group
      Timer timer =
          meterRegistry.timer(
              MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
              "topic",
              "PlatformEvent_v1",
              "consumer.group",
              customConsumerGroup);

      assertNotNull(timer);
      assertEquals(timer.count(), 1);
    }
  }

  @Test
  public void testMicrometerKafkaQueueTimeAccuracy() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set the consumer group ID
    setConsumerGroupId(processor, "generic-platform-event-job-client");

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusPE(any(GenericRecord.class)))
          .thenReturn(mockPlatformEvent);

      // Test multiple queue times
      long[] queueTimes = {100, 500, 1000, 2000, 5000}; // milliseconds

      for (int i = 0; i < queueTimes.length; i++) {
        // Create new consumer record for each test
        ConsumerRecord<String, GenericRecord> testRecord = mock(ConsumerRecord.class);
        GenericRecord testGenericRecord = mock(GenericRecord.class);
        when(testRecord.value()).thenReturn(testGenericRecord);
        when(testRecord.key()).thenReturn("test-key-" + i);
        when(testRecord.topic()).thenReturn("PlatformEvent_v1");
        when(testRecord.partition()).thenReturn(0);
        when(testRecord.offset()).thenReturn(100L + i);
        when(testRecord.timestamp()).thenReturn(System.currentTimeMillis() - queueTimes[i]);
        when(testRecord.serializedValueSize()).thenReturn(1024);

        processor.consume(testRecord);
      }

      // Verify timer statistics
      Timer timer =
          meterRegistry.timer(
              MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
              "topic",
              "PlatformEvent_v1",
              "consumer.group",
              "generic-platform-event-job-client");

      assertEquals(timer.count(), queueTimes.length);

      // Verify mean is reasonable (should be around (100+500+1000+2000+5000)/5 = 1720ms)
      double mean = timer.mean(TimeUnit.MILLISECONDS);
      assertTrue(mean >= 1500);
      assertTrue(mean <= 2000);

      // Verify max recorded time
      assertTrue(timer.max(TimeUnit.MILLISECONDS) >= 4500);
      assertTrue(timer.max(TimeUnit.MILLISECONDS) <= 5500);

      // Verify histogram was called for each record
      verify(mockMetricUtils, times(queueTimes.length))
          .histogram(eq(PlatformEventProcessor.class), eq("kafkaLag"), anyLong());
    }
  }

  @Test
  public void testMicrometerMetricsWithNullRecord() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(mockMetricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set the consumer group ID
    setConsumerGroupId(processor, "generic-platform-event-job-client");

    // Setup null record
    ConsumerRecord<String, GenericRecord> nullValueRecord = mock(ConsumerRecord.class);
    when(nullValueRecord.value()).thenReturn(null);
    when(nullValueRecord.key()).thenReturn("test-key");
    when(nullValueRecord.topic()).thenReturn("PlatformEvent_v1");
    when(nullValueRecord.partition()).thenReturn(0);
    when(nullValueRecord.offset()).thenReturn(100L);
    when(nullValueRecord.timestamp()).thenReturn(System.currentTimeMillis() - 2000);
    when(nullValueRecord.serializedValueSize()).thenReturn(0);

    // Execute
    processor.consume(nullValueRecord);

    // Verify timer was still recorded even for null record
    Timer timer =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "PlatformEvent_v1",
            "consumer.group",
            "generic-platform-event-job-client");

    assertEquals(timer.count(), 1);
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 1500);
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) <= 2500);

    // Verify null record metric was incremented
    verify(mockMetricUtils).increment(eq(PlatformEventProcessor.class), eq("null_record"), eq(1d));

    // Verify hook was NOT invoked
    verify(mockHook1, never()).invoke(any(OperationContext.class), any(PlatformEvent.class));
  }

  // Helper method to set consumer group ID via reflection
  private void setConsumerGroupId(PlatformEventProcessor processor, String consumerGroupId) {
    try {
      java.lang.reflect.Field field =
          PlatformEventProcessor.class.getDeclaredField("datahubPlatformEventConsumerGroupId");
      field.setAccessible(true);
      field.set(processor, consumerGroupId);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set datahubPlatformEventConsumerGroupId field", e);
    }
  }
}
