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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
  }

  @Test
  public void testConstructorFiltersEnabledHooks() {
    // Create processor with mixed enabled/disabled hooks
    List<PlatformEventHook> allHooks = Arrays.asList(mockHook1, mockDisabledHook, mockHook2);
    processor = new PlatformEventProcessor(mockOperationContext, allHooks);

    // Verify only enabled hooks are kept
    assertEquals(processor.getHooks().size(), 2);
    assertTrue(processor.getHooks().contains(mockHook1));
    assertTrue(processor.getHooks().contains(mockHook2));
    assertTrue(!processor.getHooks().contains(mockDisabledHook));

    // Verify init is called only on enabled hooks
    verify(mockHook1, times(1)).init();
    verify(mockHook2, times(1)).init();
    verify(mockDisabledHook, never()).init();
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
}
