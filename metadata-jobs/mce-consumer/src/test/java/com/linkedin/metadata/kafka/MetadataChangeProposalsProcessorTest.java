package com.linkedin.metadata.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.client.SystemJavaEntityClient;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.slf4j.MDC;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetadataChangeProposalsProcessorTest {

  private SystemEntityClient entityClient;

  private MetadataChangeProposalsProcessor processor;

  private OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();

  @Mock private EntityService<?> mockEntityService;

  @Mock private DeleteEntityService mockDeleteEntityService;

  @Mock private EntitySearchService mockEntitySearchService;

  @Mock private CachingEntitySearchService mockCachingEntitySearchService;

  @Mock private SearchService mockSearchService;

  @Mock private LineageSearchService mockLineageSearchService;

  @Mock private TimeseriesAspectService mockTimeseriesAspectService;

  @Mock private RollbackService mockRollbackService;

  @Mock private EventProducer mockKafkaProducer;

  @Mock private ThrottleSensor mockKafkaThrottle;

  @Mock private KafkaListenerEndpointRegistry mockRegistry;

  @Mock private ConfigurationProvider mockProvider;

  @Mock private ConsumerRecord<String, GenericRecord> mockConsumerRecord;

  @Mock private GenericRecord mockRecord;

  @Mock private Span mockSpan;

  @Mock private MetricUtils metricUtils;

  private AutoCloseable mocks;

  private MockedStatic<Span> spanMock;
  private MockedStatic<MetricUtils> metricUtilsMock;
  private MockedStatic<EventUtils> eventUtilsMock;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);

    opContext =
        opContext.toBuilder()
            .systemTelemetryContext(
                SystemTelemetryContext.builder()
                    .metricUtils(metricUtils)
                    .tracer(mock(Tracer.class))
                    .build())
            .build(opContext.getSystemActorContext().getAuthentication(), false);

    entityClient =
        new SystemJavaEntityClient(
            mockEntityService,
            mockDeleteEntityService,
            mockEntitySearchService,
            mockCachingEntitySearchService,
            mockSearchService,
            mockLineageSearchService,
            mockTimeseriesAspectService,
            mockRollbackService,
            mockKafkaProducer,
            new EntityClientCacheConfig(),
            EntityClientConfig.builder().build(),
            null);

    // Setup the processor
    processor =
        new MetadataChangeProposalsProcessor(
            opContext,
            entityClient,
            mockKafkaProducer,
            mockKafkaThrottle,
            mockRegistry,
            mockProvider);

    // Set the mceConsumerGroupId field via reflection
    try {
      java.lang.reflect.Field field =
          MetadataChangeProposalsProcessor.class.getDeclaredField("mceConsumerGroupId");
      field.setAccessible(true);
      field.set(processor, "MetadataChangeProposal-Consumer");
    } catch (Exception e) {
      throw new RuntimeException("Failed to set mceConsumerGroupId field", e);
    }

    // Setup mocks for static methods
    spanMock = mockStatic(Span.class);
    spanMock.when(Span::current).thenReturn(mockSpan);

    metricUtilsMock = mockStatic(MetricUtils.class);
    metricUtilsMock
        .when(() -> MetricUtils.name(eq(MetadataChangeProposalsProcessor.class), any()))
        .thenReturn("metricName");

    eventUtilsMock = mockStatic(EventUtils.class);

    // Setup consumer record mock
    when(mockConsumerRecord.value()).thenReturn(mockRecord);
    when(mockConsumerRecord.key()).thenReturn("test-key"); // doesn't matter for test
    when(mockConsumerRecord.topic()).thenReturn(Topics.METADATA_CHANGE_PROPOSAL);
    when(mockConsumerRecord.partition()).thenReturn(0);
    when(mockConsumerRecord.offset()).thenReturn(0L);
    when(mockConsumerRecord.timestamp()).thenReturn(System.currentTimeMillis());
    when(mockConsumerRecord.serializedValueSize()).thenReturn(100);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    // Close static mocks first
    if (spanMock != null) {
      spanMock.close();
      spanMock = null; // Set to null after closing
    }

    if (metricUtilsMock != null) {
      metricUtilsMock.close();
      metricUtilsMock = null; // Set to null after closing
    }

    if (eventUtilsMock != null) {
      eventUtilsMock.close();
      eventUtilsMock = null; // Set to null after closing
    }

    // Then close other mocks
    if (mocks != null) {
      mocks.close();
      mocks = null; // Set to null after closing
    }

    MDC.clear();
  }

  @Test
  public void testDeserializationFailure() throws Exception {
    // Mock conversion from Avro to throw IOException
    IOException deserializationException = new IOException("Failed to deserialize Avro record");
    eventUtilsMock
        .when(() -> EventUtils.avroToPegasusMCP(mockRecord))
        .thenThrow(deserializationException);

    // Execute test
    processor.consume(mockConsumerRecord);

    // Verify that kafkaProducer was not called (since we can't forward properly)
    verify(mockKafkaProducer, never()).produceFailedMetadataChangeProposal(any(), any(), any());
  }

  @Test
  public void testValidationFailureWithUrnValidationException() throws Exception {
    // Create a MCP that will fail with a specific validation exception
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    // Invalid URN to trigger validation
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,INVALID)");
    mcp.setEntityUrn(entityUrn);
    mcp.setEntityType("dataset");
    mcp.setAspectName("status");
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));

    // Mock conversion from Avro to Pegasus MCP
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord)).thenReturn(mcp);
    when(mockEntityService.ingestProposal(eq(opContext), any(AspectsBatch.class), eq(false)))
        .thenThrow(
            new IllegalArgumentException("ERROR :: /origin :: \"INVALID\" is not an enum symbol"));

    // Execute test
    processor.consume(mockConsumerRecord);

    ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(mockKafkaProducer)
        .produceFailedMetadataChangeProposal(
            eq(opContext), eq(List.of(mcp)), exceptionCaptor.capture());

    // Verify kafkaProducer was called to produce the failed MCP
    verify(mockKafkaProducer, times(1))
        .produceFailedMetadataChangeProposal(eq(opContext), eq(List.of(mcp)), any(Throwable.class));

    // Verify error handling
    Throwable validationException = exceptionCaptor.getValue();
    verify(mockSpan).recordException(validationException);
    verify(mockSpan)
        .setStatus(StatusCode.ERROR, "ERROR :: /origin :: \"INVALID\" is not an enum symbol");
  }

  @Test
  public void testValidationFailureWithEntityValidationException() throws Exception {
    // Create a MCP that will fail with a specific validation exception
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    mcp.setEntityUrn(entityUrn);
    mcp.setEntityType("FOOBAR"); // Invalid entity type
    mcp.setAspectName("status");
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));

    // Mock conversion from Avro to Pegasus MCP
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord)).thenReturn(mcp);

    // Execute test
    processor.consume(mockConsumerRecord);

    ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(mockKafkaProducer)
        .produceFailedMetadataChangeProposal(
            eq(opContext), eq(List.of(mcp)), exceptionCaptor.capture());

    // Verify kafkaProducer was called to produce the failed MCP
    verify(mockKafkaProducer, times(1))
        .produceFailedMetadataChangeProposal(eq(opContext), eq(List.of(mcp)), any(Throwable.class));

    // Verify error handling
    Throwable validationException = exceptionCaptor.getValue();
    verify(mockSpan).recordException(validationException);
    verify(mockSpan)
        .setStatus(
            StatusCode.ERROR, "URN entity type does not match MCP entity type. dataset != FOOBAR");
  }

  @Test
  public void testValidationFailureWithAspectValidationException() throws Exception {
    // Create a MCP that will fail with a specific validation exception
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    mcp.setEntityUrn(entityUrn);
    mcp.setEntityType("dataset");
    mcp.setAspectName("INVALID"); // Invalid aspect
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));

    // Mock conversion from Avro to Pegasus MCP
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord)).thenReturn(mcp);

    // Execute test
    processor.consume(mockConsumerRecord);

    ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(mockKafkaProducer)
        .produceFailedMetadataChangeProposal(
            eq(opContext), eq(List.of(mcp)), exceptionCaptor.capture());

    // Verify kafkaProducer was called to produce the failed MCP
    verify(mockKafkaProducer, times(1))
        .produceFailedMetadataChangeProposal(eq(opContext), eq(List.of(mcp)), any(Throwable.class));

    // Verify error handling
    Throwable validationException = exceptionCaptor.getValue();
    verify(mockSpan).recordException(validationException);
    verify(mockSpan).setStatus(StatusCode.ERROR, "Unknown aspect INVALID for entity dataset");
  }

  @Test
  public void testSuccess() {
    // Create a successful MCP
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    mcp.setEntityUrn(entityUrn);
    mcp.setEntityType("dataset");
    mcp.setAspectName("status");
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));

    // Mock conversion from Avro to Pegasus MCP
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord)).thenReturn(mcp);

    // Execute test
    processor.consume(mockConsumerRecord);

    verify(mockEntityService).ingestProposal(eq(opContext), any(AspectsBatch.class), eq(false));
  }

  @Test
  public void testMicrometerKafkaQueueTimeMetric() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(metricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Set timestamp to simulate queue time
    long messageTimestamp = System.currentTimeMillis() - 3000; // 3 seconds ago
    when(mockConsumerRecord.timestamp()).thenReturn(messageTimestamp);
    when(mockConsumerRecord.topic()).thenReturn("MetadataChangeProposal_v1");

    // Create MCP
    MetadataChangeProposal mcp = createSimpleMCP();
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord)).thenReturn(mcp);

    // Execute
    processor.consume(mockConsumerRecord);

    // Verify timer was recorded
    Timer timer =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "MetadataChangeProposal_v1",
            "consumer.group",
            "MetadataChangeProposal-Consumer");

    assertNotNull(timer);
    assertEquals(timer.count(), 1);
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 2500); // At least 2.5 seconds
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) <= 3500); // At most 3.5 seconds

    // Verify the histogram method was called
    verify(metricUtils)
        .histogram(eq(MetadataChangeProposalsProcessor.class), eq("kafkaLag"), anyLong());

    // Verify successful processing
    verify(mockEntityService).ingestProposal(eq(opContext), any(), eq(false));
  }

  @Test
  public void testMicrometerKafkaQueueTimeWithDifferentTopics() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(metricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Create MCP
    MetadataChangeProposal mcp = createSimpleMCP();
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord)).thenReturn(mcp);

    // Test with first topic
    long now = System.currentTimeMillis();
    when(mockConsumerRecord.timestamp()).thenReturn(now - 2000);
    when(mockConsumerRecord.topic()).thenReturn("MetadataChangeProposal_v1");
    processor.consume(mockConsumerRecord);

    // Create second consumer record mock
    ConsumerRecord<String, GenericRecord> mockConsumerRecord2 = mock(ConsumerRecord.class);
    GenericRecord mockRecord2 = mock(GenericRecord.class);
    when(mockConsumerRecord2.value()).thenReturn(mockRecord2);
    when(mockConsumerRecord2.key()).thenReturn("test-key-2");
    when(mockConsumerRecord2.topic()).thenReturn("MetadataChangeProposal_Timeseries");
    when(mockConsumerRecord2.partition()).thenReturn(0);
    when(mockConsumerRecord2.offset()).thenReturn(1L);
    when(mockConsumerRecord2.timestamp()).thenReturn(now - 5000);
    when(mockConsumerRecord2.serializedValueSize()).thenReturn(100);

    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord2)).thenReturn(mcp);

    // Test with second topic
    processor.consume(mockConsumerRecord2);

    // Verify separate timers for different topics
    Timer timer1 =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "MetadataChangeProposal_v1",
            "consumer.group",
            "MetadataChangeProposal-Consumer");

    Timer timer2 =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "MetadataChangeProposal_Timeseries",
            "consumer.group",
            "MetadataChangeProposal-Consumer");

    assertEquals(timer1.count(), 1);
    assertEquals(timer2.count(), 1);

    // Verify different queue times
    assertTrue(timer1.totalTime(TimeUnit.MILLISECONDS) >= 1500);
    assertTrue(timer1.totalTime(TimeUnit.MILLISECONDS) <= 2500);

    assertTrue(timer2.totalTime(TimeUnit.MILLISECONDS) >= 4500);
    assertTrue(timer2.totalTime(TimeUnit.MILLISECONDS) <= 5500);
  }

  @Test
  public void testMicrometerMetricsWithProcessingFailure() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(metricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Create MCP that will fail
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)"));
    mcp.setEntityType("INVALID_TYPE"); // This will cause failure
    mcp.setAspectName("status");
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));

    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord)).thenReturn(mcp);

    // Set timestamp
    long messageTimestamp = System.currentTimeMillis() - 4000; // 4 seconds ago
    when(mockConsumerRecord.timestamp()).thenReturn(messageTimestamp);
    when(mockConsumerRecord.topic()).thenReturn("MetadataChangeProposal_v1");

    // Execute
    processor.consume(mockConsumerRecord);

    // Verify timer was still recorded despite failure
    Timer timer =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "MetadataChangeProposal_v1",
            "consumer.group",
            "MetadataChangeProposal-Consumer");

    assertEquals(timer.count(), 1);
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 3500);
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) <= 4500);

    // Verify failure handling was triggered
    verify(mockKafkaProducer)
        .produceFailedMetadataChangeProposal(eq(opContext), eq(List.of(mcp)), any(Throwable.class));
  }

  @Test
  public void testMicrometerMetricsAbsentWhenRegistryNotPresent() throws Exception {
    // Configure the mock metricUtils to return empty Optional (no registry)
    when(metricUtils.getRegistry()).thenReturn(Optional.empty());

    // Create MCP
    MetadataChangeProposal mcp = createSimpleMCP();
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord)).thenReturn(mcp);

    when(mockConsumerRecord.timestamp()).thenReturn(System.currentTimeMillis() - 1000);

    // Execute - should not throw exception
    processor.consume(mockConsumerRecord);

    // Verify the histogram method was still called (for dropwizard metrics)
    verify(metricUtils)
        .histogram(eq(MetadataChangeProposalsProcessor.class), eq("kafkaLag"), anyLong());

    // Verify processing completed successfully despite no registry
    verify(mockEntityService).ingestProposal(eq(opContext), any(), eq(false));
  }

  @Test
  public void testMicrometerKafkaQueueTimeAccuracy() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the mock metricUtils to return the registry
    when(metricUtils.getRegistry()).thenReturn(Optional.of(meterRegistry));

    // Create MCP
    MetadataChangeProposal mcp = createSimpleMCP();

    // Test multiple queue times
    long[] queueTimes = {100, 500, 1000, 2000, 5000}; // milliseconds

    for (int i = 0; i < queueTimes.length; i++) {
      // Create new consumer record for each test
      ConsumerRecord<String, GenericRecord> testRecord = mock(ConsumerRecord.class);
      GenericRecord testGenericRecord = mock(GenericRecord.class);
      when(testRecord.value()).thenReturn(testGenericRecord);
      when(testRecord.key()).thenReturn("test-key-" + i);
      when(testRecord.topic()).thenReturn("MetadataChangeProposal_v1");
      when(testRecord.partition()).thenReturn(0);
      when(testRecord.offset()).thenReturn((long) i);
      when(testRecord.timestamp()).thenReturn(System.currentTimeMillis() - queueTimes[i]);
      when(testRecord.serializedValueSize()).thenReturn(100);

      eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(testGenericRecord)).thenReturn(mcp);

      processor.consume(testRecord);
    }

    // Verify timer statistics
    Timer timer =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "MetadataChangeProposal_v1",
            "consumer.group",
            "MetadataChangeProposal-Consumer");

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
        .histogram(eq(MetadataChangeProposalsProcessor.class), eq("kafkaLag"), anyLong());
  }

  @Test
  public void testMetricsNotRecordedWhenMetricUtilsAbsent() throws Exception {
    // Create a new operation context without metric utils
    OperationContext opContextNoMetrics =
        TestOperationContexts.systemContextNoSearchAuthorization().toBuilder()
            .systemTelemetryContext(
                opContext.getSystemTelemetryContext().toBuilder().metricUtils(null).build())
            .build(opContext.getSystemActorContext().getAuthentication(), false);

    // Create a new processor with this context
    MetadataChangeProposalsProcessor processorNoMetrics =
        new MetadataChangeProposalsProcessor(
            opContextNoMetrics,
            entityClient,
            mockKafkaProducer,
            mockKafkaThrottle,
            mockRegistry,
            mockProvider);

    // Create MCP
    MetadataChangeProposal mcp = createSimpleMCP();
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord)).thenReturn(mcp);

    when(mockConsumerRecord.timestamp()).thenReturn(System.currentTimeMillis() - 1000);

    // Execute - should not throw exception
    processorNoMetrics.consume(mockConsumerRecord);

    // Verify processing completed successfully
    verify(mockEntityService).ingestProposal(eq(opContextNoMetrics), any(), eq(false));

    // Verify metricUtils methods were never called since it's not present in context
    verify(metricUtils, never()).histogram(any(), any(), anyLong());
    verify(metricUtils, never()).getRegistry();
  }

  // Helper method
  private MetadataChangeProposal createSimpleMCP() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,testDataset,PROD)"));
    mcp.setEntityType("dataset");
    mcp.setAspectName("status");
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
    return mcp;
  }
}
