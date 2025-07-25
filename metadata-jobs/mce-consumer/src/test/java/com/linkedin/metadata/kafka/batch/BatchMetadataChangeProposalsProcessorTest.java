package com.linkedin.metadata.kafka.batch;

import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.codahale.metrics.Histogram;
import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.client.SystemJavaEntityClient;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
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
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
import java.util.ArrayList;
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

public class BatchMetadataChangeProposalsProcessorTest {

  private SystemEntityClient entityClient;
  private BatchMetadataChangeProposalsProcessor processor;
  private final OperationContext opContext =
      TestOperationContexts.Builder.builder()
          .systemTelemetryContextSupplier(() -> null) // mocked
          .buildSystemContext();

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

  @Mock private Histogram mockHistogram;

  @Mock private ConsumerRecord<String, GenericRecord> mockConsumerRecord1;

  @Mock private ConsumerRecord<String, GenericRecord> mockConsumerRecord2;

  @Mock private ConsumerRecord<String, GenericRecord> mockConsumerRecord3;

  @Mock private GenericRecord mockRecord1;

  @Mock private GenericRecord mockRecord2;

  @Mock private GenericRecord mockRecord3;

  @Mock private Span mockSpan;

  private AutoCloseable mocks;
  private MockedStatic<Span> spanMock;
  @Mock private MetricUtils metricUtilsMock;
  private MockedStatic<EventUtils> eventUtilsMock;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);

    // Create the entity client following the pattern in MetadataChangeProposalsProcessorTest
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
            metricUtilsMock);

    // Setup the processor
    processor =
        new BatchMetadataChangeProposalsProcessor(
            opContext,
            entityClient,
            mockKafkaProducer,
            mockKafkaThrottle,
            mockRegistry,
            mockProvider);

    // Set fmcpTopicName field via reflection
    try {
      java.lang.reflect.Field field =
          BatchMetadataChangeProposalsProcessor.class.getDeclaredField("fmcpTopicName");
      field.setAccessible(true);
      field.set(processor, Topics.FAILED_METADATA_CHANGE_PROPOSAL);

      field = BatchMetadataChangeProposalsProcessor.class.getDeclaredField("mceConsumerGroupId");
      field.setAccessible(true);
      field.set(processor, "MetadataChangeProposal-Consumer");
    } catch (Exception e) {
      throw new RuntimeException("Failed to set field via reflection", e);
    }

    // Setup mocks for static methods
    spanMock = mockStatic(Span.class);
    spanMock.when(Span::current).thenReturn(mockSpan);

    MockedStatic<MetricUtils> metricUtilsMock = mockStatic(MetricUtils.class);

    try {
      metricUtilsMock
          .when(() -> MetricUtils.name(eq(BatchMetadataChangeProposalsProcessor.class), any()))
          .thenReturn("metricName");

      eventUtilsMock = mockStatic(EventUtils.class);

      // Setup consumer record mocks
      setupConsumerRecordMock(mockConsumerRecord1, mockRecord1, "test-key-1", 0, 0L);
      setupConsumerRecordMock(mockConsumerRecord2, mockRecord2, "test-key-2", 0, 1L);
      setupConsumerRecordMock(mockConsumerRecord3, mockRecord3, "test-key-3", 0, 2L);
    } finally {
      if (metricUtilsMock != null) {
        metricUtilsMock.close();
      }
    }
  }

  private void setupConsumerRecordMock(
      ConsumerRecord<String, GenericRecord> consumerRecord,
      GenericRecord record,
      String key,
      int partition,
      long offset) {
    when(consumerRecord.value()).thenReturn(record);
    when(consumerRecord.key()).thenReturn(key);
    when(consumerRecord.topic()).thenReturn(Topics.METADATA_CHANGE_PROPOSAL);
    when(consumerRecord.partition()).thenReturn(partition);
    when(consumerRecord.offset()).thenReturn(offset);
    when(consumerRecord.timestamp()).thenReturn(System.currentTimeMillis());
    when(consumerRecord.serializedValueSize()).thenReturn(100);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    // Close static mocks first
    if (spanMock != null) {
      spanMock.close();
      spanMock = null;
    }

    if (eventUtilsMock != null) {
      eventUtilsMock.close();
      eventUtilsMock = null;
    }

    // Then close other mocks
    if (mocks != null) {
      mocks.close();
      mocks = null;
    }

    MDC.clear();
  }

  @Test
  public void testDeserializationFailure() throws Exception {
    // Mock conversion from Avro to throw IOException
    IOException deserializationException = new IOException("Failed to deserialize Avro record");
    eventUtilsMock
        .when(() -> EventUtils.avroToPegasusMCP(mockRecord1))
        .thenThrow(deserializationException);

    List<ConsumerRecord<String, GenericRecord>> records = List.of(mockConsumerRecord1);

    // Execute test
    processor.consume(records);

    // Verify that kafkaProducer was not called (since we can't forward properly)
    verify(mockKafkaProducer, never()).produceFailedMetadataChangeProposal(any(), any(), any());
  }

  @Test
  public void testSuccessfulBatchIngestion() throws Exception {
    // Disable inner batching
    when(mockProvider.getMetadataChangeProposal())
        .thenReturn(
            new MetadataChangeProposalConfig()
                .setConsumer(
                    new MetadataChangeProposalConfig.ConsumerBatchConfig()
                        .setBatch(
                            new MetadataChangeProposalConfig.BatchConfig()
                                .setSize(Integer.MAX_VALUE))));

    // Create MCPs
    MetadataChangeProposal mcp1 = new MetadataChangeProposal();
    mcp1.setSystemMetadata(new SystemMetadata());
    mcp1.setChangeType(ChangeType.UPSERT);
    mcp1.setEntityUrn(
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:test,testSuccessfulBatchIngestion1,PROD)"));
    mcp1.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
    mcp1.setEntityType("dataset");
    mcp1.setAspectName("status");
    MetadataChangeProposal mcp2 = new MetadataChangeProposal();
    mcp2.setSystemMetadata(new SystemMetadata());
    mcp2.setChangeType(ChangeType.UPSERT);
    mcp2.setEntityUrn(
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:test,testSuccessfulBatchIngestion2,PROD)"));
    mcp2.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
    mcp2.setEntityType("dataset");
    mcp2.setAspectName("status");

    // Mock conversion from Avro to Pegasus MCP
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord1)).thenReturn(mcp1);
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord2)).thenReturn(mcp2);

    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(mockConsumerRecord1, mockConsumerRecord2);

    // Execute test
    processor.consume(records);

    // Verify that mockEntityService.batchIngestProposals was called
    verify(mockEntityService, times(1)).ingestProposal(any(), any(), eq(false));

    // Verify that kafkaProducer was not called (since ingestion was successful)
    verify(mockKafkaProducer, never()).produceFailedMetadataChangeProposal(any(), any(), any());
  }

  @Test
  public void testEmptyBatch() throws Exception {
    // Execute test with empty list
    processor.consume(new ArrayList<>());

    // Verify that entityClient.batchIngestProposals was not called
    verify(mockEntityService, never())
        .ingestProposal(any(OperationContext.class), any(), anyBoolean());

    // Verify that kafkaProducer was not called
    verify(mockKafkaProducer, never()).produceFailedMetadataChangeProposal(any(), any(), any());
  }

  @Test
  public void testIngestionFailure() throws Exception {
    // Disable inner batching
    when(mockProvider.getMetadataChangeProposal())
        .thenReturn(
            new MetadataChangeProposalConfig()
                .setConsumer(
                    new MetadataChangeProposalConfig.ConsumerBatchConfig()
                        .setBatch(
                            new MetadataChangeProposalConfig.BatchConfig()
                                .setSize(Integer.MAX_VALUE))));

    // Create 3 Invalid MCPs
    MetadataChangeProposal mcp1 = new MetadataChangeProposal();
    mcp1.setSystemMetadata(new SystemMetadata());
    mcp1.setChangeType(ChangeType.UPSERT);
    mcp1.setEntityUrn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,INVALID)"));
    mcp1.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
    mcp1.setEntityType("dataset");
    mcp1.setAspectName("status");
    MetadataChangeProposal mcp2 = new MetadataChangeProposal();
    mcp2.setSystemMetadata(new SystemMetadata());
    mcp2.setChangeType(ChangeType.UPSERT);
    mcp2.setEntityUrn(
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:test,testSuccessfulBatchIngestion2,PROD)"));
    mcp2.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
    mcp2.setEntityType("FOOBAR"); // Invalid entity type
    mcp2.setAspectName("status");
    MetadataChangeProposal mcp3 = new MetadataChangeProposal();
    mcp3.setSystemMetadata(new SystemMetadata());
    mcp3.setChangeType(ChangeType.UPSERT);
    mcp3.setEntityUrn(
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:test,testSuccessfulBatchIngestion2,PROD)"));
    mcp3.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
    mcp3.setEntityType("dataset");
    mcp3.setAspectName("INVALID"); // Invalid aspect

    // Mock conversion from Avro to Pegasus MCP
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord1)).thenReturn(mcp1);
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord2)).thenReturn(mcp2);
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord3)).thenReturn(mcp3);

    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(mockConsumerRecord1, mockConsumerRecord2, mockConsumerRecord3);

    // Execute test
    processor.consume(records);

    ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(mockKafkaProducer)
        .produceFailedMetadataChangeProposal(eq(opContext), anyList(), exceptionCaptor.capture());

    // Verify error handling
    Throwable ingestionException = exceptionCaptor.getValue();
    verify(mockSpan).recordException(ingestionException);
    verify(mockSpan).setStatus(StatusCode.ERROR, ingestionException.getMessage());

    // Verify that kafkaProducer was called to produce the failed MCPs
    ArgumentCaptor<List<MetadataChangeProposal>> mcpCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockKafkaProducer, times(1))
        .produceFailedMetadataChangeProposal(
            eq(opContext), mcpCaptor.capture(), eq(ingestionException));

    List<MetadataChangeProposal> capturedMCPs = mcpCaptor.getValue();
    assert capturedMCPs.size() == 3;
    assert capturedMCPs.contains(mcp1);
    assert capturedMCPs.contains(mcp2);
    assert capturedMCPs.contains(mcp3);

    // Verify that ingestProposal was not called
    verify(mockEntityService, never())
        .ingestProposal(any(OperationContext.class), any(), anyBoolean());
  }

  @Test
  public void testMixedDeserializationResults() throws Exception {
    // Mock successful conversion for one record and failure for the other
    MetadataChangeProposal mcp1 = new MetadataChangeProposal();
    mcp1.setSystemMetadata(new SystemMetadata());
    mcp1.setChangeType(ChangeType.UPSERT);
    mcp1.setEntityUrn(
        UrnUtils.getUrn(
            "urn:li:dataset:(urn:li:dataPlatform:test,testMixedDeserializationResults,PROD)"));
    mcp1.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
    mcp1.setEntityType("dataset");
    mcp1.setAspectName("status");

    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord1)).thenReturn(mcp1);

    IOException deserializationException = new IOException("Failed to deserialize Avro record");
    eventUtilsMock
        .when(() -> EventUtils.avroToPegasusMCP(mockRecord2))
        .thenThrow(deserializationException);

    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(mockConsumerRecord1, mockConsumerRecord2);

    // Execute test
    processor.consume(records);

    // Verify that entityClient.batchIngestProposals was called with only the successful MCP
    ArgumentCaptor<AspectsBatch> mcpCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, times(1)).ingestProposal(any(), mcpCaptor.capture(), eq(false));

    AspectsBatch aspectsBatch = mcpCaptor.getValue();
    assert aspectsBatch.getMCPItems().size() == 1;
    assert aspectsBatch.getMCPItems().stream()
        .anyMatch(i -> i.getMetadataChangeProposal().equals(mcp1));

    // Verify that kafkaProducer was not called (since we handled the deserialize exception)
    verify(mockKafkaProducer, never()).produceFailedMetadataChangeProposal(any(), any(), any());
  }

  @Test
  public void testLargeBatchPartitioning() throws Exception {
    // Mock the ConfigurationProvider to return a specific batch size limit
    MetadataChangeProposalConfig.ConsumerBatchConfig batchConfig =
        new MetadataChangeProposalConfig.ConsumerBatchConfig()
            .setBatch(
                new MetadataChangeProposalConfig.BatchConfig()
                    .setSize(5 * 1024)
                    .setEnabled(true)); // 5KB batch size limit for testing
    when(mockProvider.getMetadataChangeProposal())
        .thenReturn(new MetadataChangeProposalConfig().setConsumer(batchConfig));

    // Create 3 MCPs, one with a large aspect value
    MetadataChangeProposal smallMcp1 = createMcpWithAspectSize(1000); // 1KB
    MetadataChangeProposal largeMcp =
        createMcpWithAspectSize(4500); // 4.5KB - should trigger a new batch
    MetadataChangeProposal smallMcp2 = createMcpWithAspectSize(2000); // 2KB

    // Mock conversion from Avro to Pegasus MCP
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord1)).thenReturn(smallMcp1);
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord2)).thenReturn(largeMcp);
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord3)).thenReturn(smallMcp2);

    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(mockConsumerRecord1, mockConsumerRecord2, mockConsumerRecord3);

    // Execute test
    processor.consume(records);

    // Verify that entityClient.batchIngestProposals was called 3x
    // First batch should contain only smallMcp1 (1KB)
    // Second batch should contain largeMcp but not smallMcp2 since it exceeds 5KB (4.5KB + 2KB =
    // 6.5KB, which exceeds limit so we process them separately)
    verify(mockEntityService, times(3)).ingestProposal(any(), any(), eq(false));

    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, times(3)).ingestProposal(any(), batchCaptor.capture(), eq(false));

    List<AspectsBatch> capturedBatches = batchCaptor.getAllValues();
    // First batch should contain only smallMcp1
    assertEquals(capturedBatches.get(0).getMCPItems().size(), 1);
    // Second batch should contain largeMcp
    assertEquals(capturedBatches.get(1).getMCPItems().size(), 1);
    // Third batch should contain smallMcp2
    assertEquals(capturedBatches.get(1).getMCPItems().size(), 1);
  }

  @Test
  public void testExtremelyLargeAspect() throws Exception {
    // Mock the ConfigurationProvider to return a specific batch size limit
    MetadataChangeProposalConfig.ConsumerBatchConfig batchConfig =
        new MetadataChangeProposalConfig.ConsumerBatchConfig()
            .setBatch(
                new MetadataChangeProposalConfig.BatchConfig()
                    .setSize(10000)
                    .setEnabled(true)); // 10KB batch size limit for testing
    when(mockProvider.getMetadataChangeProposal())
        .thenReturn(new MetadataChangeProposalConfig().setConsumer(batchConfig));
    mock(MetadataChangeProposalConfig.ConsumerBatchConfig.class);

    // Create an MCP with an aspect value that exceeds the batch size on its own
    MetadataChangeProposal hugeMcp =
        createMcpWithAspectSize(15000); // 15KB - larger than batch limit
    MetadataChangeProposal smallMcp = createMcpWithAspectSize(1000); // 1KB

    // Mock conversion from Avro to Pegasus MCP
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord1)).thenReturn(hugeMcp);
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord2)).thenReturn(smallMcp);

    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(mockConsumerRecord1, mockConsumerRecord2);

    // Execute test
    processor.consume(records);

    // Verify that entityClient.batchIngestProposals was called twice
    // First call for hugeMcp alone (despite exceeding the limit, it's processed alone)
    // Second call for smallMcp
    verify(mockEntityService, times(2)).ingestProposal(any(), any(), eq(false));

    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, times(2)).ingestProposal(any(), batchCaptor.capture(), eq(false));

    List<AspectsBatch> capturedBatches = batchCaptor.getAllValues();
    assertEquals(capturedBatches.get(0).getMCPItems().size(), 1);
    assertEquals(capturedBatches.get(1).getMCPItems().size(), 1);
  }

  @Test
  public void testEmptyBatchWithSpanCreation() throws Exception {
    // Execute test with empty list
    processor.consume(new ArrayList<>());

    // Verify that the span was created (we can't directly test this, but we can check
    // that the entityClient.batchIngestProposals was not called and no errors occurred)
    verify(mockEntityService, never())
        .ingestProposal(any(OperationContext.class), any(), anyBoolean());

    // Verify that kafkaProducer was not called
    verify(mockKafkaProducer, never()).produceFailedMetadataChangeProposal(any(), any(), any());
  }

  @Test
  public void testCalculateMCPSize() throws Exception {
    // Test the calculateMCPSize method using reflection
    java.lang.reflect.Method calculateMCPSizeMethod =
        BatchMetadataChangeProposalsProcessor.class.getDeclaredMethod(
            "calculateMCPSize", MetadataChangeProposal.class);
    calculateMCPSizeMethod.setAccessible(true);

    // Test with null MCP
    Long nullSize = (Long) calculateMCPSizeMethod.invoke(processor, (MetadataChangeProposal) null);
    assertEquals(nullSize.longValue(), 0L);

    // Test with MCP that has null aspect
    MetadataChangeProposal mcpNullAspect = new MetadataChangeProposal();
    Long nullAspectSize = (Long) calculateMCPSizeMethod.invoke(processor, mcpNullAspect);
    assertEquals(nullAspectSize.longValue(), 1000L); // Base size

    // Test with MCP that has aspect but null value
    MetadataChangeProposal mcpEmptyAspect = new MetadataChangeProposal();
    Long emptyAspectSize = (Long) calculateMCPSizeMethod.invoke(processor, mcpEmptyAspect);
    assertEquals(emptyAspectSize.longValue(), 1000L); // Base size

    // Test with MCP that has aspect with value
    MetadataChangeProposal mcpWithAspect = createMcpWithAspectSize(500);
    Long withAspectSize = (Long) calculateMCPSizeMethod.invoke(processor, mcpWithAspect);
    assertTrue(withAspectSize >= 1500); // Base size + aspect size
  }

  @Test
  public void testConsumeWithTelemetryMetrics() throws Exception {
    // Mock the metric utils
    MetricUtils mockMetricUtils = mock(MetricUtils.class);

    // Create a mock operation context
    OperationContext opContextWithMetrics = spy(opContext);

    // Mock the metric utils to be present
    when(opContextWithMetrics.getMetricUtils()).thenReturn(Optional.of(mockMetricUtils));

    // Mock the withQueueSpan to execute the runnable directly
    // The method signature in OperationContext is:
    // public void withQueueSpan(String name, List<SystemMetadata> systemMetadata, String topicName,
    // Runnable task, String... attributes)
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(3);
              runnable.run();
              return null;
            })
        .when(opContextWithMetrics)
        .withQueueSpan(
            anyString(), // operation name
            anyList(), // system metadata list
            anyString(), // topic name
            any(Runnable.class), // task
            anyString(),
            anyString(), // BATCH_SIZE_ATTR and its value
            anyString(),
            anyString() // MetricUtils.DROPWIZARD_NAME and metric name
            );

    BatchMetadataChangeProposalsProcessor processorWithTelemetry =
        new BatchMetadataChangeProposalsProcessor(
            opContextWithMetrics,
            entityClient,
            mockKafkaProducer,
            mockKafkaThrottle,
            mockRegistry,
            mockProvider);

    // Set required fields via reflection
    try {
      java.lang.reflect.Field field =
          BatchMetadataChangeProposalsProcessor.class.getDeclaredField("fmcpTopicName");
      field.setAccessible(true);
      field.set(processorWithTelemetry, Topics.FAILED_METADATA_CHANGE_PROPOSAL);

      field = BatchMetadataChangeProposalsProcessor.class.getDeclaredField("mceConsumerGroupId");
      field.setAccessible(true);
      field.set(processorWithTelemetry, "MetadataChangeProposal-Consumer");
    } catch (Exception e) {
      throw new RuntimeException("Failed to set field via reflection", e);
    }

    // Setup minimal configuration
    when(mockProvider.getMetadataChangeProposal())
        .thenReturn(
            new MetadataChangeProposalConfig()
                .setConsumer(
                    new MetadataChangeProposalConfig.ConsumerBatchConfig()
                        .setBatch(
                            new MetadataChangeProposalConfig.BatchConfig()
                                .setSize(Integer.MAX_VALUE))));

    // Create a simple MCP
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setSystemMetadata(new SystemMetadata());
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setEntityUrn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,testMetrics,PROD)"));
    mcp.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
    mcp.setEntityType("dataset");
    mcp.setAspectName("status");

    // Mock conversion
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord1)).thenReturn(mcp);

    // Create a single consumer record with a timestamp
    when(mockConsumerRecord1.timestamp())
        .thenReturn(System.currentTimeMillis() - 1000); // 1 second ago
    List<ConsumerRecord<String, GenericRecord>> records = List.of(mockConsumerRecord1);

    // Execute test - this should trigger the metricUtils.ifPresent line
    processorWithTelemetry.consume(records);

    // Verify that the metricUtils.histogram was called for kafka lag
    verify(mockMetricUtils, times(1))
        .histogram(eq(BatchMetadataChangeProposalsProcessor.class), eq("kafkaLag"), anyLong());

    // Verify that the processing completed successfully
    verify(mockEntityService, times(1)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testMicrometerKafkaQueueTimeMetric() throws Exception {
    // Setup a real MeterRegistry to capture metrics
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricUtils metricUtils = MetricUtils.builder().registry(meterRegistry).build();

    // Create operation context with metric utils
    OperationContext opContextWithMetrics = mock(OperationContext.class);
    when(opContextWithMetrics.getMetricUtils()).thenReturn(Optional.of(metricUtils));

    // Mock withQueueSpan to execute the runnable directly
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(3);
              runnable.run();
              return null;
            })
        .when(opContextWithMetrics)
        .withQueueSpan(anyString(), anyList(), anyString(), any(Runnable.class), any());

    // Create processor with metrics-enabled context
    BatchMetadataChangeProposalsProcessor processorWithMetrics =
        new BatchMetadataChangeProposalsProcessor(
            opContextWithMetrics,
            entityClient,
            mockKafkaProducer,
            mockKafkaThrottle,
            mockRegistry,
            mockProvider);

    // Set required fields
    setProcessorFields(processorWithMetrics);

    // Setup basic configuration
    setupBasicConfiguration();

    // Create MCP
    MetadataChangeProposal mcp = createSimpleMCP();
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord1)).thenReturn(mcp);

    // Set timestamp to simulate queue time
    long messageTimestamp = System.currentTimeMillis() - 5000; // 5 seconds ago
    when(mockConsumerRecord1.timestamp()).thenReturn(messageTimestamp);
    when(mockConsumerRecord1.topic()).thenReturn("MetadataChangeProposal_v1");

    // Execute
    processorWithMetrics.consume(List.of(mockConsumerRecord1));

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
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) >= 4000); // At least 4 seconds
    assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) <= 6000); // At most 6 seconds
  }

  @Test
  public void testMicrometerKafkaQueueTimeWithMultipleRecords() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricUtils metricUtils = MetricUtils.builder().registry(meterRegistry).build();

    // Create operation context with metric utils
    OperationContext opContextWithMetrics = mock(OperationContext.class);
    when(opContextWithMetrics.getMetricUtils()).thenReturn(Optional.of(metricUtils));

    // Mock withQueueSpan
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(3);
              runnable.run();
              return null;
            })
        .when(opContextWithMetrics)
        .withQueueSpan(anyString(), anyList(), anyString(), any(Runnable.class), any());

    // Create processor
    BatchMetadataChangeProposalsProcessor processorWithMetrics =
        new BatchMetadataChangeProposalsProcessor(
            opContextWithMetrics,
            entityClient,
            mockKafkaProducer,
            mockKafkaThrottle,
            mockRegistry,
            mockProvider);

    setProcessorFields(processorWithMetrics);
    setupBasicConfiguration();

    // Create MCPs
    MetadataChangeProposal mcp1 = createSimpleMCP();
    MetadataChangeProposal mcp2 = createSimpleMCP();
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord1)).thenReturn(mcp1);
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord2)).thenReturn(mcp2);

    // Set different timestamps for different queue times
    long now = System.currentTimeMillis();
    when(mockConsumerRecord1.timestamp()).thenReturn(now - 2000); // 2 seconds ago
    when(mockConsumerRecord1.topic()).thenReturn("MetadataChangeProposal_v1");

    when(mockConsumerRecord2.timestamp()).thenReturn(now - 8000); // 8 seconds ago
    when(mockConsumerRecord2.topic()).thenReturn("MetadataChangeProposal_v1");

    // Execute
    processorWithMetrics.consume(List.of(mockConsumerRecord1, mockConsumerRecord2));

    // Verify timer was recorded twice
    Timer timer =
        meterRegistry.timer(
            MetricUtils.KAFKA_MESSAGE_QUEUE_TIME,
            "topic",
            "MetadataChangeProposal_v1",
            "consumer.group",
            "MetadataChangeProposal-Consumer");

    assertEquals(timer.count(), 2);
    // Average should be around 5 seconds ((2 + 8) / 2)
    assertTrue(timer.mean(TimeUnit.MILLISECONDS) >= 4000);
    assertTrue(timer.mean(TimeUnit.MILLISECONDS) <= 6000);
  }

  @Test
  public void testMicrometerKafkaQueueTimeWithDifferentTopics() throws Exception {
    // Setup a real MeterRegistry
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricUtils metricUtils = MetricUtils.builder().registry(meterRegistry).build();

    // Create operation context with metric utils
    OperationContext opContextWithMetrics = mock(OperationContext.class);
    when(opContextWithMetrics.getMetricUtils()).thenReturn(Optional.of(metricUtils));

    // Mock withQueueSpan
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(3);
              runnable.run();
              return null;
            })
        .when(opContextWithMetrics)
        .withQueueSpan(anyString(), anyList(), anyString(), any(Runnable.class), any());

    // Create processor
    BatchMetadataChangeProposalsProcessor processorWithMetrics =
        new BatchMetadataChangeProposalsProcessor(
            opContextWithMetrics,
            entityClient,
            mockKafkaProducer,
            mockKafkaThrottle,
            mockRegistry,
            mockProvider);

    setProcessorFields(processorWithMetrics);
    setupBasicConfiguration();

    // Create MCPs
    MetadataChangeProposal mcp1 = createSimpleMCP();
    MetadataChangeProposal mcp2 = createSimpleMCP();
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord1)).thenReturn(mcp1);
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord2)).thenReturn(mcp2);

    // Set different topics
    long now = System.currentTimeMillis();
    when(mockConsumerRecord1.timestamp()).thenReturn(now - 3000);
    when(mockConsumerRecord1.topic()).thenReturn("MetadataChangeProposal_v1");

    when(mockConsumerRecord2.timestamp()).thenReturn(now - 5000);
    when(mockConsumerRecord2.topic()).thenReturn("MetadataChangeProposal_Timeseries");

    // Execute separately to simulate different topics
    processorWithMetrics.consume(List.of(mockConsumerRecord1));
    processorWithMetrics.consume(List.of(mockConsumerRecord2));

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
    assertTrue(timer1.totalTime(TimeUnit.MILLISECONDS) >= 2500);
    assertTrue(timer1.totalTime(TimeUnit.MILLISECONDS) <= 3500);

    assertTrue(timer2.totalTime(TimeUnit.MILLISECONDS) >= 4500);
    assertTrue(timer2.totalTime(TimeUnit.MILLISECONDS) <= 5500);
  }

  @Test
  public void testMicrometerMetricsAbsentWhenRegistryNotPresent() throws Exception {
    // Create MetricUtils without a registry
    MetricUtils metricUtilsNoRegistry = MetricUtils.builder().registry(null).build();

    // Create operation context with metric utils that has no registry
    OperationContext opContextNoRegistry = mock(OperationContext.class);
    when(opContextNoRegistry.getMetricUtils()).thenReturn(Optional.of(metricUtilsNoRegistry));

    // Mock withQueueSpan
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(3);
              runnable.run();
              return null;
            })
        .when(opContextNoRegistry)
        .withQueueSpan(anyString(), anyList(), anyString(), any(Runnable.class), any());

    // Create processor
    BatchMetadataChangeProposalsProcessor processorNoRegistry =
        new BatchMetadataChangeProposalsProcessor(
            opContextNoRegistry,
            entityClient,
            mockKafkaProducer,
            mockKafkaThrottle,
            mockRegistry,
            mockProvider);

    setProcessorFields(processorNoRegistry);
    setupBasicConfiguration();

    // Create MCP
    MetadataChangeProposal mcp = createSimpleMCP();
    eventUtilsMock.when(() -> EventUtils.avroToPegasusMCP(mockRecord1)).thenReturn(mcp);

    when(mockConsumerRecord1.timestamp()).thenReturn(System.currentTimeMillis() - 1000);

    // Execute - should not throw exception
    processorNoRegistry.consume(List.of(mockConsumerRecord1));
  }

  // Helper methods to reduce duplication
  private void setProcessorFields(BatchMetadataChangeProposalsProcessor processor)
      throws Exception {
    java.lang.reflect.Field field =
        BatchMetadataChangeProposalsProcessor.class.getDeclaredField("fmcpTopicName");
    field.setAccessible(true);
    field.set(processor, Topics.FAILED_METADATA_CHANGE_PROPOSAL);

    field = BatchMetadataChangeProposalsProcessor.class.getDeclaredField("mceConsumerGroupId");
    field.setAccessible(true);
    field.set(processor, "MetadataChangeProposal-Consumer");
  }

  private void setupBasicConfiguration() {
    when(mockProvider.getMetadataChangeProposal())
        .thenReturn(
            new MetadataChangeProposalConfig()
                .setConsumer(
                    new MetadataChangeProposalConfig.ConsumerBatchConfig()
                        .setBatch(
                            new MetadataChangeProposalConfig.BatchConfig()
                                .setSize(Integer.MAX_VALUE))));
  }

  private MetadataChangeProposal createSimpleMCP() {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setSystemMetadata(new SystemMetadata());
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setEntityUrn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,testDataset,PROD)"));
    mcp.setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)));
    mcp.setEntityType("dataset");
    mcp.setAspectName("status");
    return mcp;
  }

  // Helper method to create an MCP with a specific aspect value size
  private MetadataChangeProposal createMcpWithAspectSize(int size) {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setSystemMetadata(new SystemMetadata());
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setEntityUrn(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test" + size + ",PROD)"));
    mcp.setEntityType("dataset");
    mcp.setAspectName(DATASET_PROPERTIES_ASPECT_NAME);

    // Create an aspect with a value of the specified size
    DatasetProperties aspect = new DatasetProperties();
    StringBuilder valueBuilder = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      valueBuilder.append('x');
    }
    aspect.setDescription(valueBuilder.toString());
    mcp.setAspect(GenericRecordUtils.serializeAspect(aspect));

    return mcp;
  }
}
