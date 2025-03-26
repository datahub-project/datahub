package com.linkedin.metadata.kafka.batch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.common.Status;
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
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.mxe.Topics;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
      TestOperationContexts.systemContextNoSearchAuthorization();

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
  private MockedStatic<MetricUtils> metricUtilsMock;
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
            EntityClientConfig.builder().build());

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

    metricUtilsMock = mockStatic(MetricUtils.class);
    MetricRegistry mockMetricRegistry = mock(MetricRegistry.class);
    when(mockMetricRegistry.histogram(any(String.class))).thenReturn(mockHistogram);
    metricUtilsMock.when(MetricUtils::get).thenReturn(mockMetricRegistry);
    metricUtilsMock
        .when(() -> MetricUtils.name(eq(BatchMetadataChangeProposalsProcessor.class), any()))
        .thenReturn("metricName");

    eventUtilsMock = mockStatic(EventUtils.class);

    // Setup consumer record mocks
    setupConsumerRecordMock(mockConsumerRecord1, mockRecord1, "test-key-1", 0, 0L);
    setupConsumerRecordMock(mockConsumerRecord2, mockRecord2, "test-key-2", 0, 1L);
    setupConsumerRecordMock(mockConsumerRecord3, mockRecord3, "test-key-3", 0, 2L);
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

    if (metricUtilsMock != null) {
      metricUtilsMock.close();
      metricUtilsMock = null;
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
}
