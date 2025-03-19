package com.linkedin.metadata.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.EventUtils;
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
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.io.IOException;
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

public class MetadataChangeProposalsProcessorTest {

  private SystemEntityClient entityClient;

  private MetadataChangeProposalsProcessor processor;

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

  @Mock private ConsumerRecord<String, GenericRecord> mockConsumerRecord;

  @Mock private GenericRecord mockRecord;

  @Mock private Span mockSpan;

  private AutoCloseable mocks;

  private MockedStatic<Span> spanMock;
  private MockedStatic<MetricUtils> metricUtilsMock;
  private MockedStatic<EventUtils> eventUtilsMock;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);

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
        new MetadataChangeProposalsProcessor(
            opContext,
            entityClient,
            mockKafkaProducer,
            mockKafkaThrottle,
            mockRegistry,
            mockProvider);

    // Setup mocks for static methods
    spanMock = mockStatic(Span.class);
    spanMock.when(Span::current).thenReturn(mockSpan);

    metricUtilsMock = mockStatic(MetricUtils.class);
    MetricRegistry mockMetricRegistry = mock(MetricRegistry.class);
    metricUtilsMock.when(MetricUtils::get).thenReturn(mockMetricRegistry);
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
    verify(mockSpan).setStatus(StatusCode.ERROR, "Failed to ingest MCP.");
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
}
