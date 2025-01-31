package com.linkedin.metadata.trace;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.metadata.context.TraceIdGenerator;
import io.datahubproject.openapi.v1.models.TraceStatus;
import io.datahubproject.openapi.v1.models.TraceStorageStatus;
import io.datahubproject.openapi.v1.models.TraceWriteStatus;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TraceServiceImplTest {
  private static final String TEST_TRACE_ID_FUTURE =
      TraceContext.TRACE_ID_GENERATOR.generateTraceId(Instant.now().toEpochMilli() + 1000);
  private static final String TEST_TRACE_ID = TraceContext.TRACE_ID_GENERATOR.generateTraceId();
  protected static final String ASPECT_NAME = "status";
  protected static final String TIMESERIES_ASPECT_NAME = "datasetProfile";
  protected static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,PageViewEvent,PROD)");

  @Mock private SystemMetadataService systemMetadataService;
  @Mock private EntityService<?> entityService;
  @Mock private MCPTraceReader mcpTraceReader;
  @Mock private MCPFailedTraceReader mcpFailedTraceReader;
  @Mock private MCLTraceReader mclVersionedTraceReader;
  @Mock private MCLTraceReader mclTimeseriesTraceReader;

  private TraceServiceImpl traceService;
  private static final OperationContext operationContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);

    traceService =
        TraceServiceImpl.builder()
            .entityRegistry(operationContext.getEntityRegistry())
            .systemMetadataService(systemMetadataService)
            .entityService(entityService)
            .mcpTraceReader(mcpTraceReader)
            .mcpFailedTraceReader(mcpFailedTraceReader)
            .mclVersionedTraceReader(mclVersionedTraceReader)
            .mclTimeseriesTraceReader(mclTimeseriesTraceReader)
            .build();
  }

  @Test
  public void testTraceWithActiveState() throws Exception {
    // Arrange
    Map<Urn, List<String>> aspectNames =
        Collections.singletonMap(TEST_URN, Collections.singletonList(ASPECT_NAME));

    // Mock entityService response for primary storage
    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put(TraceContext.TELEMETRY_TRACE_KEY, TEST_TRACE_ID);
    systemMetadata.setProperties(new StringMap(properties));

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setCreated(new AuditStamp().setTime(Instant.now().toEpochMilli()));
    envelopedAspect.setSystemMetadata(systemMetadata);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(
        new EnvelopedAspectMap(Collections.singletonMap(ASPECT_NAME, envelopedAspect)));
    entityResponse.setEntityName(TEST_URN.getEntityType());
    entityResponse.setUrn(TEST_URN);

    when(entityService.getEntitiesV2(any(), anyString(), anySet(), anySet(), anyBoolean()))
        .thenReturn(Collections.singletonMap(TEST_URN, entityResponse));

    // Mock search storage response
    AspectRowSummary summary = mock(AspectRowSummary.class);
    when(summary.getUrn()).thenReturn(TEST_URN.toString());
    when(summary.getAspectName()).thenReturn(ASPECT_NAME);
    when(summary.getTelemetryTraceId()).thenReturn(TEST_TRACE_ID);
    when(systemMetadataService.findAspectsByUrn(eq(TEST_URN), anyList(), eq(true)))
        .thenReturn(Collections.singletonList(summary));

    // Act
    Map<Urn, Map<String, TraceStatus>> result =
        traceService.trace(operationContext, TEST_TRACE_ID, aspectNames, false, false);

    // Assert
    assertNotNull(result);
    assertTrue(result.containsKey(TEST_URN));
    Map<String, TraceStatus> urnStatus = result.get(TEST_URN);
    assertTrue(urnStatus.containsKey(ASPECT_NAME));

    TraceStatus status = urnStatus.get(ASPECT_NAME);
    assertEquals(status.getPrimaryStorage().getWriteStatus(), TraceWriteStatus.ACTIVE_STATE);
    assertEquals(status.getSearchStorage().getWriteStatus(), TraceWriteStatus.ACTIVE_STATE);
    assertTrue(status.isSuccess());
  }

  @Test
  public void testTraceWithPendingStatus() throws Exception {
    // Arrange
    Map<Urn, List<String>> aspectNames =
        Collections.singletonMap(TEST_URN, Collections.singletonList(ASPECT_NAME));

    // Mock empty entity response (not in SQL)
    when(entityService.getEntitiesV2(any(), anyString(), anySet(), anySet(), anyBoolean()))
        .thenReturn(Collections.emptyMap());

    // Mock pending status from Kafka
    Map<String, TraceStorageStatus> pendingStatus = new LinkedHashMap<>();
    pendingStatus.put(
        ASPECT_NAME,
        TraceStorageStatus.ok(TraceWriteStatus.PENDING, "Consumer has not processed offset."));

    when(mcpTraceReader.tracePendingStatuses(any(), eq(TEST_TRACE_ID), any(), anyBoolean()))
        .thenReturn(Collections.singletonMap(TEST_URN, pendingStatus));

    // Act
    Map<Urn, Map<String, TraceStatus>> result =
        traceService.trace(operationContext, TEST_TRACE_ID, aspectNames, false, false);

    // Assert
    assertNotNull(result);
    assertTrue(result.containsKey(TEST_URN));
    Map<String, TraceStatus> urnStatus = result.get(TEST_URN);
    assertTrue(urnStatus.containsKey(ASPECT_NAME));

    TraceStatus status = urnStatus.get(ASPECT_NAME);
    assertEquals(status.getPrimaryStorage().getWriteStatus(), TraceWriteStatus.PENDING);
    assertTrue(status.isSuccess());
  }

  @Test
  public void testTraceWithErrorStatus() throws Exception {
    // Arrange
    Map<Urn, List<String>> aspectNames =
        Collections.singletonMap(TEST_URN, Collections.singletonList(ASPECT_NAME));

    // Mock empty entity response
    when(entityService.getEntitiesV2(any(), anyString(), anySet(), anySet(), anyBoolean()))
        .thenReturn(Collections.emptyMap());

    // Mock error status from Kafka
    Map<String, TraceStorageStatus> errorStatus = new LinkedHashMap<>();
    errorStatus.put(
        ASPECT_NAME, TraceStorageStatus.fail(TraceWriteStatus.ERROR, "Failed to process message."));

    when(mcpTraceReader.tracePendingStatuses(any(), eq(TEST_TRACE_ID), any(), anyBoolean()))
        .thenReturn(Collections.singletonMap(TEST_URN, errorStatus));

    // Act
    Map<Urn, Map<String, TraceStatus>> result =
        traceService.trace(operationContext, TEST_TRACE_ID, aspectNames, true, true);

    // Assert
    assertNotNull(result);
    assertTrue(result.containsKey(TEST_URN));
    Map<String, TraceStatus> urnStatus = result.get(TEST_URN);
    assertTrue(urnStatus.containsKey(ASPECT_NAME));

    TraceStatus status = urnStatus.get(ASPECT_NAME);
    assertEquals(status.getPrimaryStorage().getWriteStatus(), TraceWriteStatus.ERROR);
    assertFalse(status.isSuccess());
  }

  @Test
  public void testTraceWithTimeseriesAspect() throws Exception {
    // Arrange
    Map<Urn, List<String>> aspectNames =
        Collections.singletonMap(TEST_URN, Collections.singletonList(TIMESERIES_ASPECT_NAME));

    // Act
    Map<Urn, Map<String, TraceStatus>> result =
        traceService.trace(operationContext, TEST_TRACE_ID, aspectNames, false, false);

    // Assert
    assertNotNull(result);
    assertTrue(result.containsKey(TEST_URN));
    Map<String, TraceStatus> urnStatus = result.get(TEST_URN);
    assertTrue(urnStatus.containsKey(TIMESERIES_ASPECT_NAME));

    TraceStatus status = urnStatus.get(TIMESERIES_ASPECT_NAME);
    assertEquals(status.getPrimaryStorage().getWriteStatus(), TraceWriteStatus.NO_OP);
    assertEquals(
        status.getSearchStorage().getWriteStatus(), TraceWriteStatus.TRACE_NOT_IMPLEMENTED);
    assertTrue(status.isSuccess());
  }

  @Test
  public void testTraceWithHistoricState() throws Exception {
    // Arrange
    Map<Urn, List<String>> aspectNames =
        Collections.singletonMap(TEST_URN, Collections.singletonList(ASPECT_NAME));

    // Mock primary storage with historic state
    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put(TraceContext.TELEMETRY_TRACE_KEY, TEST_TRACE_ID_FUTURE);
    systemMetadata.setProperties(new StringMap(properties));

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setCreated(
        new AuditStamp()
            .setTime(
                TraceIdGenerator.getTimestampMillis(TEST_TRACE_ID_FUTURE))); // Future timestamp
    envelopedAspect.setSystemMetadata(systemMetadata);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(
        new EnvelopedAspectMap(Collections.singletonMap(ASPECT_NAME, envelopedAspect)));
    entityResponse.setEntityName(TEST_URN.getEntityType());
    entityResponse.setUrn(TEST_URN);

    when(entityService.getEntitiesV2(any(), anyString(), anySet(), anySet(), anyBoolean()))
        .thenReturn(Collections.singletonMap(TEST_URN, entityResponse));

    // Mock search storage with historic state
    AspectRowSummary summary = mock(AspectRowSummary.class);
    when(summary.getUrn()).thenReturn(TEST_URN.toString());
    when(summary.getAspectName()).thenReturn(ASPECT_NAME);
    when(summary.hasTimestamp()).thenReturn(true);
    when(summary.getTimestamp())
        .thenReturn(TraceIdGenerator.getTimestampMillis(TEST_TRACE_ID_FUTURE)); // Future timestamp
    when(summary.getTelemetryTraceId()).thenReturn(TEST_TRACE_ID_FUTURE);

    when(systemMetadataService.findAspectsByUrn(eq(TEST_URN), anyList(), eq(true)))
        .thenReturn(Collections.singletonList(summary));

    // Act
    Map<Urn, Map<String, TraceStatus>> result =
        traceService.trace(operationContext, TEST_TRACE_ID, aspectNames, false, false);

    // Assert
    assertNotNull(result);
    assertTrue(result.containsKey(TEST_URN));
    Map<String, TraceStatus> urnStatus = result.get(TEST_URN);
    assertTrue(urnStatus.containsKey(ASPECT_NAME));

    TraceStatus status = urnStatus.get(ASPECT_NAME);
    assertEquals(status.getPrimaryStorage().getWriteStatus(), TraceWriteStatus.HISTORIC_STATE);
    assertEquals(status.getSearchStorage().getWriteStatus(), TraceWriteStatus.HISTORIC_STATE);
    assertTrue(status.isSuccess());
  }

  @Test
  public void testTraceWithFailedMessage() throws Exception {
    // Arrange
    Map<Urn, List<String>> aspectNames =
        Collections.singletonMap(TEST_URN, Collections.singletonList(ASPECT_NAME));

    // Mock primary storage with ERROR status
    Map<String, TraceStorageStatus> errorStatus = new LinkedHashMap<>();
    errorStatus.put(ASPECT_NAME, TraceStorageStatus.fail(TraceWriteStatus.ERROR, "Initial error"));

    when(mcpTraceReader.tracePendingStatuses(any(), eq(TEST_TRACE_ID), any(), anyBoolean()))
        .thenReturn(Collections.singletonMap(TEST_URN, errorStatus));

    // Mock the failed message in MCPFailedTraceReader
    SystemMetadata failedMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put(TraceContext.TELEMETRY_TRACE_KEY, TEST_TRACE_ID);
    failedMetadata.setProperties(new StringMap(properties));

    FailedMetadataChangeProposal failedMCP =
        new FailedMetadataChangeProposal()
            .setError(
                "[{\"message\":\"Processing failed: Test error message\",\"exceptionClass\":\"java.lang.IllegalArgumentException\"}]")
            .setMetadataChangeProposal(
                new MetadataChangeProposal()
                    .setEntityUrn(TEST_URN)
                    .setChangeType(ChangeType.UPSERT)
                    .setAspectName(ASPECT_NAME)
                    .setEntityType(TEST_URN.getEntityType())
                    .setSystemMetadata(failedMetadata));

    GenericRecord genericRecord = EventUtils.pegasusToAvroFailedMCP(failedMCP);
    ConsumerRecord<String, GenericRecord> failedRecord = mock(ConsumerRecord.class);
    when(failedRecord.value()).thenReturn(genericRecord);

    Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> failedMessages =
        Collections.singletonMap(ASPECT_NAME, Pair.of(failedRecord, failedMetadata));

    when(mcpFailedTraceReader.findMessages(any(), eq(TEST_TRACE_ID), any()))
        .thenReturn(Collections.singletonMap(TEST_URN, failedMessages));

    // Mock failed record read with error message
    when(mcpFailedTraceReader.read(eq(genericRecord))).thenReturn(Optional.of(failedMCP));

    // Act
    Map<Urn, Map<String, TraceStatus>> result =
        traceService.trace(operationContext, TEST_TRACE_ID, aspectNames, true, true);

    // Assert
    assertNotNull(result);
    assertTrue(result.containsKey(TEST_URN));
    Map<String, TraceStatus> urnStatus = result.get(TEST_URN);
    assertTrue(urnStatus.containsKey(ASPECT_NAME));

    TraceStatus status = urnStatus.get(ASPECT_NAME);
    assertEquals(status.getPrimaryStorage().getWriteStatus(), TraceWriteStatus.ERROR);
    assertNotNull(status.getPrimaryStorage().getWriteExceptions());
    assertEquals(status.getPrimaryStorage().getWriteExceptions().size(), 1);
    assertEquals(
        status.getPrimaryStorage().getWriteExceptions().get(0).getMessage(),
        "Processing failed: Test error message");
    assertEquals(
        status.getPrimaryStorage().getWriteExceptions().get(0).getExceptionClass(),
        "java.lang.IllegalArgumentException");
    assertFalse(status.isSuccess());
  }
}
