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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.FailedMetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.context.TraceIdGenerator;
import io.datahubproject.openapi.v1.models.TraceStatus;
import io.datahubproject.openapi.v1.models.TraceStorageStatus;
import io.datahubproject.openapi.v1.models.TraceWriteStatus;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Duration;
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
      SystemTelemetryContext.TRACE_ID_GENERATOR.generateTraceId(
          Instant.now().toEpochMilli() + 1000);
  private static final String TEST_TRACE_ID =
      SystemTelemetryContext.TRACE_ID_GENERATOR.generateTraceId();
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
    properties.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, TEST_TRACE_ID);
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
    properties.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, TEST_TRACE_ID_FUTURE);
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
    properties.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, TEST_TRACE_ID);
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

  @Test
  public void testTraceWithNoOpState() throws Exception {
    // Arrange
    Map<Urn, List<String>> aspectNames =
        Collections.singletonMap(TEST_URN, Collections.singletonList(ASPECT_NAME));

    // Create system metadata with NO_OP state
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setProperties(
        new StringMap(Map.of(SystemTelemetryContext.TELEMETRY_TRACE_KEY, TEST_TRACE_ID)));
    SystemMetadataUtils.setNoOp(systemMetadata, true); // Set NO_OP flag

    // Create enveloped aspect with NO_OP system metadata
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setCreated(new AuditStamp().setTime(Instant.now().toEpochMilli()));
    envelopedAspect.setSystemMetadata(systemMetadata);

    // Set up entity response
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(
        new EnvelopedAspectMap(Collections.singletonMap(ASPECT_NAME, envelopedAspect)));
    entityResponse.setEntityName(TEST_URN.getEntityType());
    entityResponse.setUrn(TEST_URN);

    // Mock entity service response
    when(entityService.getEntitiesV2(any(), anyString(), anySet(), anySet(), anyBoolean()))
        .thenReturn(Collections.singletonMap(TEST_URN, entityResponse));

    // Act
    Map<Urn, Map<String, TraceStatus>> result =
        traceService.trace(operationContext, TEST_TRACE_ID, aspectNames, false, false);

    // Assert
    assertNotNull(result);
    assertTrue(result.containsKey(TEST_URN));
    Map<String, TraceStatus> urnStatus = result.get(TEST_URN);
    assertTrue(urnStatus.containsKey(ASPECT_NAME));

    TraceStatus status = urnStatus.get(ASPECT_NAME);
    assertEquals(status.getPrimaryStorage().getWriteStatus(), TraceWriteStatus.NO_OP);
    assertEquals(status.getSearchStorage().getWriteStatus(), TraceWriteStatus.NO_OP);
    assertTrue(status.isSuccess());
  }

  @Test
  public void testTraceHistoricStateSearchPropagation() throws Exception {
    // This test verifies that when primary storage has HISTORIC_STATE,
    // the search storage is also set to HISTORIC_STATE based on this line:
    // finalResponse.put(aspectName, TraceStorageStatus.ok(TraceWriteStatus.HISTORIC_STATE));

    // Arrange - create request with one URN and one aspect
    Map<Urn, List<String>> aspectNames =
        Collections.singletonMap(TEST_URN, Collections.singletonList(ASPECT_NAME));

    // Create a primary storage response with HISTORIC_STATE
    Map<String, TraceStorageStatus> primaryStatus = new LinkedHashMap<>();
    primaryStatus.put(ASPECT_NAME, TraceStorageStatus.ok(TraceWriteStatus.HISTORIC_STATE));

    // Mock entityService to return empty to skip that branch
    when(entityService.getEntitiesV2(any(), anyString(), anySet(), anySet(), anyBoolean()))
        .thenReturn(Collections.emptyMap());

    // Mock mcpTraceReader to return our primary status with HISTORIC_STATE
    when(mcpTraceReader.tracePendingStatuses(any(), eq(TEST_TRACE_ID), any(), anyBoolean()))
        .thenReturn(Collections.singletonMap(TEST_URN, primaryStatus));

    // Mock systemMetadataService to return empty list to ensure
    // we don't get a pre-existing search status
    when(systemMetadataService.findAspectsByUrn(eq(TEST_URN), anyList(), eq(true)))
        .thenReturn(Collections.emptyList());

    // Act
    Map<Urn, Map<String, TraceStatus>> result =
        traceService.trace(operationContext, TEST_TRACE_ID, aspectNames, false, false);

    // Assert
    assertNotNull(result);
    assertTrue(result.containsKey(TEST_URN));
    Map<String, TraceStatus> urnStatus = result.get(TEST_URN);
    assertTrue(urnStatus.containsKey(ASPECT_NAME));

    // The key assertion - if primary is HISTORIC_STATE, search should also be HISTORIC_STATE
    TraceStatus status = urnStatus.get(ASPECT_NAME);
    assertEquals(status.getPrimaryStorage().getWriteStatus(), TraceWriteStatus.HISTORIC_STATE);
    assertEquals(
        status.getSearchStorage().getWriteStatus(),
        TraceWriteStatus.HISTORIC_STATE,
        "When primary storage is HISTORIC_STATE, search storage should also be HISTORIC_STATE");
    assertTrue(status.isSuccess());
  }

  @Test
  public void testExtractTraceIdEpochMillis() {
    // Test case 1: Valid system metadata with trace ID
    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, TEST_TRACE_ID);
    systemMetadata.setProperties(new StringMap(properties));

    Long epochMillis = TraceServiceImpl.extractTraceIdEpochMillis(systemMetadata);
    assertNotNull(epochMillis);
    assertEquals(epochMillis, TraceIdGenerator.getTimestampMillis(TEST_TRACE_ID));

    // Test case 2: System metadata without trace ID property
    SystemMetadata systemMetadataNoTrace = new SystemMetadata();
    systemMetadataNoTrace.setProperties(new StringMap(new HashMap<>()));

    Long epochMillisNoTrace = TraceServiceImpl.extractTraceIdEpochMillis(systemMetadataNoTrace);
    assertNull(epochMillisNoTrace);

    // Test case 3: System metadata with null properties
    SystemMetadata systemMetadataNullProps = new SystemMetadata();
    systemMetadataNullProps.setProperties(null, SetMode.REMOVE_IF_NULL);

    Long epochMillisNullProps = TraceServiceImpl.extractTraceIdEpochMillis(systemMetadataNullProps);
    assertNull(epochMillisNullProps);

    // Test case 4: Null system metadata
    Long epochMillisNull = TraceServiceImpl.extractTraceIdEpochMillis(null);
    assertNull(epochMillisNull);
  }

  @Test
  public void testExtractTraceIdEpochMillis_externalW3CTraceId() {
    // W3C trace IDs from external OTel systems don't encode epoch micros in the first 16 chars.
    // Parsing them produces an implausible timestamp, which should return null.
    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    // Example W3C trace ID — first 16 hex chars are random, not epoch micros
    properties.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, "4bf92f3577b34da6a3ce929d0e0e4736");
    systemMetadata.setProperties(new StringMap(properties));

    assertNull(TraceServiceImpl.extractTraceIdEpochMillis(systemMetadata));
  }

  @Test
  public void testExtractTraceIdEpochMillis_malformedTraceId() {
    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();

    // Too short
    properties.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, "abc123");
    systemMetadata.setProperties(new StringMap(properties));
    assertNull(TraceServiceImpl.extractTraceIdEpochMillis(systemMetadata));

    // Not valid hex
    properties.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, "zzzzzzzzzzzzzzzz0000000000000000");
    systemMetadata.setProperties(new StringMap(properties));
    assertNull(TraceServiceImpl.extractTraceIdEpochMillis(systemMetadata));
  }

  @Test
  public void testExtractTraceIdEpochMillis_w3cTraceIdWouldCauseArithmeticOverflow() {
    // Reproduces the production incident: 538 ArithmeticException stack traces in 20 minutes.
    //
    // When a service mesh/API gateway propagates W3C trace IDs via traceparent headers,
    // the first 16 hex chars are random (not epoch micros). TraceIdGenerator.getTimestampMillis()
    // parses them as unsigned long, producing a nonsensical epoch far in the future.
    //
    // The downstream code in MCLKafkaListener.updateMetrics() computes:
    //   queueTimeMs = System.currentTimeMillis() - extractedEpochMillis
    // then calls:
    //   Duration.ofMillis(queueTimeMs).record()
    // which internally calls Duration.toNanos() → Math.multiplyExact(seconds, NANOS_PER_SECOND)
    //
    // With a far-future epoch, queueTimeMs is a large negative number (~-3.7 trillion),
    // and toNanos() overflows: multiplyExact(-3.7 billion sec, 1 billion) → ArithmeticException

    // This is the actual W3C trace ID from the production incident report
    String w3cTraceId = "4bf92f3577b34da6a3ce929d0e0e4736";

    // Step 1: Verify that parsing this trace ID produces a far-future epoch
    // (without the fix, this would be returned as the epoch millis)
    long rawEpochMicros = Long.parseUnsignedLong(w3cTraceId.substring(0, 16), 16);
    long rawEpochMillis = rawEpochMicros / 1000;
    // 4bf92f3577b34da6 → 5,475,922,892,990,000,550 micros → ~5.4 trillion millis (~year 2143)
    assertTrue(
        rawEpochMillis > System.currentTimeMillis() * 2,
        "Raw parsed epoch should be far in the future: " + rawEpochMillis);

    // Step 2: Verify that the queue time calculation would produce a value that overflows
    long queueTimeMs = System.currentTimeMillis() - rawEpochMillis;
    // queueTimeMs ≈ -3.7 trillion ms
    assertTrue(
        queueTimeMs < -1_000_000_000_000L, "Queue time should be a large negative: " + queueTimeMs);

    // Step 3: Confirm that Duration.ofMillis(queueTimeMs).toNanos() would overflow
    // This is the exact crash that occurred 538 times in 20 minutes in production
    try {
      Duration.ofMillis(queueTimeMs).toNanos();
      fail("Expected ArithmeticException from Duration.toNanos() overflow");
    } catch (ArithmeticException e) {
      // This is the exception that was crashing MCLKafkaListener.updateMetrics()
      assertTrue(
          e.getMessage().contains("overflow"),
          "Expected 'long overflow' but got: " + e.getMessage());
    }

    // Step 4: Verify the fix — extractTraceIdEpochMillis returns null for this trace ID,
    // so the Duration.ofMillis() path is never reached
    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    properties.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, w3cTraceId);
    systemMetadata.setProperties(new StringMap(properties));

    assertNull(
        TraceServiceImpl.extractTraceIdEpochMillis(systemMetadata),
        "extractTraceIdEpochMillis should return null for W3C trace IDs to prevent overflow");
  }

  @Test
  public void testExtractTraceIdEpochMillis_preDataHubEpoch() {
    // A trace ID encoding a timestamp from before 2020 (before DataHub tracing existed)
    SystemMetadata systemMetadata = new SystemMetadata();
    Map<String, String> properties = new HashMap<>();
    // Generate a trace ID with epoch = 0 (1970-01-01)
    String ancientTraceId = SystemTelemetryContext.TRACE_ID_GENERATOR.generateTraceId(0L);
    properties.put(SystemTelemetryContext.TELEMETRY_TRACE_KEY, ancientTraceId);
    systemMetadata.setProperties(new StringMap(properties));

    assertNull(TraceServiceImpl.extractTraceIdEpochMillis(systemMetadata));
  }
}
