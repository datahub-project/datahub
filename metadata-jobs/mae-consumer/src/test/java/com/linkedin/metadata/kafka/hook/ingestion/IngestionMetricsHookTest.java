package com.linkedin.metadata.kafka.hook.ingestion;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestionMetricsHookTest {

  private static final String ASPECT_NAME = "dataHubExecutionRequestResult";
  private static final String ENTITY_TYPE = "dataHubExecutionRequest";
  private static final String TEST_EXECUTION_REQUEST_URN =
      "urn:li:dataHubExecutionRequest:test-pipeline-12345";

  private IngestionMetricsHook hook;
  private SimpleMeterRegistry meterRegistry;

  @BeforeMethod
  public void setup() {
    meterRegistry = new SimpleMeterRegistry();
    // Pass null for entityClient in tests - ingestion_source will default to "unknown"
    hook = new IngestionMetricsHook(meterRegistry, null, true);
    hook.init(TestOperationContexts.systemContextNoSearchAuthorization());
  }

  @Test
  public void testInvokeRecordsMetrics() throws Exception {
    MetadataChangeLog event = createTestEvent("SUCCEEDED", 1000L, 100, 5, 2, 3);

    hook.invoke(event);

    // Verify run counter
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter != null && runsCounter.count() == 1.0);

    // Verify duration was recorded
    DistributionSummary durationSummary =
        meterRegistry.find("com.datahub.ingest.duration_ms").summary();
    assertTrue(durationSummary != null && durationSummary.totalAmount() == 1000.0);

    // Verify events_produced counter
    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertTrue(eventsCounter != null && eventsCounter.count() == 100.0);

    // Verify records_written counter
    Counter recordsCounter = meterRegistry.find("com.datahub.ingest.records_written").counter();
    assertTrue(recordsCounter != null && recordsCounter.count() == 5.0);

    // Verify warnings counter
    Counter warningsCounter = meterRegistry.find("com.datahub.ingest.warnings").counter();
    assertTrue(warningsCounter != null && warningsCounter.count() == 2.0);

    // Verify failures counter
    Counter failuresCounter = meterRegistry.find("com.datahub.ingest.failures").counter();
    assertTrue(failuresCounter != null && failuresCounter.count() == 3.0);
  }

  @Test
  public void testInvokeSkipsWhenDisabled() throws Exception {
    IngestionMetricsHook disabledHook = new IngestionMetricsHook(meterRegistry, null, false);
    disabledHook.init(TestOperationContexts.systemContextNoSearchAuthorization());

    MetadataChangeLog event = createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0);

    disabledHook.invoke(event);

    // Verify no metrics recorded
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testInvokeSkipsNonIngestionReport() throws Exception {
    // Create an execution result with a non-ingestion report type
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("TEST_CONNECTION"); // Not CLI_INGEST or RUN_INGEST
    structuredReport.setSerializedValue("{}");
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    hook.invoke(event);

    // Verify no metrics recorded for non-ingestion report
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testInvokeWithRunIngestReportType() throws Exception {
    // Test executor format (RUN_INGEST) which uses different JSON structure
    MetadataChangeLog event = createExecutorTestEvent("SUCCEEDED", 2000L, 500, 50, 1, 0);

    hook.invoke(event);

    // Verify run counter
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter != null && runsCounter.count() == 1.0);

    // Verify duration was recorded
    DistributionSummary durationSummary =
        meterRegistry.find("com.datahub.ingest.duration_ms").summary();
    assertTrue(durationSummary != null && durationSummary.totalAmount() == 2000.0);

    // Verify events_produced counter
    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertTrue(eventsCounter != null && eventsCounter.count() == 500.0);

    // Verify records_written counter
    Counter recordsCounter = meterRegistry.find("com.datahub.ingest.records_written").counter();
    assertTrue(recordsCounter != null && recordsCounter.count() == 50.0);

    // Verify warnings counter
    Counter warningsCounter = meterRegistry.find("com.datahub.ingest.warnings").counter();
    assertTrue(warningsCounter != null && warningsCounter.count() == 1.0);

    // Verify platform tag is extracted correctly
    Counter platformCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("platform", "snowflake").counter();
    assertTrue(platformCounter != null && platformCounter.count() == 1.0);
  }

  @Test
  public void testInvokeWithDifferentStatuses() throws Exception {
    // Test with FAILURE status
    MetadataChangeLog failedEvent = createTestEvent("FAILURE", 500L, 50, 0, 0, 5);
    hook.invoke(failedEvent);

    Counter failedRunsCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("status", "FAILURE").counter();
    assertTrue(failedRunsCounter != null && failedRunsCounter.count() == 1.0);

    // Test with SUCCEEDED status
    MetadataChangeLog successEvent = createTestEvent("SUCCEEDED", 1000L, 100, 10, 0, 0);
    hook.invoke(successEvent);

    Counter succeededRunsCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("status", "SUCCEEDED").counter();
    assertTrue(succeededRunsCounter != null && succeededRunsCounter.count() == 1.0);
  }

  @Test
  public void testMultipleInvocations() throws Exception {
    // First invocation
    MetadataChangeLog event1 = createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0);
    hook.invoke(event1);

    // Second invocation
    MetadataChangeLog event2 = createTestEvent("SUCCEEDED", 2000L, 200, 10, 0, 0);
    hook.invoke(event2);

    // Verify counters accumulated
    Counter runsCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("status", "SUCCEEDED").counter();
    assertTrue(runsCounter != null && runsCounter.count() == 2.0);

    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertTrue(eventsCounter != null && eventsCounter.count() == 300.0); // 100 + 200
  }

  @Test
  public void testInvokeWithMinimalData() throws Exception {
    // Create event with minimal data (no optional metrics)
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    // No duration set

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue("{}"); // Empty report
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    // Should not throw
    hook.invoke(event);

    // Verify run counter was still recorded
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter != null && runsCounter.count() == 1.0);
  }

  @Test
  public void testIngestionSourceExtractedFromEntityClient() throws Exception {
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn execUrn = UrnUtils.getUrn(TEST_EXECUTION_REQUEST_URN);
    String ingestionSourceUrnStr = "urn:li:dataHubIngestionSource:my-snowflake-source";

    mockExecutionRequestInput(mockClient, execUrn, ingestionSourceUrnStr);

    when(mockClient.getV2(
            any(OperationContext.class),
            eq("dataHubIngestionSource"),
            any(Urn.class),
            eq(ImmutableSet.of("globalTags"))))
        .thenReturn(new EntityResponse());

    IngestionMetricsHook hookWithClient = new IngestionMetricsHook(meterRegistry, mockClient, true);
    hookWithClient.init(TestOperationContexts.systemContextNoSearchAuthorization());

    hookWithClient.invoke(createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0));

    Counter sourceCounter =
        meterRegistry
            .find("com.datahub.ingest.runs")
            .tag("ingestion_source", "my-snowflake-source")
            .counter();
    assertTrue(sourceCounter != null && sourceCounter.count() == 1.0);
  }

  @Test
  public void testCriticalityExtractedFromGlobalTags() throws Exception {
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn execUrn = UrnUtils.getUrn(TEST_EXECUTION_REQUEST_URN);
    String ingestionSourceUrnStr = "urn:li:dataHubIngestionSource:critical-source";

    mockExecutionRequestInput(mockClient, execUrn, ingestionSourceUrnStr);
    mockGlobalTags(mockClient, "critical");

    IngestionMetricsHook hookWithClient = new IngestionMetricsHook(meterRegistry, mockClient, true);
    hookWithClient.init(TestOperationContexts.systemContextNoSearchAuthorization());

    hookWithClient.invoke(createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0));

    Counter critCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("criticality", "critical").counter();
    assertTrue(critCounter != null && critCounter.count() == 1.0);
  }

  @Test
  public void testAlertDisabledCriticality() throws Exception {
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn execUrn = UrnUtils.getUrn(TEST_EXECUTION_REQUEST_URN);

    mockExecutionRequestInput(mockClient, execUrn, "urn:li:dataHubIngestionSource:disabled-source");
    mockGlobalTags(mockClient, "alert-disabled");

    IngestionMetricsHook hookWithClient = new IngestionMetricsHook(meterRegistry, mockClient, true);
    hookWithClient.init(TestOperationContexts.systemContextNoSearchAuthorization());

    hookWithClient.invoke(createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0));

    Counter counter =
        meterRegistry
            .find("com.datahub.ingest.runs")
            .tag("criticality", "alert-disabled")
            .counter();
    assertTrue(counter != null && counter.count() == 1.0);
  }

  @Test
  public void testCriticalityCacheAvoidsDuplicateRpcs() throws Exception {
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn execUrn = UrnUtils.getUrn(TEST_EXECUTION_REQUEST_URN);
    String ingestionSourceUrnStr = "urn:li:dataHubIngestionSource:cached-source";
    Urn ingestionSourceUrn = UrnUtils.getUrn(ingestionSourceUrnStr);

    mockExecutionRequestInput(mockClient, execUrn, ingestionSourceUrnStr);

    GlobalTags tags = new GlobalTags();
    TagAssociation tag = new TagAssociation();
    tag.setTag(new TagUrn("warning"));
    tags.setTags(new TagAssociationArray(tag));

    EnvelopedAspectMap tagAspects = new EnvelopedAspectMap();
    tagAspects.put("globalTags", new EnvelopedAspect().setValue(new Aspect(tags.data())));
    EntityResponse tagResponse = new EntityResponse();
    tagResponse.setAspects(tagAspects);

    when(mockClient.getV2(
            any(OperationContext.class),
            eq("dataHubIngestionSource"),
            eq(ingestionSourceUrn),
            eq(ImmutableSet.of("globalTags"))))
        .thenReturn(tagResponse);

    IngestionMetricsHook hookWithClient = new IngestionMetricsHook(meterRegistry, mockClient, true);
    hookWithClient.init(TestOperationContexts.systemContextNoSearchAuthorization());

    hookWithClient.invoke(createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0));
    hookWithClient.invoke(createTestEvent("SUCCEEDED", 2000L, 200, 10, 0, 0));

    // globalTags fetched only once (cached for second call)
    verify(mockClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("dataHubIngestionSource"),
            eq(ingestionSourceUrn),
            eq(ImmutableSet.of("globalTags")));

    // ExecutionRequestInput fetched each time (not cached — each execution request URN is unique in
    // production)
    verify(mockClient, times(2))
        .getV2(
            any(OperationContext.class),
            eq(execUrn.getEntityType()),
            eq(execUrn),
            eq(ImmutableSet.of("dataHubExecutionRequestInput")));
  }

  @Test
  public void testSinkFailuresRecorded() throws Exception {
    String reportJson =
        "{"
            + "\"cli\": {\"cli_version\": \"0.14.0\"},"
            + "\"Source (file)\": {"
            + "  \"events_produced\": 100,"
            + "  \"warnings\": [],"
            + "  \"failures\": [],"
            + "  \"platform\": \"file\""
            + "},"
            + "\"Sink (datahub-rest)\": {"
            + "  \"total_records_written\": 90,"
            + "  \"failures\": [{\"error\":\"write failed\"},{\"error\":\"timeout\"}]"
            + "}"
            + "}";

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);
    result.setStartTimeMs(System.currentTimeMillis() - 1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    hook.invoke(event);

    Counter sinkFailuresCounter = meterRegistry.find("com.datahub.ingest.sink_failures").counter();
    assertTrue(sinkFailuresCounter != null && sinkFailuresCounter.count() == 2.0);
  }

  @Test
  public void testVersionExtractedFromCliField() throws Exception {
    // Executor format includes cli.cli_version
    MetadataChangeLog event = createExecutorTestEvent("SUCCEEDED", 2000L, 500, 50, 0, 0);

    hook.invoke(event);

    Counter versionCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("version", "1.3.1").counter();
    assertTrue(versionCounter != null && versionCounter.count() == 1.0);
  }

  @Test
  public void testVersionExtractedFromCliFormat() throws Exception {
    // CLI format also includes cli.cli_version
    MetadataChangeLog event = createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0);

    hook.invoke(event);

    Counter versionCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("version", "0.14.0").counter();
    assertTrue(versionCounter != null && versionCounter.count() == 1.0);
  }

  @Test
  public void testSinkHostExtractedFromRecipe() throws Exception {
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn execUrn = UrnUtils.getUrn(TEST_EXECUTION_REQUEST_URN);
    String ingestionSourceUrnStr = "urn:li:dataHubIngestionSource:my-source";

    mockExecutionRequestInput(mockClient, execUrn, ingestionSourceUrnStr);
    mockGlobalTags(mockClient, "warning");
    mockIngestionSourceInfo(
        mockClient, "{\"sink\":{\"config\":{\"server\":\"https://customer1.example.com\"}}}");

    IngestionMetricsHook hookWithClient = new IngestionMetricsHook(meterRegistry, mockClient, true);
    hookWithClient.init(TestOperationContexts.systemContextNoSearchAuthorization());

    hookWithClient.invoke(createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0));

    Counter sinkHostCounter =
        meterRegistry
            .find("com.datahub.ingest.runs")
            .tag("sink_host", "customer1.example.com")
            .counter();
    assertTrue(sinkHostCounter != null && sinkHostCounter.count() == 1.0);
  }

  @Test
  public void testSinkHostCacheAvoidsDuplicateRpcs() throws Exception {
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    Urn execUrn = UrnUtils.getUrn(TEST_EXECUTION_REQUEST_URN);
    String ingestionSourceUrnStr = "urn:li:dataHubIngestionSource:cached-sink-host-source";
    Urn ingestionSourceUrn = UrnUtils.getUrn(ingestionSourceUrnStr);

    mockExecutionRequestInput(mockClient, execUrn, ingestionSourceUrnStr);
    mockGlobalTags(mockClient, "warning");
    mockIngestionSourceInfo(
        mockClient, "{\"sink\":{\"config\":{\"server\":\"https://customer2.example.com\"}}}");

    IngestionMetricsHook hookWithClient = new IngestionMetricsHook(meterRegistry, mockClient, true);
    hookWithClient.init(TestOperationContexts.systemContextNoSearchAuthorization());

    hookWithClient.invoke(createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0));
    hookWithClient.invoke(createTestEvent("SUCCEEDED", 2000L, 200, 10, 0, 0));

    // dataHubIngestionSourceInfo fetched only once (cached for second call)
    verify(mockClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq("dataHubIngestionSource"),
            eq(ingestionSourceUrn),
            eq(ImmutableSet.of("dataHubIngestionSourceInfo")));
  }

  @Test
  public void testRestateChangeTypeIgnored() throws Exception {
    MetadataChangeLog event = createTestEvent("SUCCEEDED", 1000L, 100, 5, 0, 0);
    event.setChangeType(ChangeType.RESTATE);

    hook.invoke(event);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  // Helper methods

  private MetadataChangeLog createTestEvent(
      String status,
      Long durationMs,
      int eventsProduced,
      int recordsWritten,
      int warnings,
      int failures)
      throws Exception {

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus(status);
    if (durationMs != null) {
      result.setDurationMs(durationMs);
    }
    result.setStartTimeMs(System.currentTimeMillis() - (durationMs != null ? durationMs : 0));

    // Build structured report JSON
    StringBuilder failuresJson = new StringBuilder("[");
    for (int i = 0; i < failures; i++) {
      if (i > 0) failuresJson.append(",");
      failuresJson.append("{\"error\":\"test error ").append(i).append("\"}");
    }
    failuresJson.append("]");

    StringBuilder warningsJson = new StringBuilder("[");
    for (int i = 0; i < warnings; i++) {
      if (i > 0) warningsJson.append(",");
      warningsJson.append("{\"message\":\"test warning ").append(i).append("\"}");
    }
    warningsJson.append("]");

    String reportJson =
        String.format(
            "{"
                + "\"cli\": {\"cli_version\": \"0.14.0\"},"
                + "\"Source (file)\": {"
                + "  \"events_produced\": %d,"
                + "  \"warnings\": %s,"
                + "  \"failures\": %s,"
                + "  \"platform\": \"file\""
                + "},"
                + "\"Sink (datahub-rest)\": {"
                + "  \"total_records_written\": %d,"
                + "  \"failures\": []"
                + "}"
                + "}",
            eventsProduced, warningsJson, failuresJson, recordsWritten);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    return event;
  }

  /**
   * Creates a test event in executor format (RUN_INGEST). The executor uses a different JSON
   * structure: {"source": {"type": "...", "report": {...}}, "sink": {"type": "...", "report":
   * {...}}}
   */
  private MetadataChangeLog createExecutorTestEvent(
      String status,
      Long durationMs,
      int eventsProduced,
      int recordsWritten,
      int warnings,
      int failures)
      throws Exception {

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus(status);
    if (durationMs != null) {
      result.setDurationMs(durationMs);
    }
    result.setStartTimeMs(System.currentTimeMillis() - (durationMs != null ? durationMs : 0));

    // Build structured report JSON in executor format
    StringBuilder failuresJson = new StringBuilder("[");
    for (int i = 0; i < failures; i++) {
      if (i > 0) failuresJson.append(",");
      failuresJson.append("{\"error\":\"test error ").append(i).append("\"}");
    }
    failuresJson.append("]");

    StringBuilder warningsJson = new StringBuilder("[");
    for (int i = 0; i < warnings; i++) {
      if (i > 0) warningsJson.append(",");
      warningsJson.append("{\"message\":\"test warning ").append(i).append("\"}");
    }
    warningsJson.append("]");

    // Executor format: nested source.report and sink.report
    String reportJson =
        String.format(
            "{"
                + "\"cli\": {\"cli_version\": \"1.3.1\"},"
                + "\"source\": {"
                + "  \"type\": \"snowflake\","
                + "  \"report\": {"
                + "    \"events_produced\": %d,"
                + "    \"warnings\": %s,"
                + "    \"failures\": %s,"
                + "    \"platform\": \"snowflake\""
                + "  }"
                + "},"
                + "\"sink\": {"
                + "  \"type\": \"datahub-rest\","
                + "  \"report\": {"
                + "    \"total_records_written\": %d,"
                + "    \"failures\": []"
                + "  }"
                + "}"
                + "}",
            eventsProduced, warningsJson, failuresJson, recordsWritten);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("RUN_INGEST"); // Executor uses RUN_INGEST
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(ENTITY_TYPE);
    event.setAspectName(ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setAspect(GenericRecordUtils.serializeAspect(result));
    event.setEntityUrn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));

    return event;
  }

  /** Mocks getV2 for ExecutionRequestInput, returning an input with the given ingestion source. */
  private void mockExecutionRequestInput(
      SystemEntityClient mockClient, Urn execUrn, String ingestionSourceUrnStr) throws Exception {
    ExecutionRequestSource source = new ExecutionRequestSource();
    source.setType("INGESTION_SOURCE");
    source.setIngestionSource(UrnUtils.getUrn(ingestionSourceUrnStr));

    ExecutionRequestInput input = new ExecutionRequestInput();
    input.setTask("RUN_INGEST");
    input.setArgs(new StringMap());
    input.setExecutorId("default");
    input.setRequestedAt(System.currentTimeMillis());
    input.setSource(source);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        "dataHubExecutionRequestInput", new EnvelopedAspect().setValue(new Aspect(input.data())));
    EntityResponse response = new EntityResponse();
    response.setAspects(aspects);

    when(mockClient.getV2(
            any(OperationContext.class),
            eq(execUrn.getEntityType()),
            eq(execUrn),
            eq(ImmutableSet.of("dataHubExecutionRequestInput"))))
        .thenReturn(response);
  }

  /** Mocks getV2 for GlobalTags on any dataHubIngestionSource, returning the given tag name. */
  private void mockGlobalTags(SystemEntityClient mockClient, String tagName) throws Exception {
    TagAssociation tagAssoc = new TagAssociation();
    tagAssoc.setTag(new TagUrn(tagName));
    GlobalTags tags = new GlobalTags();
    tags.setTags(new TagAssociationArray(tagAssoc));

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put("globalTags", new EnvelopedAspect().setValue(new Aspect(tags.data())));
    EntityResponse response = new EntityResponse();
    response.setAspects(aspects);

    when(mockClient.getV2(
            any(OperationContext.class),
            eq("dataHubIngestionSource"),
            any(Urn.class),
            eq(ImmutableSet.of("globalTags"))))
        .thenReturn(response);
  }

  /**
   * Mocks getV2 for DataHubIngestionSourceInfo on any dataHubIngestionSource, returning the given
   * recipe JSON.
   */
  private void mockIngestionSourceInfo(SystemEntityClient mockClient, String recipeJson)
      throws Exception {
    DataHubIngestionSourceConfig config = new DataHubIngestionSourceConfig();
    config.setRecipe(recipeJson);

    DataHubIngestionSourceInfo sourceInfo = new DataHubIngestionSourceInfo();
    sourceInfo.setName("test-source");
    sourceInfo.setType("snowflake");
    sourceInfo.setConfig(config);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        "dataHubIngestionSourceInfo",
        new EnvelopedAspect().setValue(new Aspect(sourceInfo.data())));
    EntityResponse response = new EntityResponse();
    response.setAspects(aspects);

    when(mockClient.getV2(
            any(OperationContext.class),
            eq("dataHubIngestionSource"),
            any(Urn.class),
            eq(ImmutableSet.of("dataHubIngestionSourceInfo"))))
        .thenReturn(response);
  }
}
