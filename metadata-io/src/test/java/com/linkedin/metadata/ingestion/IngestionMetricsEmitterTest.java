package com.linkedin.metadata.ingestion;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestionMetricsEmitterTest {

  private static final String TEST_EXECUTION_REQUEST_URN =
      "urn:li:dataHubExecutionRequest:test-pipeline-12345";

  private IngestionMetricsEmitter emitter;
  private SimpleMeterRegistry meterRegistry;

  @BeforeMethod
  public void setup() {
    meterRegistry = new SimpleMeterRegistry();
    emitter = new IngestionMetricsEmitter(meterRegistry);
  }

  @Test
  public void testCliIngestProcessed() throws Exception {
    AspectsBatch batch =
        toBatch(
            List.of(
                createBatchItem(
                    createCliResult("SUCCEEDED", 1000L, 100, 5, 2, 3), ChangeType.UPSERT)));

    emitter.processProposals(batch);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);

    DistributionSummary durationSummary =
        meterRegistry.find("com.datahub.ingest.duration_ms").summary();
    assertNotNull(durationSummary);
    assertEquals(durationSummary.totalAmount(), 1000.0);

    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertNotNull(eventsCounter);
    assertEquals(eventsCounter.count(), 100.0);

    Counter recordsCounter = meterRegistry.find("com.datahub.ingest.records_written").counter();
    assertNotNull(recordsCounter);
    assertEquals(recordsCounter.count(), 5.0);

    Counter warningsCounter = meterRegistry.find("com.datahub.ingest.warnings").counter();
    assertNotNull(warningsCounter);
    assertEquals(warningsCounter.count(), 2.0);

    Counter failuresCounter = meterRegistry.find("com.datahub.ingest.failures").counter();
    assertNotNull(failuresCounter);
    assertEquals(failuresCounter.count(), 3.0);

    // Verify connector tag from CLI format (Source (file) → connector=file)
    Counter connectorCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("connector", "file").counter();
    assertNotNull(connectorCounter);
    assertEquals(connectorCounter.count(), 1.0);

    // Verify cli_version tag
    Counter cliVersionCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("cli_version", "0.14.0").counter();
    assertNotNull(cliVersionCounter);
    assertEquals(cliVersionCounter.count(), 1.0);
  }

  @Test
  public void testRunIngestProcessed() throws Exception {
    AspectsBatch batch =
        toBatch(
            List.of(
                createBatchItem(
                    createExecutorResult("SUCCEEDED", 2000L, 500, 50, 1, 0), ChangeType.UPSERT)));

    emitter.processProposals(batch);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);

    DistributionSummary durationSummary =
        meterRegistry.find("com.datahub.ingest.duration_ms").summary();
    assertNotNull(durationSummary);
    assertEquals(durationSummary.totalAmount(), 2000.0);

    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertNotNull(eventsCounter);
    assertEquals(eventsCounter.count(), 500.0);

    Counter recordsCounter = meterRegistry.find("com.datahub.ingest.records_written").counter();
    assertNotNull(recordsCounter);
    assertEquals(recordsCounter.count(), 50.0);

    // Verify connector tag from executor format
    Counter connectorCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("connector", "snowflake").counter();
    assertNotNull(connectorCounter);
    assertEquals(connectorCounter.count(), 1.0);
  }

  @Test
  public void testNonIngestionAspectIgnored() throws Exception {
    BatchItem item = mock(BatchItem.class);
    when(item.getAspectName()).thenReturn("schemaMetadata");
    when(item.getChangeType()).thenReturn(ChangeType.UPSERT);

    emitter.processProposals(toBatch(List.of(item)));

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testRestateIgnored() throws Exception {
    AspectsBatch batch =
        toBatch(
            List.of(
                createBatchItem(
                    createCliResult("SUCCEEDED", 1000L, 100, 5, 0, 0), ChangeType.RESTATE)));

    emitter.processProposals(batch);

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testNonIngestionReportIgnored() throws Exception {
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("TEST_CONNECTION"); // Not CLI_INGEST or RUN_INGEST
    structuredReport.setSerializedValue("{}");
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    emitter.processProposals(toBatch(List.of(createBatchItem(result, ChangeType.UPSERT))));

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testMalformedJsonDoesNotThrow() throws Exception {
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue("{invalid json{{");
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    // Should not throw
    emitter.processProposals(toBatch(List.of(createBatchItem(result, ChangeType.UPSERT))));

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testSinkFailuresRecorded() throws Exception {
    String reportJson =
        "{"
            + "\"cli\": {\"cli_version\": \"0.14.0\"},"
            + "\"source\": {"
            + "  \"type\": \"file\","
            + "  \"report\": {"
            + "    \"events_produced\": 100,"
            + "    \"warnings\": [],"
            + "    \"failures\": [],"
            + "    \"platform\": \"file\""
            + "  }"
            + "},"
            + "\"sink\": {"
            + "  \"type\": \"datahub-rest\","
            + "  \"report\": {"
            + "    \"total_records_written\": 90,"
            + "    \"failures\": [\"write failed\",\"timeout\"]"
            + "  }"
            + "}"
            + "}";

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    emitter.processProposals(toBatch(List.of(createBatchItem(result, ChangeType.UPSERT))));

    Counter sinkFailuresCounter = meterRegistry.find("com.datahub.ingest.sink_failures").counter();
    assertNotNull(sinkFailuresCounter);
    assertEquals(sinkFailuresCounter.count(), 2.0);
  }

  @Test
  public void testMultipleItemsMixed() throws Exception {
    // One ingestion item
    BatchItem ingestionItem =
        createBatchItem(createCliResult("SUCCEEDED", 1000L, 100, 5, 0, 0), ChangeType.UPSERT);

    // One non-ingestion item (different aspect)
    BatchItem nonIngestionItem = mock(BatchItem.class);
    when(nonIngestionItem.getAspectName()).thenReturn("schemaMetadata");
    when(nonIngestionItem.getChangeType()).thenReturn(ChangeType.UPSERT);

    emitter.processProposals(toBatch(List.of(ingestionItem, nonIngestionItem)));

    // Only the ingestion item should be counted
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildRunEventMapUsesConnectorField() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String reportJsonStr =
        "{"
            + "\"source\": {"
            + "  \"type\": \"snowflake\","
            + "  \"report\": {"
            + "    \"events_produced\": 100,"
            + "    \"warnings\": [], \"failures\": []"
            + "  }"
            + "},"
            + "\"sink\": {"
            + "  \"type\": \"datahub-rest\","
            + "  \"report\": {\"total_records_written\": 90, \"failures\": []}"
            + "}"
            + "}";
    JsonNode reportJson = mapper.readTree(reportJsonStr);
    JsonNode sourceReport = reportJson.path("source").path("report");
    JsonNode sinkReport = reportJson.path("sink").path("report");

    IngestionMetricsEmitter.RunEvent runEvent =
        new IngestionMetricsEmitter.RunEvent(
            Urn.createFromString(TEST_EXECUTION_REQUEST_URN),
            "snowflake",
            "SUCCEEDED",
            1000L,
            100L,
            90L,
            0,
            0,
            0,
            reportJson,
            sourceReport,
            sinkReport);

    Map<String, Object> event = emitter.buildRunEventMap(runEvent);

    // Verify "connector" field (not "platform")
    assertEquals(event.get("connector"), "snowflake");
    assertTrue(!event.containsKey("platform"));
    // Verify no customer field
    assertTrue(!event.containsKey("customer"));
    // Verify other essential fields
    assertEquals(event.get("status"), "SUCCEEDED");
    assertEquals(event.get("execution_id"), "test-pipeline-12345");
  }

  @Test
  public void testThreeLabelsPresent() throws Exception {
    AspectsBatch batch =
        toBatch(
            List.of(
                createBatchItem(
                    createExecutorResult("SUCCEEDED", 2000L, 500, 50, 0, 0), ChangeType.UPSERT)));

    emitter.processProposals(batch);

    // All 3 labels should be present on the runs counter
    Counter counter =
        meterRegistry
            .find("com.datahub.ingest.runs")
            .tag("connector", "snowflake")
            .tag("status", "SUCCEEDED")
            .tag("cli_version", "1.3.1")
            .counter();
    assertNotNull(counter);
    assertEquals(counter.count(), 1.0);
  }

  @Test
  public void testFailureStatusRecorded() throws Exception {
    AspectsBatch batch =
        toBatch(
            List.of(
                createBatchItem(createCliResult("FAILURE", 500L, 0, 0, 0, 2), ChangeType.UPSERT)));

    emitter.processProposals(batch);

    Counter counter =
        meterRegistry.find("com.datahub.ingest.runs").tag("status", "FAILURE").counter();
    assertNotNull(counter);
    assertEquals(counter.count(), 1.0);

    Counter failuresCounter =
        meterRegistry.find("com.datahub.ingest.failures").tag("status", "FAILURE").counter();
    assertNotNull(failuresCounter);
    assertEquals(failuresCounter.count(), 2.0);
  }

  @Test
  public void testMissingStructuredReportIgnored() throws Exception {
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);
    // No structuredReport set

    emitter.processProposals(toBatch(List.of(createBatchItem(result, ChangeType.UPSERT))));

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  @Test
  public void testMissingSourceKeyInReport() throws Exception {
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");
    result.setDurationMs(1000L);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue("{\"cli\": {\"cli_version\": \"0.14.0\"}}");
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    // Should not throw — emitter handles missing source/sink gracefully
    emitter.processProposals(toBatch(List.of(createBatchItem(result, ChangeType.UPSERT))));

    // Run is still counted (it's a valid ingestion report)
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);

    // Connector defaults to unknown
    Counter connectorCounter =
        meterRegistry.find("com.datahub.ingest.runs").tag("connector", "unknown").counter();
    assertNotNull(connectorCounter);
    assertEquals(connectorCounter.count(), 1.0);

    // Numeric fields default to 0
    Counter eventsCounter = meterRegistry.find("com.datahub.ingest.events_produced").counter();
    assertNotNull(eventsCounter);
    assertEquals(eventsCounter.count(), 0.0);
  }

  @Test
  public void testMissingCliVersionDefaultsToUnknown() throws Exception {
    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus("SUCCEEDED");

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue("{}");
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    emitter.processProposals(toBatch(List.of(createBatchItem(result, ChangeType.UPSERT))));

    Counter counter =
        meterRegistry.find("com.datahub.ingest.runs").tag("cli_version", "unknown").counter();
    assertNotNull(counter);
    assertEquals(counter.count(), 1.0);
  }

  @Test
  public void testEmptyBatch() {
    emitter.processProposals(toBatch(Collections.emptyList()));

    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);
  }

  /**
   * Simulates the async guard in EntityServiceImpl: emitter is only called when async=false. When
   * async=true, the proposal goes to Kafka; the MCE consumer calls back with async=false. This test
   * replicates that logic to verify no double-counting.
   */
  @Test
  public void testAsyncGuardPreventsDoubleCounting() throws Exception {
    AspectsBatch batch =
        toBatch(
            List.of(
                createBatchItem(
                    createCliResult("SUCCEEDED", 1000L, 100, 5, 0, 0), ChangeType.UPSERT)));

    // Replicate EntityServiceImpl logic: if (!async) { emitter.processProposals(batch); }
    boolean asyncTrue = true;
    if (!asyncTrue) {
      emitter.processProposals(batch);
    }

    // async=true path: nothing emitted
    Counter runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertTrue(runsCounter == null || runsCounter.count() == 0.0);

    // Now the MCE consumer calls back with async=false
    boolean asyncFalse = false;
    if (!asyncFalse) {
      emitter.processProposals(batch);
    }

    // Counted exactly once
    runsCounter = meterRegistry.find("com.datahub.ingest.runs").counter();
    assertNotNull(runsCounter);
    assertEquals(runsCounter.count(), 1.0);
  }

  // Helper methods

  private BatchItem createBatchItem(ExecutionRequestResult result, ChangeType changeType)
      throws Exception {
    BatchItem item = mock(BatchItem.class);
    when(item.getAspectName()).thenReturn("dataHubExecutionRequestResult");
    when(item.getChangeType()).thenReturn(changeType);
    when(item.getUrn()).thenReturn(Urn.createFromString(TEST_EXECUTION_REQUEST_URN));
    when(item.getAspect(ExecutionRequestResult.class)).thenReturn(result);
    return item;
  }

  private AspectsBatch toBatch(List<BatchItem> items) {
    AspectsBatch batch = mock(AspectsBatch.class);
    when(batch.getItems()).thenReturn((Collection) items);
    return batch;
  }

  private ExecutionRequestResult createCliResult(
      String status,
      Long durationMs,
      int eventsProduced,
      int recordsWritten,
      int warnings,
      int failures) {

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus(status);
    if (durationMs != null) {
      result.setDurationMs(durationMs);
    }
    result.setStartTimeMs(System.currentTimeMillis() - (durationMs != null ? durationMs : 0));

    StringBuilder failuresJson = new StringBuilder("[");
    for (int i = 0; i < failures; i++) {
      if (i > 0) failuresJson.append(",");
      failuresJson
          .append("{\"title\":\"Extraction error\",\"message\":\"test error ")
          .append(i)
          .append("\",\"context\":[\"ErrorType: detail ")
          .append(i)
          .append("\"]}");
    }
    failuresJson.append("]");

    StringBuilder warningsJson = new StringBuilder("[");
    for (int i = 0; i < warnings; i++) {
      if (i > 0) warningsJson.append(",");
      warningsJson
          .append("{\"title\":\"Config issue\",\"message\":\"test warning ")
          .append(i)
          .append("\",\"context\":[]}");
    }
    warningsJson.append("]");

    String reportJson =
        String.format(
            "{"
                + "\"cli\": {\"cli_version\": \"0.14.0\"},"
                + "\"source\": {"
                + "  \"type\": \"file\","
                + "  \"report\": {"
                + "    \"events_produced\": %d,"
                + "    \"warnings\": %s,"
                + "    \"failures\": %s,"
                + "    \"platform\": \"file\""
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
    structuredReport.setType("CLI_INGEST");
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    return result;
  }

  private ExecutionRequestResult createExecutorResult(
      String status,
      Long durationMs,
      int eventsProduced,
      int recordsWritten,
      int warnings,
      int failures) {

    ExecutionRequestResult result = new ExecutionRequestResult();
    result.setStatus(status);
    if (durationMs != null) {
      result.setDurationMs(durationMs);
    }
    result.setStartTimeMs(System.currentTimeMillis() - (durationMs != null ? durationMs : 0));

    StringBuilder failuresJson = new StringBuilder("[");
    for (int i = 0; i < failures; i++) {
      if (i > 0) failuresJson.append(",");
      failuresJson
          .append("{\"title\":\"Extraction error\",\"message\":\"test error ")
          .append(i)
          .append("\",\"context\":[\"ErrorType: detail ")
          .append(i)
          .append("\"]}");
    }
    failuresJson.append("]");

    StringBuilder warningsJson = new StringBuilder("[");
    for (int i = 0; i < warnings; i++) {
      if (i > 0) warningsJson.append(",");
      warningsJson
          .append("{\"title\":\"Config issue\",\"message\":\"test warning ")
          .append(i)
          .append("\",\"context\":[]}");
    }
    warningsJson.append("]");

    // Executor format with gms_version in sink report
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
                + "    \"failures\": [],"
                + "    \"gms_version\": \"v0.3.17rc5\""
                + "  }"
                + "}"
                + "}",
            eventsProduced, warningsJson, failuresJson, recordsWritten);

    StructuredExecutionReport structuredReport = new StructuredExecutionReport();
    structuredReport.setType("RUN_INGEST");
    structuredReport.setSerializedValue(reportJson);
    structuredReport.setContentType("application/json");
    result.setStructuredReport(structuredReport);

    return result;
  }
}
