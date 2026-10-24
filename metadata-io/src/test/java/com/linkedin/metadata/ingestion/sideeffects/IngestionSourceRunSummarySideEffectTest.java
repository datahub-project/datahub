package com.linkedin.metadata.ingestion.sideeffects;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestionSourceRunSummarySideEffectTest {

  private static final Urn SOURCE_URN = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-a");
  private static final Urn OTHER_SOURCE_URN =
      UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-b");
  private static final Urn OLD_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-old");
  private static final Urn NEW_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-new");

  private final IngestionSourceRunSummarySideEffect sideEffect =
      new IngestionSourceRunSummarySideEffect()
          .setConfig(IngestionSourceRunSummarySideEffect.pluginConfig());

  private final Map<Urn, Map<String, Aspect>> store = new HashMap<>();
  private CachingAspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    store.clear();
    aspectRetriever = mock(CachingAspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(new TestEntityRegistry());
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              Set<String> aspectNames = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> found = new HashMap<>();
              for (Urn urn : urns) {
                Map<String, Aspect> aspects = new HashMap<>(store.getOrDefault(urn, Map.of()));
                aspects.keySet().retainAll(aspectNames);
                if (!aspects.isEmpty()) {
                  found.put(urn, aspects);
                }
              }
              return found;
            });
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(aspectRetriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();
    putSource(SOURCE_URN);
  }

  @Test
  public void testNewRunSetsSummary() {
    DataHubIngestionSourceRunSummary summary =
        onlySummary(
            run(mcl(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200))),
            SOURCE_URN);

    assertEquals(summary.getLastExecutionRequest(), NEW_RUN);
    assertEquals(summary.getLastRunRequestedAtMillis(), 200L);
    assertFalse(summary.hasLastRunStatus());
  }

  @Test
  public void testResultUpdatesStatusOfLatestRun() {
    put(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200));
    put(SOURCE_URN, INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME, summary(NEW_RUN, 200, "RUNNING"));

    DataHubIngestionSourceRunSummary summary =
        onlySummary(
            run(mcl(NEW_RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("SUCCESS"))), SOURCE_URN);

    assertEquals(summary.getLastExecutionRequest(), NEW_RUN);
    assertEquals(summary.getLastRunStatus(), "SUCCESS");
  }

  @Test
  public void testLateResultForOlderRunIsIgnored() {
    put(OLD_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 100));
    put(SOURCE_URN, INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME, summary(NEW_RUN, 200, "RUNNING"));

    assertTrue(
        run(mcl(OLD_RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("FAILURE"))).isEmpty());
  }

  @Test
  public void testUnchangedSummaryIsNotRewritten() {
    put(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200));
    put(SOURCE_URN, INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME, summary(NEW_RUN, 200, "SUCCESS"));

    assertTrue(
        run(mcl(NEW_RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("SUCCESS"))).isEmpty());
  }

  @Test
  public void testRunForDeletedSourceIsIgnored() {
    store.remove(SOURCE_URN);

    assertTrue(
        run(mcl(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200))).isEmpty());
  }

  @Test
  public void testRequestWithoutIngestionSourceIsIgnored() {
    ExecutionRequestInput testConnection =
        input(SOURCE_URN, 200)
            .setSource(new ExecutionRequestSource().setType("MANUAL_INGESTION_SOURCE"));

    assertTrue(run(mcl(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, testConnection)).isEmpty());
  }

  @Test
  public void testNewestRunInBatchWins() {
    put(OLD_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 100));

    DataHubIngestionSourceRunSummary summary =
        onlySummary(
            run(
                mcl(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200)),
                mcl(OLD_RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("SUCCESS"))),
            SOURCE_URN);

    assertEquals(summary.getLastExecutionRequest(), NEW_RUN);
  }

  @Test
  public void testEachSourceInBatchGetsItsOwnSummary() {
    putSource(OTHER_SOURCE_URN);

    List<MCPItem> output =
        run(
            mcl(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200)),
            mcl(OLD_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(OTHER_SOURCE_URN, 100)));

    Map<Urn, Urn> runBySource = new HashMap<>();
    output.forEach(
        item ->
            runBySource.put(
                item.getUrn(),
                item.getAspect(DataHubIngestionSourceRunSummary.class).getLastExecutionRequest()));
    assertEquals(runBySource, Map.of(SOURCE_URN, NEW_RUN, OTHER_SOURCE_URN, OLD_RUN));
  }

  @Test
  public void testInputIsPreferredAsTrigger() {
    AuditStamp inputStamp = AuditStampUtils.createDefaultAuditStamp().setTime(1L);

    MCPItem item =
        run(
                mcl(NEW_RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("RUNNING")),
                mcl(
                    NEW_RUN,
                    EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                    input(SOURCE_URN, 200),
                    ChangeType.UPSERT,
                    inputStamp))
            .get(0);

    assertEquals(item.getAuditStamp(), inputStamp);
  }

  @Test
  public void testRestateIsIgnored() {
    assertTrue(
        run(mcl(
                NEW_RUN,
                EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                input(SOURCE_URN, 200),
                ChangeType.RESTATE,
                AuditStampUtils.createDefaultAuditStamp()))
            .isEmpty());
  }

  @Test
  public void testReadFailureDoesNotPropagate() {
    MCLItem item = mcl(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200));
    doThrow(new RuntimeException("database unavailable"))
        .when(aspectRetriever)
        .getLatestAspectObjects(any(), any(), any());

    assertTrue(run(item).isEmpty());
  }

  private List<MCPItem> run(MCLItem... items) {
    return sideEffect
        .postApply(OperationFingerprint.EMPTY, List.of(items), retrieverContext)
        .toList();
  }

  private static DataHubIngestionSourceRunSummary onlySummary(List<MCPItem> output, Urn source) {
    assertEquals(output.size(), 1);
    assertEquals(output.get(0).getUrn(), source);
    assertEquals(output.get(0).getAspectName(), INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME);
    return output.get(0).getAspect(DataHubIngestionSourceRunSummary.class);
  }

  private MCLItem mcl(Urn urn, String aspectName, RecordTemplate aspect) {
    return mcl(
        urn, aspectName, aspect, ChangeType.UPSERT, AuditStampUtils.createDefaultAuditStamp());
  }

  private MCLItem mcl(
      Urn urn,
      String aspectName,
      RecordTemplate aspect,
      ChangeType changeType,
      AuditStamp auditStamp) {
    return MCLItemImpl.builder()
        .metadataChangeLog(
            new MetadataChangeLog()
                .setEntityUrn(urn)
                .setEntityType(EXECUTION_REQUEST_ENTITY_NAME)
                .setChangeType(changeType)
                .setAspectName(aspectName)
                .setAspect(GenericRecordUtils.serializeAspect(aspect))
                .setCreated(auditStamp))
        .build(retrieverContext.getAspectRetriever());
  }

  private void putSource(Urn sourceUrn) {
    put(
        sourceUrn,
        INGESTION_INFO_ASPECT_NAME,
        new DataHubIngestionSourceInfo()
            .setName(sourceUrn.getId())
            .setType("mysql")
            .setConfig(new DataHubIngestionSourceConfig().setRecipe("{}")));
  }

  private void put(Urn urn, String aspectName, RecordTemplate aspect) {
    store.computeIfAbsent(urn, u -> new HashMap<>()).put(aspectName, new Aspect(aspect.data()));
  }

  private static ExecutionRequestInput input(Urn sourceUrn, long requestedAt) {
    return new ExecutionRequestInput()
        .setTask("RUN_INGEST")
        .setArgs(new StringMap())
        .setExecutorId("default")
        .setRequestedAt(requestedAt)
        .setSource(
            new ExecutionRequestSource().setType("INGESTION_SOURCE").setIngestionSource(sourceUrn));
  }

  private static ExecutionRequestResult result(String status) {
    return new ExecutionRequestResult().setStatus(status);
  }

  private static DataHubIngestionSourceRunSummary summary(
      Urn runUrn, long requestedAt, String status) {
    return new DataHubIngestionSourceRunSummary()
        .setLastExecutionRequest(runUrn)
        .setLastRunRequestedAtMillis(requestedAt)
        .setLastRunStatus(status);
  }
}
