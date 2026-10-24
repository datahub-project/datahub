package com.linkedin.metadata.ingestion.sideeffects;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
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
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
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
  private static final Urn OLD_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-old");
  private static final Urn NEW_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-new");

  private final IngestionSourceRunSummarySideEffect sideEffect =
      new IngestionSourceRunSummarySideEffect()
          .setConfig(
              AspectPluginConfig.builder()
                  .className(IngestionSourceRunSummarySideEffect.class.getName())
                  .enabled(true)
                  .supportedOperations(List.of("UPSERT"))
                  .supportedEntityAspectNames(
                      List.of(
                          AspectPluginConfig.EntityAspectName.builder()
                              .entityName(EXECUTION_REQUEST_ENTITY_NAME)
                              .aspectName(EXECUTION_REQUEST_INPUT_ASPECT_NAME)
                              .build(),
                          AspectPluginConfig.EntityAspectName.builder()
                              .entityName(EXECUTION_REQUEST_ENTITY_NAME)
                              .aspectName(EXECUTION_REQUEST_RESULT_ASPECT_NAME)
                              .build()))
                  .build());

  private final Map<Urn, Map<String, Aspect>> store = new HashMap<>();
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    store.clear();
    CachingAspectRetriever aspectRetriever = mock(CachingAspectRetriever.class);
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
    put(
        SOURCE_URN,
        INGESTION_INFO_ASPECT_NAME,
        new DataHubIngestionSourceInfo()
            .setName("source a")
            .setType("mysql")
            .setConfig(new DataHubIngestionSourceConfig().setRecipe("{}")));
  }

  @Test
  public void testNewRunSetsSummary() {
    DataHubIngestionSourceRunSummary summary =
        onlySummary(
            run(
                List.of(
                    mcl(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200)))));

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
            run(List.of(mcl(NEW_RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("SUCCESS")))));

    assertEquals(summary.getLastExecutionRequest(), NEW_RUN);
    assertEquals(summary.getLastRunStatus(), "SUCCESS");
  }

  @Test
  public void testLateResultForOlderRunIsIgnored() {
    put(OLD_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 100));
    put(SOURCE_URN, INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME, summary(NEW_RUN, 200, "RUNNING"));

    assertTrue(
        run(List.of(mcl(OLD_RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("FAILURE"))))
            .isEmpty());
  }

  @Test
  public void testUnchangedSummaryIsNotRewritten() {
    put(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200));
    put(SOURCE_URN, INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME, summary(NEW_RUN, 200, "SUCCESS"));

    assertTrue(
        run(List.of(mcl(NEW_RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("SUCCESS"))))
            .isEmpty());
  }

  @Test
  public void testRunForDeletedSourceIsIgnored() {
    store.remove(SOURCE_URN);

    assertTrue(
        run(List.of(mcl(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200))))
            .isEmpty());
  }

  @Test
  public void testNewestRunInBatchWins() {
    put(OLD_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 100));

    DataHubIngestionSourceRunSummary summary =
        onlySummary(
            run(
                List.of(
                    mcl(NEW_RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(SOURCE_URN, 200)),
                    mcl(OLD_RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("SUCCESS")))));

    assertEquals(summary.getLastExecutionRequest(), NEW_RUN);
  }

  private List<MCPItem> run(List<MCLItem> items) {
    return sideEffect
        .postMCPSideEffect(OperationFingerprint.EMPTY, items, retrieverContext)
        .toList();
  }

  private static DataHubIngestionSourceRunSummary onlySummary(List<MCPItem> output) {
    assertEquals(output.size(), 1);
    assertEquals(output.get(0).getUrn(), SOURCE_URN);
    assertEquals(output.get(0).getAspectName(), INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME);
    return output.get(0).getAspect(DataHubIngestionSourceRunSummary.class);
  }

  private MCLItem mcl(Urn urn, String aspectName, RecordTemplate aspect) {
    return MCLItemImpl.builder()
        .metadataChangeLog(
            new MetadataChangeLog()
                .setEntityUrn(urn)
                .setEntityType(EXECUTION_REQUEST_ENTITY_NAME)
                .setChangeType(ChangeType.UPSERT)
                .setAspectName(aspectName)
                .setAspect(GenericRecordUtils.serializeAspect(aspect))
                .setCreated(AuditStampUtils.createDefaultAuditStamp()))
        .build(retrieverContext.getAspectRetriever());
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
