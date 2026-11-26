package com.linkedin.metadata.kafka.hook.ingestion;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
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
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceRunEvent;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LatestIngestionRuns;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestionSourceRunEventHookTest {

  private static final Urn SOURCE = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-a");
  private static final Urn RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run");
  private static final Urn EARLIER_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:earlier");

  private final Map<Urn, ExecutionRequestInput> inputs = new HashMap<>();
  private final Set<Urn> sources = new HashSet<>();
  private OperationContext opContext;
  private SystemEntityClient client;
  private EntitySearchService entitySearchService;
  private IngestionSourceRunEventHook hook;

  @BeforeMethod
  public void setup() throws Exception {
    inputs.clear();
    sources.clear();
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    client = mock(SystemEntityClient.class);
    entitySearchService = mock(EntitySearchService.class);
    hook = new IngestionSourceRunEventHook(client, entitySearchService, true);
    sources.add(SOURCE);
    inputs.put(RUN, input(SOURCE, 100L));
    when(client.batchGetV2(any(OperationContext.class), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              Map<Urn, EntityResponse> responses = new HashMap<>();
              for (Urn urn : urns) {
                if (sources.contains(urn)) {
                  responses.put(urn, response(INGESTION_INFO_ASPECT_NAME, sourceInfo()));
                } else if (inputs.containsKey(urn)) {
                  responses.put(
                      urn, response(EXECUTION_REQUEST_INPUT_ASPECT_NAME, inputs.get(urn)));
                }
              }
              return responses;
            });
  }

  @Test
  public void testNewRequestRecordsPendingRun() throws Exception {
    hook.invoke(opContext, mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, inputs.get(RUN), null));

    DataHubIngestionSourceRunEvent event = emittedEvents(1).get(0);
    assertEquals(event.getStatus(), "PENDING");
    assertEquals(event.getExecutionRequest(), RUN);
    assertEquals(event.getRequestedAtMillis(), 100L);
    assertEquals(event.getSourceType(), "mysql");
    assertEquals(event.getExecutorId(), "default");
    DataHubIngestionSourceRunSummary summary = emittedSummaries(1).get(0);
    assertEquals(summary.getLastExecutionRequest(), RUN);
    assertEquals(summary.getLastRunStatus(), "PENDING");
    assertFalse(summary.hasLastSuccessfulRunRequestedAtMillis());
  }

  @Test
  public void testTimeoutCountsAsAFailedRun() throws Exception {
    hook.invoke(
        opContext,
        mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("TIMEOUT"), result("RUNNING")));

    assertEquals(emittedEvents(1).get(0).getStatus(), "TIMEOUT");
    assertEquals(
        emittedSummaries(1).get(0).getLastFailedRunRequestedAtMillis(), Long.valueOf(100L));
  }

  @Test
  public void testHeartbeatIsSkippedWithoutReads() throws Exception {
    hook.invokeBatch(
        opContext,
        List.of(
            mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("RUNNING"), result("RUNNING")),
            mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, inputs.get(RUN), inputs.get(RUN))));

    verify(client, never()).batchGetV2(any(OperationContext.class), any(), any());
    verify(client, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testBatchSharesReads() throws Exception {
    Urn otherRun = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:other");
    inputs.put(otherRun, input(SOURCE, 50L));

    hook.invokeBatch(
        opContext,
        List.of(
            mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("SUCCESS"), result("RUNNING")),
            mcl(
                otherRun,
                EXECUTION_REQUEST_RESULT_ASPECT_NAME,
                result("FAILURE"),
                result("RUNNING"))));

    // One read for the results' inputs and one for the source.
    verify(client, times(2)).batchGetV2(any(OperationContext.class), any(), any());
    verify(entitySearchService, never()).getLatestIngestionRuns(any(), any());
    List<DataHubIngestionSourceRunSummary> summaries = emittedSummaries(2);
    assertEquals(summaries.get(1).getLastRunRequestedAtMillis(), 50L);
    assertEquals(summaries.get(1).getLastFailedRunRequestedAtMillis(), Long.valueOf(50L));
  }

  @Test
  public void testRollbackProposesThePreviousSuccess() throws Exception {
    inputs.put(EARLIER_RUN, input(SOURCE, 60L));
    when(entitySearchService.getLatestIngestionRuns(any(), eq(Set.of(SOURCE))))
        .thenReturn(Map.of(SOURCE, new LatestIngestionRuns(RUN, EARLIER_RUN, null)));

    hook.invoke(
        opContext,
        mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("ROLLED_BACK"), result("SUCCESS")));

    DataHubIngestionSourceRunSummary summary = emittedSummaries(1).get(0);
    assertEquals(summary.getLastRunStatus(), "ROLLED_BACK");
    assertEquals(summary.getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(60L));
  }

  @Test
  public void testRollbackKeepsTheSuccessWhileTheIndexStillShowsIt() throws Exception {
    when(entitySearchService.getLatestIngestionRuns(any(), eq(Set.of(SOURCE))))
        .thenReturn(Map.of(SOURCE, new LatestIngestionRuns(RUN, RUN, null)));

    hook.invoke(
        opContext,
        mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("ROLLING_BACK"), result("SUCCESS")));

    assertEquals(
        emittedSummaries(1).get(0).getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(100L));
  }

  @Test
  public void testRollbackOfTheOnlySuccessProposesNone() throws Exception {
    when(entitySearchService.getLatestIngestionRuns(any(), eq(Set.of(SOURCE))))
        .thenReturn(Map.of(SOURCE, new LatestIngestionRuns(RUN, null, null)));

    hook.invoke(
        opContext,
        mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("ROLLED_BACK"), result("SUCCESS")));

    assertFalse(emittedSummaries(1).get(0).hasLastSuccessfulRunRequestedAtMillis());
  }

  @Test
  public void testDeletedSourceIsSkipped() throws Exception {
    sources.clear();

    hook.invoke(opContext, mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, inputs.get(RUN), null));

    verify(client, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testRestateIsIgnored() throws Exception {
    MetadataChangeLog restate =
        mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, inputs.get(RUN), null);
    restate.setChangeType(ChangeType.RESTATE);

    hook.invoke(opContext, restate);

    verify(client, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testFailedWriteIsCountedAndTheBatchContinues() throws Exception {
    Urn otherSource = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-b");
    Urn otherRun = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:other");
    sources.add(otherSource);
    inputs.put(otherRun, input(otherSource, 100L));
    MetricUtils metricUtils = mock(MetricUtils.class);
    OperationContext withMetrics = mock(OperationContext.class);
    when(withMetrics.getMetricUtils()).thenReturn(Optional.of(metricUtils));
    when(client.batchIngestProposals(eq(withMetrics), any(), anyBoolean()))
        .thenThrow(new RemoteInvocationException("down"))
        .thenReturn(List.of());

    hook.invokeBatch(
        withMetrics,
        List.of(
            mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, inputs.get(RUN), null),
            mcl(otherRun, EXECUTION_REQUEST_INPUT_ASPECT_NAME, inputs.get(otherRun), null)));

    verify(client, times(2)).batchIngestProposals(eq(withMetrics), any(), anyBoolean());
    verify(metricUtils).increment(eq(IngestionSourceRunEventHook.class), any(String.class), eq(1d));
  }

  @Test
  public void testFailedReadFailsTheBatch() throws Exception {
    when(client.batchGetV2(any(OperationContext.class), any(), any()))
        .thenThrow(new RemoteInvocationException("down"));

    assertThrows(
        IllegalStateException.class,
        () ->
            hook.invoke(
                opContext,
                mcl(
                    RUN,
                    EXECUTION_REQUEST_RESULT_ASPECT_NAME,
                    result("SUCCESS"),
                    result("RUNNING"))));
    verify(client, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  private List<DataHubIngestionSourceRunEvent> emittedEvents(int count) throws Exception {
    return emitted(
        count, INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME, DataHubIngestionSourceRunEvent.class);
  }

  private List<DataHubIngestionSourceRunSummary> emittedSummaries(int count) throws Exception {
    return emitted(
        count, INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME, DataHubIngestionSourceRunSummary.class);
  }

  @SuppressWarnings("unchecked")
  private <T extends RecordTemplate> List<T> emitted(int count, String aspectName, Class<T> clazz)
      throws Exception {
    ArgumentCaptor<Collection<MetadataChangeProposal>> captor =
        ArgumentCaptor.forClass(Collection.class);
    verify(client, times(count)).batchIngestProposals(any(), captor.capture(), eq(false));
    return captor.getAllValues().stream()
        .flatMap(Collection::stream)
        .filter(proposal -> aspectName.equals(proposal.getAspectName()))
        .peek(proposal -> assertEquals(proposal.getEntityUrn(), SOURCE))
        .map(
            proposal ->
                GenericRecordUtils.deserializeAspect(
                    proposal.getAspect().getValue(), proposal.getAspect().getContentType(), clazz))
        .toList();
  }

  private static MetadataChangeLog mcl(
      Urn runUrn, String aspectName, RecordTemplate aspect, @Nullable RecordTemplate previous) {
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityType(EXECUTION_REQUEST_ENTITY_NAME)
            .setEntityUrn(runUrn)
            .setAspectName(aspectName)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(aspect))
            .setCreated(
                new AuditStamp().setTime(42L).setActor(UrnUtils.getUrn("urn:li:corpuser:a")));
    if (previous != null) {
      event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(previous));
    }
    return event;
  }

  private static ExecutionRequestInput input(Urn sourceUrn, long requestedAt) {
    return new ExecutionRequestInput()
        .setTask("RUN_INGEST")
        .setExecutorId("default")
        .setRequestedAt(requestedAt)
        .setArgs(new StringMap())
        .setSource(
            new ExecutionRequestSource()
                .setType("SCHEDULED_INGESTION_SOURCE")
                .setIngestionSource(sourceUrn));
  }

  private static DataHubIngestionSourceInfo sourceInfo() {
    return new DataHubIngestionSourceInfo()
        .setName("source")
        .setType("mysql")
        .setConfig(new DataHubIngestionSourceConfig().setRecipe("{}"));
  }

  private static ExecutionRequestResult result(String status) {
    return new ExecutionRequestResult().setStatus(status);
  }

  private static EntityResponse response(String aspectName, RecordTemplate aspect) {
    return new EntityResponse()
        .setAspects(
            new EnvelopedAspectMap(
                Map.of(aspectName, new EnvelopedAspect().setValue(new Aspect(aspect.data())))));
  }
}
