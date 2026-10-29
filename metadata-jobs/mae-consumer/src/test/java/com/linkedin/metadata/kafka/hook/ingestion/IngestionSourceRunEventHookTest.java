package com.linkedin.metadata.kafka.hook.ingestion;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

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
import com.linkedin.ingestion.DataHubIngestionSourceRunEvent;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LatestIngestionRuns;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
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
  private static final Urn NEWER_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:newer");

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
    inputs.put(RUN, input());
    latestRequest(RUN);
    when(client.batchGetV2(any(OperationContext.class), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              Map<Urn, EntityResponse> responses = new HashMap<>();
              for (Urn urn : urns) {
                if (sources.contains(urn)) {
                  responses.put(
                      urn,
                      response(
                          INGESTION_INFO_ASPECT_NAME, new ExecutionRequestResult().setStatus("x")));
                } else if (inputs.containsKey(urn)) {
                  responses.put(
                      urn, response(EXECUTION_REQUEST_INPUT_ASPECT_NAME, inputs.get(urn)));
                }
              }
              return responses;
            });
  }

  @Test
  public void testNewRequestRecordsPendingEvent() throws Exception {
    hook.invoke(opContext, mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(), null));

    DataHubIngestionSourceRunEvent event = emitted();
    assertEquals(event.getStatus(), "PENDING");
    assertEquals(event.getExecutionRequest(), RUN);
    assertEquals(event.getRequestedAtMillis(), 100L);
    assertEquals(event.getTimestampMillis(), 42L);
  }

  @Test
  public void testStatusChangeRecordsEvent() throws Exception {
    hook.invoke(
        opContext,
        mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("SUCCESS"), result("RUNNING")));

    DataHubIngestionSourceRunEvent event = emitted();
    assertEquals(event.getStatus(), "SUCCESS");
    assertEquals(event.getSuccessfulRunRequestedAtMillis(), Long.valueOf(100L));
  }

  @Test
  public void testHeartbeatIsSkippedWithoutReads() throws Exception {
    hook.invokeBatch(
        opContext,
        List.of(
            mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("RUNNING"), result("RUNNING")),
            mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(), input())));

    verify(client, never()).batchGetV2(any(OperationContext.class), any(), any());
    verify(entitySearchService, never()).getLatestIngestionRuns(any(), any());
    verify(client, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testBatchSharesReads() throws Exception {
    Urn otherRun = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:other");
    inputs.put(otherRun, input().setRequestedAt(50L));

    hook.invokeBatch(
        opContext,
        List.of(
            mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("SUCCESS"), result("RUNNING")),
            mcl(
                otherRun,
                EXECUTION_REQUEST_RESULT_ASPECT_NAME,
                result("FAILURE"),
                result("RUNNING"))));

    // One read for the results' inputs and one for the source; the newest request is in the batch.
    verify(client, times(2)).batchGetV2(any(OperationContext.class), any(), any());
    verify(entitySearchService, times(1)).getLatestIngestionRuns(any(), eq(Set.of(SOURCE)));
    verify(client, times(1))
        .getTimeseriesAspectValues(any(), any(), any(), any(), any(), any(), any(), any());
    // The older run is checked against the event just written for the newer one.
    assertEquals(emitted().getExecutionRequest(), RUN);
  }

  @Test
  public void testOlderRunFinishingLateIsSkipped() throws Exception {
    latestRequest(NEWER_RUN);
    inputs.put(NEWER_RUN, input().setRequestedAt(200L));

    hook.invoke(
        opContext,
        mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("SUCCESS"), result("RUNNING")));

    verify(client, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testNewRequestNotYetIndexedIsRecorded() throws Exception {
    // A request's input MCL can arrive before the request is searchable, so the newest indexed
    // request is still the previous run.
    latestRequest(NEWER_RUN);
    inputs.put(NEWER_RUN, input().setRequestedAt(50L));

    hook.invoke(opContext, mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(), null));

    assertEquals(emitted().getStatus(), "PENDING");
  }

  @Test
  public void testRequestOlderThanTheLatestEventIsSkipped() throws Exception {
    latestEvent(event(200L, null));

    hook.invoke(opContext, mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(), null));

    verify(client, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testLastSuccessCarriesForwardOntoAFailure() throws Exception {
    latestEvent(event(80L, 80L));

    hook.invoke(
        opContext,
        mcl(RUN, EXECUTION_REQUEST_RESULT_ASPECT_NAME, result("FAILURE"), result("RUNNING")));

    DataHubIngestionSourceRunEvent event = emitted();
    assertEquals(event.getSuccessfulRunRequestedAtMillis(), Long.valueOf(80L));
    assertEquals(event.getFailedRunRequestedAtMillis(), Long.valueOf(100L));
  }

  @Test
  public void testDeletedSourceIsSkipped() throws Exception {
    sources.clear();

    hook.invoke(opContext, mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(), null));

    verify(client, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testRestateIsIgnored() throws Exception {
    MetadataChangeLog restate = mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(), null);
    restate.setChangeType(ChangeType.RESTATE);

    hook.invoke(opContext, restate);

    verify(client, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testFailedWriteIsCountedAndTheBatchContinues() throws Exception {
    Urn otherSource = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-b");
    Urn otherRun = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:other");
    sources.add(otherSource);
    inputs.put(otherRun, input(otherSource));
    MetricUtils metricUtils = mock(MetricUtils.class);
    OperationContext withMetrics = mock(OperationContext.class);
    when(withMetrics.getMetricUtils()).thenReturn(Optional.of(metricUtils));
    when(client.ingestProposal(eq(withMetrics), any(), anyBoolean()))
        .thenThrow(new RemoteInvocationException("down"))
        .thenReturn("ok");

    hook.invokeBatch(
        withMetrics,
        List.of(
            mcl(RUN, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(), null),
            mcl(otherRun, EXECUTION_REQUEST_INPUT_ASPECT_NAME, input(otherSource), null)));

    verify(client, times(2)).ingestProposal(eq(withMetrics), any(), anyBoolean());
    verify(metricUtils).increment(eq(IngestionSourceRunEventHook.class), any(String.class), eq(1d));
  }

  private DataHubIngestionSourceRunEvent emitted() throws Exception {
    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(client).ingestProposal(any(), captor.capture(), eq(false));
    MetadataChangeProposal proposal = captor.getValue();
    assertEquals(proposal.getEntityUrn(), SOURCE);
    assertEquals(proposal.getAspectName(), INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME);
    return GenericRecordUtils.deserializeAspect(
        proposal.getAspect().getValue(),
        proposal.getAspect().getContentType(),
        DataHubIngestionSourceRunEvent.class);
  }

  private void latestRequest(Urn runUrn) {
    when(entitySearchService.getLatestIngestionRuns(any(), any()))
        .thenReturn(Map.of(SOURCE, new LatestIngestionRuns(runUrn, null, null)));
  }

  private void latestEvent(DataHubIngestionSourceRunEvent event) throws Exception {
    when(client.getTimeseriesAspectValues(
            any(),
            eq(SOURCE.toString()),
            eq(INGESTION_SOURCE_ENTITY_NAME),
            eq(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME),
            any(),
            any(),
            any(),
            any()))
        .thenReturn(
            List.of(
                new com.linkedin.metadata.aspect.EnvelopedAspect()
                    .setAspect(GenericRecordUtils.serializeAspect(event))));
  }

  private static DataHubIngestionSourceRunEvent event(
      long requestedAt, @Nullable Long successfulRequestedAt) {
    DataHubIngestionSourceRunEvent event =
        new DataHubIngestionSourceRunEvent()
            .setTimestampMillis(1L)
            .setExecutionRequest(NEWER_RUN)
            .setRequestedAtMillis(requestedAt)
            .setStatus("SUCCESS");
    if (successfulRequestedAt != null) {
      event.setSuccessfulRunRequestedAtMillis(successfulRequestedAt);
    }
    return event;
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

  private static ExecutionRequestInput input() {
    return input(SOURCE);
  }

  private static ExecutionRequestInput input(Urn sourceUrn) {
    return new ExecutionRequestInput()
        .setTask("RUN_INGEST")
        .setExecutorId("default")
        .setRequestedAt(100L)
        .setArgs(new StringMap())
        .setSource(
            new ExecutionRequestSource()
                .setType("SCHEDULED_INGESTION_SOURCE")
                .setIngestionSource(sourceUrn));
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
