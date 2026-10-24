package com.linkedin.datahub.upgrade.system.ingestion;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BackfillIngestionSourceRunSummaryStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final Urn SOURCE_A = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-a");
  private static final Urn SOURCE_B = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-b");
  private static final Urn RUN_A = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-a");
  private static final Urn RUN_B = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-b");
  private static final Urn NEWER_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-new");

  private final Map<Urn, Urn> latestRunBySource = new HashMap<>();
  private final Map<Urn, Map<String, RecordTemplate>> store = new HashMap<>();
  private EntityService<?> entityService;
  private SearchService searchService;
  private EntitySearchService entitySearchService;
  private UpgradeContext context;

  @BeforeMethod
  public void setup() throws Exception {
    latestRunBySource.clear();
    store.clear();
    entityService = mock(EntityService.class);
    searchService = mock(SearchService.class);
    entitySearchService = mock(EntitySearchService.class);
    context = mock(UpgradeContext.class);
    when(context.opContext()).thenReturn(OP_CONTEXT);

    when(entitySearchService.filter(
            any(OperationContext.class),
            eq(EXECUTION_REQUEST_ENTITY_NAME),
            any(Filter.class),
            any(),
            eq(0),
            eq(1)))
        .thenAnswer(
            invocation -> {
              Filter filter = invocation.getArgument(2);
              Urn source =
                  UrnUtils.getUrn(filter.getOr().get(0).getAnd().get(0).getValues().get(0));
              SearchEntityArray runs = new SearchEntityArray();
              if (latestRunBySource.containsKey(source)) {
                runs.add(new SearchEntity().setEntity(latestRunBySource.get(source)));
              }
              return new SearchResult().setEntities(runs);
            });
    when(entityService.getEntitiesV2(any(OperationContext.class), anyString(), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(2);
              Map<Urn, EntityResponse> responses = new HashMap<>();
              urns.stream()
                  .filter(store::containsKey)
                  .forEach(urn -> responses.put(urn, response(urn, store.get(urn))));
              return responses;
            });
  }

  @Test
  public void testWritesSummaryFromLatestRun() {
    source(SOURCE_A, RUN_A, 200, "SUCCESS");
    stubScroll(page(null, SOURCE_A));

    assertEquals(run(), DataHubUpgradeState.SUCCEEDED);

    DataHubIngestionSourceRunSummary summary = summaryOf(capturedItems(1).get(0));
    assertEquals(summary.getLastExecutionRequest(), RUN_A);
    assertEquals(summary.getLastRunRequestedAtMillis(), 200L);
    assertEquals(summary.getLastRunStatus(), "SUCCESS");
    verifyMarkerWritten(true);
  }

  @Test
  public void testNewerSummaryIsNotOverwritten() {
    source(SOURCE_A, RUN_A, 200, "SUCCESS");
    put(
        SOURCE_A,
        INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME,
        new DataHubIngestionSourceRunSummary()
            .setLastExecutionRequest(NEWER_RUN)
            .setLastRunRequestedAtMillis(300));
    stubScroll(page(null, SOURCE_A));

    assertEquals(run(), DataHubUpgradeState.SUCCEEDED);

    verifyNoSummaryWrites();
  }

  @Test
  public void testDeletedSourceIsSkipped() {
    source(SOURCE_A, RUN_A, 200, "SUCCESS");
    store.get(SOURCE_A).remove(INGESTION_INFO_ASPECT_NAME);
    stubScroll(page(null, SOURCE_A));

    assertEquals(run(), DataHubUpgradeState.SUCCEEDED);

    verifyNoSummaryWrites();
  }

  @Test
  public void testFollowsScrollIdUntilExhausted() {
    source(SOURCE_A, RUN_A, 200, "SUCCESS");
    source(SOURCE_B, RUN_B, 100, "FAILURE");
    stubScroll(page("next", SOURCE_A), page(null, SOURCE_B));

    assertEquals(run(), DataHubUpgradeState.SUCCEEDED);

    assertEquals(
        capturedItems(2).stream().map(MCPItem::getUrn).toList(), List.of(SOURCE_A, SOURCE_B));
  }

  @Test
  public void testFailedBatchFailsWithoutMarker() {
    source(SOURCE_A, RUN_A, 200, "SUCCESS");
    source(SOURCE_B, RUN_B, 100, "FAILURE");
    stubScroll(page("next", SOURCE_A), page(null, SOURCE_B));
    doThrow(new RuntimeException("database unavailable"))
        .doReturn(List.of())
        .when(entityService)
        .ingestProposal(any(OperationContext.class), any(AspectsBatch.class), anyBoolean());

    assertEquals(run(), DataHubUpgradeState.FAILED);

    capturedItems(2);
    verifyMarkerWritten(false);
  }

  @Test
  public void testSkipRespectsMarkerAndReprocess() {
    when(entityService.exists(
            any(OperationContext.class),
            any(Urn.class),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(true);

    assertTrue(step(false).skip(context));
    assertFalse(step(true).skip(context));
  }

  @Test
  public void testDoesNotSkipWithoutMarker() {
    assertFalse(step(false).skip(context));
  }

  private BackfillIngestionSourceRunSummaryStep step(boolean reprocessEnabled) {
    return new BackfillIngestionSourceRunSummaryStep(
        OP_CONTEXT, entityService, searchService, entitySearchService, reprocessEnabled, 500);
  }

  private DataHubUpgradeState run() {
    UpgradeStepResult result = step(false).executable().apply(context);
    return result.result();
  }

  private void source(Urn sourceUrn, Urn runUrn, long requestedAt, String status) {
    latestRunBySource.put(sourceUrn, runUrn);
    put(
        sourceUrn,
        INGESTION_INFO_ASPECT_NAME,
        new DataHubIngestionSourceInfo()
            .setName(sourceUrn.getId())
            .setType("mysql")
            .setConfig(new DataHubIngestionSourceConfig().setRecipe("{}")));
    put(
        runUrn,
        EXECUTION_REQUEST_INPUT_ASPECT_NAME,
        new ExecutionRequestInput()
            .setTask("RUN_INGEST")
            .setArgs(new StringMap())
            .setExecutorId("default")
            .setRequestedAt(requestedAt)
            .setSource(
                new ExecutionRequestSource()
                    .setType("INGESTION_SOURCE")
                    .setIngestionSource(sourceUrn)));
    put(
        runUrn,
        EXECUTION_REQUEST_RESULT_ASPECT_NAME,
        new ExecutionRequestResult().setStatus(status));
  }

  private void put(Urn urn, String aspectName, RecordTemplate aspect) {
    store.computeIfAbsent(urn, u -> new HashMap<>()).put(aspectName, aspect);
  }

  private static EntityResponse response(Urn urn, Map<String, RecordTemplate> aspects) {
    EnvelopedAspectMap envelopedAspects = new EnvelopedAspectMap();
    aspects.forEach(
        (name, aspect) ->
            envelopedAspects.put(
                name, new EnvelopedAspect().setName(name).setValue(new Aspect(aspect.data()))));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(
            urn.getEntityType().equals(EXECUTION_REQUEST_ENTITY_NAME)
                ? EXECUTION_REQUEST_ENTITY_NAME
                : INGESTION_SOURCE_ENTITY_NAME)
        .setAspects(envelopedAspects);
  }

  private static ScrollResult page(String scrollId, Urn... urns) {
    SearchEntityArray entities = new SearchEntityArray();
    Arrays.stream(urns).forEach(urn -> entities.add(new SearchEntity().setEntity(urn)));
    ScrollResult result = mock(ScrollResult.class);
    when(result.getEntities()).thenReturn(entities);
    when(result.getScrollId()).thenReturn(scrollId);
    return result;
  }

  private void stubScroll(ScrollResult first, ScrollResult... rest) {
    when(searchService.scrollAcrossEntities(
            any(OperationContext.class), any(), anyString(), any(), any(), any(), any(), anyInt()))
        .thenReturn(first, rest);
  }

  private List<MCPItem> capturedItems(int expectedCount) {
    ArgumentCaptor<AspectsBatch> captor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(entityService, atLeastOnce())
        .ingestProposal(any(OperationContext.class), captor.capture(), eq(false));
    List<MCPItem> items =
        captor.getAllValues().stream().flatMap(batch -> batch.getMCPItems().stream()).toList();
    assertEquals(items.size(), expectedCount);
    return items;
  }

  private void verifyNoSummaryWrites() {
    verify(entityService, never())
        .ingestProposal(any(OperationContext.class), any(AspectsBatch.class), anyBoolean());
  }

  private void verifyMarkerWritten(boolean written) {
    verify(entityService, written ? atLeastOnce() : never())
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            anyBoolean());
  }

  private static DataHubIngestionSourceRunSummary summaryOf(MCPItem item) {
    return RecordUtils.toRecordTemplate(
        DataHubIngestionSourceRunSummary.class,
        item.getMetadataChangeProposal().getAspect().getValue().asString(StandardCharsets.UTF_8));
  }
}
