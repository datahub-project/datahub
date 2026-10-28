package com.linkedin.datahub.upgrade.system.ingestion;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
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
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceRunEvent;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LatestIngestionRuns;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
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

public class BackfillIngestionSourceRunEventsStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final Urn SOURCE_A = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-a");
  private static final Urn SOURCE_B = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-b");
  private static final Urn SUCCESS_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:ok");
  private static final Urn FAILED_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:failed");
  private static final Urn LATEST_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:latest");
  private static final Urn RUN_B = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-b");

  private final Map<Urn, LatestIngestionRuns> latestRuns = new HashMap<>();
  private final Map<Urn, Map<String, RecordTemplate>> store = new HashMap<>();
  private EntityService<?> entityService;
  private SearchService searchService;
  private EntitySearchService entitySearchService;
  private UpgradeContext context;

  @BeforeMethod
  public void setup() throws Exception {
    latestRuns.clear();
    store.clear();
    entityService = mock(EntityService.class);
    searchService = mock(SearchService.class);
    entitySearchService = mock(EntitySearchService.class);
    context = mock(UpgradeContext.class);
    when(context.opContext()).thenReturn(OP_CONTEXT);
    when(entitySearchService.getLatestIngestionRuns(any(OperationContext.class), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> sources = invocation.getArgument(1);
              Map<Urn, LatestIngestionRuns> result = new HashMap<>();
              sources.stream()
                  .filter(latestRuns::containsKey)
                  .forEach(source -> result.put(source, latestRuns.get(source)));
              return result;
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
  public void testOneEventPerSourceCarriesLastSuccessAndFailure() {
    // The index keeps only the newest event per source in a batch, so the latest run's event
    // must carry the last success and failure times itself.
    source(SOURCE_A);
    run(SUCCESS_RUN, SOURCE_A, 100, "SUCCESS");
    run(FAILED_RUN, SOURCE_A, 200, "FAILURE");
    run(LATEST_RUN, SOURCE_A, 300, "RUNNING");
    latestRuns.put(SOURCE_A, new LatestIngestionRuns(LATEST_RUN, SUCCESS_RUN, FAILED_RUN));
    stubScroll(page(null, SOURCE_A));

    assertEquals(execute(), DataHubUpgradeState.SUCCEEDED);

    DataHubIngestionSourceRunEvent event = eventOf(capturedItems(1).get(0));
    assertEquals(event.getExecutionRequest(), LATEST_RUN);
    assertEquals(event.getStatus(), "RUNNING");
    assertEquals(event.getSuccessfulRunRequestedAtMillis(), Long.valueOf(100));
    assertEquals(event.getFailedRunRequestedAtMillis(), Long.valueOf(200));
    verifyMarkerWritten(true);
  }

  @Test
  public void testOnlySourcesWithoutARecordedRunAreScrolled() {
    // A source the hook has already recorded is current; backfilling it from search could
    // replace a newer run with an older one.
    stubScroll(page(null));

    assertEquals(execute(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<Filter> filter = ArgumentCaptor.forClass(Filter.class);
    verify(searchService)
        .scrollAcrossEntities(
            any(OperationContext.class),
            any(),
            anyString(),
            filter.capture(),
            any(),
            any(),
            any(),
            anyInt());
    Criterion criterion = filter.getValue().getOr().get(0).getAnd().get(0);
    assertEquals(criterion.getField(), "lastRunRequestedAtMillis");
    assertEquals(criterion.getCondition(), Condition.EXISTS);
    assertTrue(criterion.isNegated());
  }

  @Test
  public void testLatestRunThatSucceededIsRecordedOnce() {
    source(SOURCE_A);
    run(LATEST_RUN, SOURCE_A, 300, "SUCCESS");
    latestRuns.put(SOURCE_A, new LatestIngestionRuns(LATEST_RUN, LATEST_RUN, null));
    stubScroll(page(null, SOURCE_A));

    assertEquals(execute(), DataHubUpgradeState.SUCCEEDED);

    assertEquals(eventOf(capturedItems(1).get(0)).getStatus(), "SUCCESS");
  }

  @Test
  public void testDeletedSourceIsSkipped() {
    run(LATEST_RUN, SOURCE_A, 300, "SUCCESS");
    latestRuns.put(SOURCE_A, new LatestIngestionRuns(LATEST_RUN, null, null));
    stubScroll(page(null, SOURCE_A));

    assertEquals(execute(), DataHubUpgradeState.SUCCEEDED);

    verify(entityService, never())
        .ingestProposal(any(OperationContext.class), any(AspectsBatch.class), anyBoolean());
  }

  @Test
  public void testScrollsByUrnUntilExhausted() {
    source(SOURCE_A);
    source(SOURCE_B);
    run(LATEST_RUN, SOURCE_A, 300, "SUCCESS");
    run(RUN_B, SOURCE_B, 100, "FAILURE");
    latestRuns.put(SOURCE_A, new LatestIngestionRuns(LATEST_RUN, null, null));
    latestRuns.put(SOURCE_B, new LatestIngestionRuns(RUN_B, null, null));
    stubScroll(page("next", SOURCE_A), page(null, SOURCE_B));

    assertEquals(execute(), DataHubUpgradeState.SUCCEEDED);

    assertEquals(
        capturedItems(2).stream().map(MCPItem::getUrn).toList(), List.of(SOURCE_A, SOURCE_B));
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<SortCriterion>> sort = ArgumentCaptor.forClass(List.class);
    verify(searchService, atLeastOnce())
        .scrollAcrossEntities(
            any(OperationContext.class),
            any(),
            anyString(),
            any(),
            sort.capture(),
            any(),
            any(),
            anyInt());
    assertEquals(
        sort.getValue(),
        List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)));
  }

  @Test
  public void testFailedLookupFailsWithoutMarker() {
    source(SOURCE_A);
    stubScroll(page(null, SOURCE_A));
    when(entitySearchService.getLatestIngestionRuns(any(OperationContext.class), any()))
        .thenThrow(new RuntimeException("search unavailable"));

    assertEquals(execute(), DataHubUpgradeState.FAILED);

    verifyMarkerWritten(false);
  }

  @Test
  public void testReprocessRewritesRecordedSources() {
    stubScroll(page(null));

    assertEquals(step(true).executable().apply(context).result(), DataHubUpgradeState.SUCCEEDED);

    verify(searchService)
        .scrollAcrossEntities(
            any(OperationContext.class),
            any(),
            anyString(),
            isNull(),
            any(),
            any(),
            any(),
            anyInt());
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

  private BackfillIngestionSourceRunEventsStep step(boolean reprocessEnabled) {
    return new BackfillIngestionSourceRunEventsStep(
        OP_CONTEXT, entityService, searchService, entitySearchService, reprocessEnabled, 500);
  }

  private DataHubUpgradeState execute() {
    return step(false).executable().apply(context).result();
  }

  private void source(Urn sourceUrn) {
    put(
        sourceUrn,
        INGESTION_INFO_ASPECT_NAME,
        new DataHubIngestionSourceInfo()
            .setName(sourceUrn.getId())
            .setType("mysql")
            .setConfig(new DataHubIngestionSourceConfig().setRecipe("{}")));
  }

  private void run(Urn runUrn, Urn sourceUrn, long requestedAt, String status) {
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
    items.forEach(
        item -> assertEquals(item.getAspectName(), INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME));
    return items;
  }

  private void verifyMarkerWritten(boolean written) {
    verify(entityService, written ? atLeastOnce() : never())
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            anyBoolean());
  }

  private static DataHubIngestionSourceRunEvent eventOf(MCPItem item) {
    return RecordUtils.toRecordTemplate(
        DataHubIngestionSourceRunEvent.class,
        item.getMetadataChangeProposal().getAspect().getValue().asString(StandardCharsets.UTF_8));
  }
}
