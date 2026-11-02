package com.linkedin.datahub.upgrade.system.aliases;

import static com.linkedin.metadata.Constants.ALIASES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Aliases;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BackfillDatasetAliasesStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  // The rule lowercases the dataset name only; platform and env casing are preserved.
  private static final String URN_MIXED_CASE =
      "urn:li:dataset:(urn:li:dataPlatform:MySQL,db.AAA,PROD)";
  private static final String URN_MIXED_CASE_LOWERED =
      "urn:li:dataset:(urn:li:dataPlatform:MySQL,db.aaa,PROD)";
  private static final String URN_OTHER =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.BBB,PROD)";
  // Uppercase platform and env are deliberate: only the name is lowercased.
  private static final String URN_ALREADY_LOWERCASED =
      "urn:li:dataset:(urn:li:dataPlatform:MySQL,db.ccc,PROD)";
  // Parses as a generic urn but not as a DatasetUrn: the key tuple is missing the env.
  private static final String URN_NOT_A_DATASET = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.t)";

  private EntityService<?> mockEntityService;
  private SearchService mockSearchService;
  private Upgrade mockUpgrade;
  private UpgradeContext mockContext;

  @BeforeMethod
  public void setup() {
    mockEntityService = mock(EntityService.class);
    mockSearchService = mock(SearchService.class);
    mockUpgrade = mock(Upgrade.class);
    mockContext = mock(UpgradeContext.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mock(UpgradeReport.class));
    when(mockContext.opContext()).thenReturn(OP_CONTEXT);
  }

  private BackfillDatasetAliasesStep buildStep(boolean reprocessEnabled) {
    return new BackfillDatasetAliasesStep(
        OP_CONTEXT, mockEntityService, mockSearchService, 1000, 0, reprocessEnabled);
  }

  private void stubPreviousResult(DataHubUpgradeState state, String lastUrn) {
    DataHubUpgradeResult previous = new DataHubUpgradeResult().setState(state);
    if (lastUrn != null) {
      previous.setResult(new StringMap(Map.of("lastUrn", lastUrn)));
    }
    when(mockUpgrade.getUpgradeResult(any(OperationContext.class), any(Urn.class), any()))
        .thenReturn(Optional.of(previous));
  }

  private static ScrollResult page(String scrollId, String... urns) {
    SearchEntityArray entities = new SearchEntityArray();
    Arrays.stream(urns)
        .forEach(urn -> entities.add(new SearchEntity().setEntity(UrnUtils.getUrn(urn))));
    ScrollResult result = mock(ScrollResult.class);
    when(result.getEntities()).thenReturn(entities);
    when(result.getScrollId()).thenReturn(scrollId);
    return result;
  }

  private void stubScroll(ScrollResult first, ScrollResult... rest) {
    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class), any(), anyString(), any(), any(), any(), any(), anyInt()))
        .thenReturn(first, rest);
  }

  /**
   * Every page is emitted as one batch, so the proposals are read back off the captured batches.
   */
  private List<MCPItem> capturedProposals(int expectedCount) {
    ArgumentCaptor<AspectsBatch> captor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(mockEntityService, atLeastOnce())
        .ingestProposal(eq(OP_CONTEXT), captor.capture(), eq(true));
    List<MCPItem> items =
        captor.getAllValues().stream()
            .flatMap(batch -> batch.getMCPItems().stream())
            .collect(Collectors.toList());
    assertEquals(items.size(), expectedCount);
    return items;
  }

  private static Aliases aliasesOf(MCPItem item) {
    return RecordUtils.toRecordTemplate(
        Aliases.class,
        item.getMetadataChangeProposal().getAspect().getValue().asString(StandardCharsets.UTF_8));
  }

  /** The result map written with `state`, which carries the run counters and the resume cursor. */
  private Map<String, String> capturedResult(DataHubUpgradeState state) {
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
    verify(mockUpgrade)
        .setUpgradeResult(
            any(OperationContext.class), any(Urn.class), any(), eq(state), captor.capture());
    return captor.getValue();
  }

  private Filter capturedFilter() {
    ArgumentCaptor<Filter> captor = ArgumentCaptor.forClass(Filter.class);
    verify(mockSearchService)
        .scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of(DATASET_ENTITY_NAME)),
            anyString(),
            captor.capture(),
            any(),
            any(),
            any(),
            anyInt());
    return captor.getValue();
  }

  @Test
  public void testEmitsLowercasedAliasPerHit() {
    stubScroll(page(null, URN_MIXED_CASE, URN_OTHER));

    UpgradeStepResult result = buildStep(false).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    MCPItem first = capturedProposals(2).get(0);
    assertEquals(first.getUrn(), UrnUtils.getUrn(URN_MIXED_CASE));
    assertEquals(first.getMetadataChangeProposal().getEntityType(), DATASET_ENTITY_NAME);
    assertEquals(first.getAspectName(), ALIASES_ASPECT_NAME);
    assertEquals(aliasesOf(first).getLowercasedUrn().toString(), URN_MIXED_CASE_LOWERED);
    assertEquals(first.getSystemMetadata().getRunId(), BackfillDatasetAliasesStep.UPGRADE_ID);
    assertEquals(first.getSystemMetadata().getProperties().get(APP_SOURCE), SYSTEM_UPDATE_SOURCE);
  }

  @Test
  public void testDefaultFilterSelectsMissingLowercasedUrn() {
    stubScroll(page(null, URN_MIXED_CASE));

    buildStep(false).executable().apply(mockContext);

    Criterion criterion = capturedFilter().getOr().get(0).getAnd().get(0);
    assertEquals(criterion.getField(), "lowercasedUrn");
    assertEquals(criterion.getCondition(), Condition.IS_NULL);
    assertFalse(criterion.isNegated());
  }

  @Test
  public void testScansInUrnOrder() {
    stubScroll(page(null, URN_MIXED_CASE));

    buildStep(false).executable().apply(mockContext);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<SortCriterion>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockSearchService)
        .scrollAcrossEntities(
            any(OperationContext.class),
            any(),
            anyString(),
            any(),
            captor.capture(),
            any(),
            any(),
            anyInt());
    // _score is recomputed on write, so it cannot order a scan that runs alongside ingestion
    List<SortCriterion> sortCriteria = captor.getValue();
    assertEquals(sortCriteria.size(), 1);
    assertEquals(sortCriteria.get(0).getField(), "urn");
    assertEquals(sortCriteria.get(0).getOrder(), SortOrder.ASCENDING);
  }

  @Test
  public void testReprocessScansEveryDataset() {
    stubScroll(page(null, URN_MIXED_CASE));

    buildStep(true).executable().apply(mockContext);

    assertNull(capturedFilter(), "reprocess must not restrict the scan to missing aliases");
    capturedProposals(1);
  }

  @Test
  public void testHiddenDatasetsAreIncluded() {
    stubScroll(page(null, URN_MIXED_CASE));

    buildStep(false).executable().apply(mockContext);

    ArgumentCaptor<OperationContext> captor = ArgumentCaptor.forClass(OperationContext.class);
    verify(mockSearchService)
        .scrollAcrossEntities(
            captor.capture(), any(), anyString(), any(), any(), any(), any(), anyInt());
    SearchFlags flags = captor.getValue().getSearchContext().getSearchFlags();
    assertTrue(flags.isIncludeSoftDeleted());
    assertFalse(flags.isFilterNonLatestVersions());
    assertTrue(flags.isIncludeHiddenLifecycleStages());
  }

  @Test
  public void testFollowsScrollIdUntilExhausted() {
    stubScroll(page("next", URN_MIXED_CASE), page(null, URN_OTHER));

    UpgradeStepResult result = buildStep(false).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    capturedProposals(2);
  }

  @Test
  public void testEmptyScanStillMarksComplete() {
    stubScroll(page(null));

    buildStep(false).executable().apply(mockContext);

    verify(mockEntityService, never())
        .ingestProposal(any(OperationContext.class), any(AspectsBatch.class), eq(true));
    verify(mockUpgrade)
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            any());
  }

  @Test
  public void testEmptyPageWithLiveScrollIdContinuesTheScan() {
    // A page whose every hit failed urn parsing: no entities, but the scroll id still advances.
    // Treating that as exhaustion would abandon the rest of the population under a SUCCEEDED
    // marker.
    stubScroll(page("next"), page(null, URN_MIXED_CASE));

    UpgradeStepResult result = buildStep(false).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockSearchService, times(2))
        .scrollAcrossEntities(
            any(OperationContext.class), any(), anyString(), any(), any(), any(), any(), anyInt());
    assertEquals(capturedProposals(1).get(0).getUrn(), UrnUtils.getUrn(URN_MIXED_CASE));
  }

  @Test
  public void testEmitFailureStopsTheScanWithoutMarker() {
    stubScroll(page("next", URN_MIXED_CASE), page(null, URN_OTHER));
    doThrow(new RuntimeException("kafka down"))
        .when(mockEntityService)
        .ingestProposal(eq(OP_CONTEXT), any(AspectsBatch.class), eq(true));

    expectThrows(RuntimeException.class, () -> buildStep(false).executable().apply(mockContext));

    // the failing page ends the run: the next page is never fetched
    verify(mockSearchService, times(1))
        .scrollAcrossEntities(
            any(OperationContext.class), any(), anyString(), any(), any(), any(), any(), anyInt());
    verify(mockUpgrade, never())
        .setUpgradeResult(any(OperationContext.class), any(Urn.class), any(), any(), any());
  }

  @Test
  public void testUrnThatCannotBeLowercasedIsSkipped() {
    stubScroll(page(null, URN_NOT_A_DATASET, URN_MIXED_CASE));

    UpgradeStepResult result = buildStep(false).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(capturedProposals(1).get(0).getUrn(), UrnUtils.getUrn(URN_MIXED_CASE));
  }

  @Test
  public void testUrnAlreadyLowercasedIsSkipped() {
    stubScroll(page(null, URN_ALREADY_LOWERCASED, URN_MIXED_CASE));

    UpgradeStepResult result = buildStep(false).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(capturedProposals(1).get(0).getUrn(), UrnUtils.getUrn(URN_MIXED_CASE));
  }

  @Test
  public void testSkipsWhenAlreadyRun() {
    stubPreviousResult(DataHubUpgradeState.SUCCEEDED, null);

    assertTrue(buildStep(false).skip(mockContext));
    assertFalse(buildStep(true).skip(mockContext), "reprocess must override a previous run");
  }

  @Test
  public void testDoesNotSkipWithoutMarker() {
    assertFalse(buildStep(false).skip(mockContext));
  }

  @Test
  public void testDoesNotSkipAnInterruptedRun() {
    stubPreviousResult(DataHubUpgradeState.IN_PROGRESS, URN_MIXED_CASE);

    // the IN_PROGRESS result only carries the cursor; the scan still has pages left
    assertFalse(buildStep(false).skip(mockContext));
  }

  @Test
  public void testResumesAfterTheRecordedUrn() {
    stubPreviousResult(DataHubUpgradeState.IN_PROGRESS, URN_ALREADY_LOWERCASED);
    stubScroll(page(null, URN_MIXED_CASE));

    buildStep(false).executable().apply(mockContext);

    Criterion cursor = capturedFilter().getOr().get(0).getAnd().get(1);
    assertEquals(cursor.getField(), "urn");
    assertEquals(cursor.getCondition(), Condition.GREATER_THAN);
    assertEquals(cursor.getValues(), List.of(URN_ALREADY_LOWERCASED));
  }

  @Test
  public void testReprocessIgnoresTheCursor() {
    stubPreviousResult(DataHubUpgradeState.IN_PROGRESS, URN_ALREADY_LOWERCASED);
    stubScroll(page(null, URN_MIXED_CASE));

    buildStep(true).executable().apply(mockContext);

    assertNull(capturedFilter(), "reprocess must scan from the start");
  }

  @Test
  public void testRecordsTheLastScannedUrnPerPage() {
    // the skipped urn sorts last, so a cursor tracking emits would rescan it
    stubScroll(page(null, URN_MIXED_CASE, URN_ALREADY_LOWERCASED));

    buildStep(false).executable().apply(mockContext);

    assertEquals(
        capturedResult(DataHubUpgradeState.IN_PROGRESS).get("lastUrn"), URN_ALREADY_LOWERCASED);
  }

  @Test
  public void testReprocessRecordsNoCursor() {
    // An ordinary run would read the cursor back and reapply the IS_NULL filter, skipping the
    // stale-valued datasets the interrupted reprocess never reached.
    stubScroll(page("next", URN_MIXED_CASE), page(null, URN_OTHER));

    buildStep(true).executable().apply(mockContext);

    verify(mockUpgrade, never())
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.IN_PROGRESS),
            any());
    capturedProposals(2);
  }
}
