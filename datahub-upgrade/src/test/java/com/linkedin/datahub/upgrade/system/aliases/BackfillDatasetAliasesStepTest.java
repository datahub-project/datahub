package com.linkedin.datahub.upgrade.system.aliases;

import static com.linkedin.metadata.Constants.ALIASES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
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
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
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
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

  private List<MetadataChangeProposal> capturedProposals(int expectedCount) {
    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService, times(expectedCount))
        .ingestProposal(eq(OP_CONTEXT), captor.capture(), any(), eq(true));
    return captor.getAllValues();
  }

  private static Aliases aliasesOf(MetadataChangeProposal proposal) {
    return RecordUtils.toRecordTemplate(
        Aliases.class, proposal.getAspect().getValue().asString(StandardCharsets.UTF_8));
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
    List<MetadataChangeProposal> proposals = capturedProposals(2);
    MetadataChangeProposal first = proposals.get(0);
    assertEquals(first.getEntityUrn(), UrnUtils.getUrn(URN_MIXED_CASE));
    assertEquals(first.getEntityType(), DATASET_ENTITY_NAME);
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

    buildStep(false).executable().apply(mockContext);

    capturedProposals(2);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
    verify(mockUpgrade)
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            captor.capture());
    // the marker carries the run counts so they outlive the job and can be alerted on
    assertEquals(captor.getValue().get("emitted"), "2");
    assertEquals(captor.getValue().get("unparseable"), "0");
  }

  @Test
  public void testEmptyScanStillMarksComplete() {
    stubScroll(page(null));

    buildStep(false).executable().apply(mockContext);

    verify(mockEntityService, never())
        .ingestProposal(any(OperationContext.class), any(), any(), eq(true));
    verify(mockUpgrade)
        .setUpgradeResult(
            any(OperationContext.class),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.SUCCEEDED),
            any());
  }

  @Test
  public void testEmitFailureStopsTheScanWithoutMarker() {
    stubScroll(page(null, URN_MIXED_CASE, URN_OTHER));
    doThrow(new RuntimeException("kafka down"))
        .when(mockEntityService)
        .ingestProposal(
            eq(OP_CONTEXT),
            argThat(p -> UrnUtils.getUrn(URN_MIXED_CASE).equals(p.getEntityUrn())),
            any(),
            eq(true));

    expectThrows(RuntimeException.class, () -> buildStep(false).executable().apply(mockContext));

    // the urn after the failure is never attempted
    verify(mockEntityService, never())
        .ingestProposal(
            eq(OP_CONTEXT),
            argThat(p -> UrnUtils.getUrn(URN_OTHER).equals(p.getEntityUrn())),
            any(),
            eq(true));
    verify(mockUpgrade, never())
        .setUpgradeResult(any(OperationContext.class), any(Urn.class), any(), any(), any());
  }

  @Test
  public void testUrnThatCannotBeLowercasedIsSkipped() {
    stubScroll(page(null, URN_NOT_A_DATASET, URN_MIXED_CASE));

    UpgradeStepResult result = buildStep(false).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    List<MetadataChangeProposal> proposals = capturedProposals(1);
    assertEquals(proposals.get(0).getEntityUrn(), UrnUtils.getUrn(URN_MIXED_CASE));
  }

  @Test
  public void testSkipsWhenAlreadyRun() {
    when(mockEntityService.exists(
            any(OperationContext.class), any(Urn.class), anyString(), anyBoolean()))
        .thenReturn(true);

    assertTrue(buildStep(false).skip(mockContext));
    assertFalse(buildStep(true).skip(mockContext), "reprocess must override a previous run");
  }

  @Test
  public void testDoesNotSkipWithoutMarker() {
    assertFalse(buildStep(false).skip(mockContext));
  }
}
