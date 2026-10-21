package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SYSTEM_METADATA_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.AuthenticationException;
import com.datahub.authorization.AuthUtil;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.RollbackResponse;
import com.linkedin.metadata.run.UnsafeEntityInfo;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RollbackServiceTest {

  private EntityService<?> mockEntityService;
  private SystemMetadataService mockSystemMetadataService;
  private TimeseriesAspectService mockTimeseriesAspectService;
  private RollbackService rollbackService;
  private final OperationContext operationContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private static final String TEST_RUN_ID = "test-run-id";
  private static final String TEST_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,test-dataset-1,PROD)";
  private static final String TEST_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,test-dataset-2,PROD)";
  private static final int MAX_SEARCH_RESULTS = 5;

  @BeforeMethod
  public void setup() {
    mockEntityService = mock(EntityService.class);
    mockSystemMetadataService = mock(SystemMetadataService.class);
    mockTimeseriesAspectService = mock(TimeseriesAspectService.class);

    rollbackService = withoutPause(newRollbackService(null));
  }

  /** Multi-page runs would otherwise wait the real delay between pages. */
  private static RollbackService withoutPause(RollbackService service) {
    RollbackService spied = spy(service);
    doNothing().when(spied).pauseBetweenPages();
    return spied;
  }

  /** The first page of the run: read from the start, so no row to continue after. */
  private OngoingStubbing<List<AspectRowSummary>> whenFirstPage(boolean includeSoftDeleted) {
    return when(
        mockSystemMetadataService.findByRunIdAfter(
            any(OperationContext.class),
            eq(TEST_RUN_ID),
            eq(includeSoftDeleted),
            isNull(),
            eq(MAX_SEARCH_RESULTS)));
  }

  /** A later page: continues after the last row of the page before it. */
  private OngoingStubbing<List<AspectRowSummary>> whenPageAfter(
      boolean includeSoftDeleted, AspectRowSummary lastRowOfPreviousPage) {
    return when(
        mockSystemMetadataService.findByRunIdAfter(
            any(OperationContext.class),
            eq(TEST_RUN_ID),
            eq(includeSoftDeleted),
            same(lastRowOfPreviousPage),
            eq(MAX_SEARCH_RESULTS)));
  }

  private void verifyFirstPageRead(boolean includeSoftDeleted) {
    verify(mockSystemMetadataService)
        .findByRunIdAfter(
            any(OperationContext.class),
            eq(TEST_RUN_ID),
            eq(includeSoftDeleted),
            isNull(),
            eq(MAX_SEARCH_RESULTS));
  }

  private RollbackService newRollbackService(IngestionRollbackDispatcher dispatcher) {
    return new RollbackService(
        mockEntityService,
        mockSystemMetadataService,
        mockTimeseriesAspectService,
        TEST_SYSTEM_METADATA_SERVICE_CONFIG.toBuilder()
            .limit(
                TEST_SYSTEM_METADATA_SERVICE_CONFIG.getLimit().toBuilder()
                    .results(
                        ResultsLimitConfig.builder()
                            .max(MAX_SEARCH_RESULTS)
                            .apiDefault(MAX_SEARCH_RESULTS)
                            .build())
                    .build())
            .build(),
        dispatcher);
  }

  @Test
  public void testRollbackTargetAspects() {
    // Arrange
    List<AspectRowSummary> expectedAspects = createTestAspectRows(false);
    whenFirstPage(true).thenReturn(expectedAspects);

    // Act
    List<AspectRowSummary> result =
        rollbackService.rollbackTargetAspects(operationContext, TEST_RUN_ID, true);

    // Assert
    assertEquals(result, expectedAspects);
    verifyFirstPageRead(true);
  }

  @Test
  public void testRollbackIngestion_DryRun() throws AuthenticationException {
    // Arrange
    List<AspectRowSummary> testAspects = createTestAspectRows(true);
    whenFirstPage(false).thenReturn(testAspects);

    // For each urn in test aspects
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    // Act
    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            true, // dryRun
            false, // hardDelete
            null);

    // Assert
    verify(mockEntityService, never()).rollbackRun(any(), any(), any(), anyBoolean());
    assertEquals(response.getEntitiesDeleted(), 2);
    assertEquals(response.getEntitiesAffected(), 2);
    assertEquals(response.getAspectsReverted(), 2); // 4 total - 2 key aspects
  }

  @Test
  public void testRollbackIngestion_ActualRun() throws AuthenticationException {
    // Arrange
    List<AspectRowSummary> testAspects = createTestAspectRows(true);

    whenFirstPage(true)
        .thenReturn(testAspects)
        .thenReturn(new ArrayList<>()); // Second call returns empty to end the loop

    // Create RollbackResults for the detailed result tracking
    List<RollbackResult> rollbackResults = new ArrayList<>();

    // Create realistic RollbackResult instances
    Urn testUrn1 = UrnUtils.getUrn(TEST_URN_1);
    RollbackResult result1 =
        createRollbackResult(testUrn1, "dataset", "status", ChangeType.UPSERT, false, 0);

    Urn testUrn2 = UrnUtils.getUrn(TEST_URN_2);
    RollbackResult result2 =
        createRollbackResult(testUrn2, "dataset", "status", ChangeType.DELETE, true, 0);

    rollbackResults.add(result1);
    rollbackResults.add(result2);

    RollbackRunResult mockRollbackResult = new RollbackRunResult(testAspects, 0, rollbackResults);

    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(mockRollbackResult);

    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(5L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);

    // Mock empty lists for unsafe entities calculation
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    // Act
    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            true, // hardDelete
            null);

    // Assert
    verify(mockEntityService).rollbackRun(operationContext, testAspects, TEST_RUN_ID, true);
    verify(mockTimeseriesAspectService).rollbackTimeseriesAspects(operationContext, TEST_RUN_ID);

    assertEquals(response.getEntitiesAffected(), 2);
    assertEquals(response.getAspectsReverted(), 9); // 4 from aspects + 5 from timeseries

    // Verify the results were processed correctly
    assertNotNull(response.getAspectRowSummaries());
    assertTrue(response.getAspectRowSummaries().size() <= 100); // Check it respects the limit
  }

  @Test
  public void testRollbackIngestion_SoftDeleteExecuteExcludesKeyAspects()
      throws AuthenticationException {
    // Soft-delete execute: rollbackRun returns only the non-key rows (key aspects are not
    // reverted). aspectsReverted therefore equals the non-key row count; rowSummaries reflect
    // the targeted aspect rows (master execute does not strip key aspects from rowSummaries).
    List<AspectRowSummary> testAspects = createTestAspectRows(true);
    List<AspectRowSummary> nonKeyRows =
        testAspects.stream().filter(row -> !row.isKeyAspect()).collect(Collectors.toList());

    whenFirstPage(false).thenReturn(testAspects).thenReturn(new ArrayList<>());

    RollbackRunResult mockRollbackResult =
        new RollbackRunResult(nonKeyRows, 0, Collections.emptyList());
    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(false)))
        .thenReturn(mockRollbackResult);

    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(0L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            false, // hardDelete — soft delete
            null);

    assertEquals(response.getAspectsReverted(), 2); // non-key rows only
    assertEquals(response.getEntitiesDeleted(), 2); // key aspects targeted across all pages
    assertNotNull(response.getAspectRowSummaries());
  }

  @Test
  public void testRollbackIngestion_MultiplePages() throws AuthenticationException {
    // Arrange
    List<AspectRowSummary> firstPageAspects = createTestAspectRows(true);
    AspectRowSummary extraForPagination = new AspectRowSummary();
    extraForPagination.setUrn(TEST_URN_2);
    extraForPagination.setAspectName("datasetProperties");
    extraForPagination.setRunId(TEST_RUN_ID);
    extraForPagination.setKeyAspect(false);
    firstPageAspects.add(extraForPagination);

    List<AspectRowSummary> secondPageAspects = createMoreTestAspectRows();

    // The second page continues after the last row of the first; it is short, so the loop ends.
    whenFirstPage(true).thenReturn(firstPageAspects);
    whenPageAfter(true, firstPageAspects.get(firstPageAspects.size() - 1))
        .thenReturn(secondPageAspects);

    // Create rollback results for detailed tracking with proper RollbackResult objects
    List<RollbackResult> firstRollbackResults = new ArrayList<>();

    Urn urn1 = UrnUtils.getUrn(TEST_URN_1);
    firstRollbackResults.add(
        createRollbackResult(urn1, "dataset", "status", ChangeType.UPSERT, false, 0));

    Urn urn2 = UrnUtils.getUrn(TEST_URN_2);
    firstRollbackResults.add(
        createRollbackResult(urn2, "dataset", "status", ChangeType.DELETE, false, 0));

    List<RollbackResult> secondRollbackResults = new ArrayList<>();
    String testUrn3 = "urn:li:dataset:(urn:li:dataPlatform:hive,test-dataset-3,PROD)";
    Urn urn3 = UrnUtils.getUrn(testUrn3);
    secondRollbackResults.add(
        createRollbackResult(urn3, "dataset", "status", ChangeType.UPSERT, false, 0));

    RollbackRunResult firstRunResult =
        new RollbackRunResult(firstPageAspects, 0, firstRollbackResults);

    RollbackRunResult secondRunResult =
        new RollbackRunResult(secondPageAspects, 0, secondRollbackResults);

    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(firstRunResult) // First call
        .thenReturn(secondRunResult) // Second call
        .thenReturn(
            new RollbackRunResult(
                Collections.emptyList(), 0, Collections.emptyList())); // Third call

    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(0L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);

    // Mock empty lists for unsafe entities calculation
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    // Act
    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            true, // hardDelete
            null);

    // Assert
    verify(mockEntityService, times(2)).rollbackRun(any(), any(), eq(TEST_RUN_ID), eq(true));
    verify(mockTimeseriesAspectService).rollbackTimeseriesAspects(operationContext, TEST_RUN_ID);
    assertEquals(response.getEntitiesAffected(), 3);
    assertEquals(response.getAspectsReverted(), 8); // 5 + 3 from the two batches

    // Verify we processed all rollback results
    assertEquals(firstRollbackResults.size() + secondRollbackResults.size(), 3);
  }

  @Test
  public void testRollbackIngestion_MultiPageUnsafeEntitiesCoverAllPages()
      throws AuthenticationException {
    // Prove affected/unsafe entity math spans every page, not just the last. First page carries
    // key aspects for urn1 and urn2; second page carries a key aspect for urn3. The while-loop
    // exits when the second page (3 rows) drops below apiDefault (5), so the last
    // aspectRowsToDelete is secondPageAspects — a last-page-only keyAspects derivation would find
    // urn3's key only and undercount to unsafeEntitiesCount=1, missing urn1/urn2. allKeyAspects
    // must accumulate across pages to cover all three.
    // A hard rollback only looks at entities it kept, so every key row is left out of
    // rowsRolledBack below: that is what makes each entity a candidate for the unsafe report.
    List<AspectRowSummary> firstPageAspects = createTestAspectRows(true); // keys for urn1, urn2
    AspectRowSummary extra = new AspectRowSummary();
    extra.setUrn(TEST_URN_2);
    extra.setAspectName("datasetProperties");
    extra.setRunId(TEST_RUN_ID);
    extra.setKeyAspect(false);
    firstPageAspects.add(extra);

    List<AspectRowSummary> secondPageAspects = createMoreTestAspectRows(); // key for urn3

    whenFirstPage(true).thenReturn(firstPageAspects);
    whenPageAfter(true, firstPageAspects.get(firstPageAspects.size() - 1))
        .thenReturn(secondPageAspects);

    RollbackRunResult firstRunResult =
        new RollbackRunResult(withoutKeyRows(firstPageAspects), 0, Collections.emptyList());
    RollbackRunResult secondRunResult =
        new RollbackRunResult(withoutKeyRows(secondPageAspects), 0, Collections.emptyList());
    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(firstRunResult)
        .thenReturn(secondRunResult)
        .thenReturn(new RollbackRunResult(Collections.emptyList(), 0, Collections.emptyList()));

    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(0L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);

    // For each key-aspect URN, return one referencing aspect from a different run — this is
    // what makes the URN "unsafe". Use a distinct runId so the filter (!runId.equals(runId))
    // keeps it.
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), eq(TEST_URN_1), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(unsafeRow(TEST_URN_1, "other-run"));
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), eq(TEST_URN_2), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(unsafeRow(TEST_URN_2, "other-run"));
    String testUrn3 = "urn:li:dataset:(urn:li:dataPlatform:hive,test-dataset-3,PROD)";
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), eq(testUrn3), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(unsafeRow(testUrn3, "other-run"));

    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            true, // hardDelete
            null);

    // urn1 (page 1 only) and urn3 (page 2 only) must both show up as unsafe. A last-page-only
    // derivation would miss urn1 entirely.
    assertEquals(response.getUnsafeEntitiesCount(), 3);
    assertTrue(response.getUnsafeEntities().stream().anyMatch(u -> u.getUrn().equals(TEST_URN_1)));
    assertTrue(response.getUnsafeEntities().stream().anyMatch(u -> u.getUrn().equals(testUrn3)));
  }

  /** The rows a hard rollback reverts when it keeps every entity: all but the key rows. */
  private static List<AspectRowSummary> withoutKeyRows(List<AspectRowSummary> rows) {
    return rows.stream().filter(row -> !row.isKeyAspect()).collect(Collectors.toList());
  }

  private static List<AspectRowSummary> unsafeRow(String urn, String runId) {
    AspectRowSummary row = new AspectRowSummary();
    row.setUrn(urn);
    row.setAspectName("datasetProperties");
    row.setRunId(runId);
    row.setKeyAspect(false);
    return new ArrayList<>(Collections.singletonList(row));
  }

  @Test(timeOut = 10000)
  public void testRollbackIngestion_PageOfRefusedKeyRowsDoesNotStallLaterPages()
      throws AuthenticationException {
    // A hard rollback keeps an entity others wrote to, so its key row is not rolled back and keeps
    // this run's id in the index. A full page of such rows must not be read again: the next page
    // continues after the last of them and reaches the rows behind.
    List<AspectRowSummary> keptKeyRows = new ArrayList<>();
    for (int i = 0; i < MAX_SEARCH_RESULTS; i++) {
      keptKeyRows.add(summaryRow(datasetUrn(i), "datasetKey", true));
    }
    AspectRowSummary lastKeptKeyRow = keptKeyRows.get(keptKeyRows.size() - 1);
    AspectRowSummary rowBehindThem = summaryRow(datasetUrn(MAX_SEARCH_RESULTS), "status", false);
    List<AspectRowSummary> shortPage = new ArrayList<>(Collections.singletonList(rowBehindThem));

    whenFirstPage(true).thenReturn(keptKeyRows);
    whenPageAfter(true, lastKeptKeyRow).thenReturn(shortPage);
    when(mockEntityService.rollbackRun(
            eq(operationContext), eq(keptKeyRows), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(new RollbackRunResult(new ArrayList<>(), 0, Collections.emptyList()));
    when(mockEntityService.rollbackRun(
            eq(operationContext), eq(shortPage), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(
            new RollbackRunResult(
                new ArrayList<>(Collections.singletonList(rowBehindThem)),
                0,
                Collections.emptyList()));
    stubTimeseriesRollback(0L);
    stubNoOtherRunsOnEntities();

    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            true, // hardDelete
            null);

    verify(mockTimeseriesAspectService, times(1)).rollbackTimeseriesAspects(any(), anyString());
    verify(mockEntityService)
        .rollbackRun(
            eq(operationContext),
            argThat(rows -> rows.contains(rowBehindThem)),
            eq(TEST_RUN_ID),
            eq(true));
    // Only the first page starts from the beginning; re-reading it is what made the loop spin.
    verify(mockSystemMetadataService, times(1))
        .findByRunIdAfter(
            any(OperationContext.class),
            eq(TEST_RUN_ID),
            eq(true),
            isNull(),
            eq(MAX_SEARCH_RESULTS));
    assertEquals(response.getEntitiesDeleted(), 0);
    assertEquals(response.getAspectsReverted(), 1);
  }

  @Test
  public void testRollbackIngestion_HardRollbackCountsOnlyKeyRowsItRolledBack()
      throws AuthenticationException {
    AspectRowSummary keyRolledBack1 = summaryRow(datasetUrn(1), "datasetKey", true);
    AspectRowSummary keyKept = summaryRow(datasetUrn(2), "datasetKey", true);
    AspectRowSummary keyRolledBack2 = summaryRow(datasetUrn(3), "datasetKey", true);
    AspectRowSummary nonKey = summaryRow(datasetUrn(1), "datasetProperties", false);
    List<AspectRowSummary> page =
        new ArrayList<>(Arrays.asList(keyRolledBack1, keyKept, keyRolledBack2, nonKey));

    whenFirstPage(true).thenReturn(page);
    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(
            new RollbackRunResult(
                new ArrayList<>(Arrays.asList(keyRolledBack1, keyRolledBack2, nonKey)),
                0,
                Collections.emptyList()));
    stubTimeseriesRollback(0L);
    stubNoOtherRunsOnEntities();

    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            true, // hardDelete
            null);

    assertEquals(response.getEntitiesDeleted(), 2);
  }

  @Test
  public void testRollbackIngestion_SoftRollbackCountsEveryKeyRow() throws AuthenticationException {
    // A soft rollback never rolls a key row back; it marks the entity removed instead, so every
    // key row the run touched counts even though none is in rowsRolledBack.
    List<AspectRowSummary> page =
        new ArrayList<>(
            Arrays.asList(
                summaryRow(datasetUrn(1), "datasetKey", true),
                summaryRow(datasetUrn(2), "datasetKey", true),
                summaryRow(datasetUrn(3), "datasetKey", true),
                summaryRow(datasetUrn(3), "datasetProperties", false)));

    whenFirstPage(false).thenReturn(page);
    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(false)))
        .thenReturn(new RollbackRunResult(new ArrayList<>(), 0, Collections.emptyList()));
    stubTimeseriesRollback(0L);
    stubNoOtherRunsOnEntities();

    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            false, // hardDelete — soft delete
            null);

    assertEquals(response.getEntitiesDeleted(), 3);
  }

  @Test
  public void testRollbackIngestion_HardRollbackChecksOnlyEntitiesItKept()
      throws AuthenticationException {
    String rolledBackUrn = datasetUrn(1);
    String keptUrn = datasetUrn(2);
    AspectRowSummary keyRolledBack = summaryRow(rolledBackUrn, "datasetKey", true);
    AspectRowSummary keyKept = summaryRow(keptUrn, "datasetKey", true);
    whenFirstPage(true).thenReturn(new ArrayList<>(Arrays.asList(keyRolledBack, keyKept)));
    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(
            new RollbackRunResult(
                new ArrayList<>(Collections.singletonList(keyRolledBack)),
                0,
                Collections.emptyList()));
    stubTimeseriesRollback(0L);
    // Both entities have another run's aspect, so only the lookup decides which one is reported.
    stubOtherRunOnEntity(rolledBackUrn);
    stubOtherRunOnEntity(keptUrn);

    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            true, // hardDelete
            null);

    verify(mockSystemMetadataService)
        .findByUrn(
            any(OperationContext.class), eq(keptUrn), eq(false), eq(0), eq(MAX_SEARCH_RESULTS));
    verify(mockSystemMetadataService, never())
        .findByUrn(any(OperationContext.class), eq(rolledBackUrn), anyBoolean(), anyInt(), any());
    assertEquals(unsafeEntityUrns(response), Collections.singletonList(keptUrn));
    assertEquals(response.getUnsafeEntitiesCount(), 1);
    assertEquals(response.getEntitiesDeleted(), 1);
  }

  @Test
  public void testRollbackIngestion_SoftRollbackChecksEveryKeyRowEntity()
      throws AuthenticationException {
    // A soft rollback never returns key rows in rowsRolledBack, yet it hides every entity it
    // touched, so each one has to be checked for other runs' aspects.
    String urnA = datasetUrn(1);
    String urnB = datasetUrn(2);
    whenFirstPage(false)
        .thenReturn(
            new ArrayList<>(
                Arrays.asList(
                    summaryRow(urnA, "datasetKey", true), summaryRow(urnB, "datasetKey", true))));
    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(false)))
        .thenReturn(new RollbackRunResult(new ArrayList<>(), 0, Collections.emptyList()));
    stubTimeseriesRollback(0L);
    stubOtherRunOnEntity(urnA);
    stubOtherRunOnEntity(urnB);

    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            false, // hardDelete — soft delete
            null);

    verify(mockSystemMetadataService)
        .findByUrn(any(OperationContext.class), eq(urnA), eq(false), eq(0), eq(MAX_SEARCH_RESULTS));
    verify(mockSystemMetadataService)
        .findByUrn(any(OperationContext.class), eq(urnB), eq(false), eq(0), eq(MAX_SEARCH_RESULTS));
    assertEquals(response.getUnsafeEntitiesCount(), 2);
  }

  @Test
  public void testRollbackIngestion_FailedPageReadStopsBeforeTimeseriesAndRolledBackStatus()
      throws Exception {
    stubExecutionRequestResult();
    List<AspectRowSummary> fullPage = new ArrayList<>();
    for (int i = 0; i < MAX_SEARCH_RESULTS; i++) {
      fullPage.add(summaryRow(datasetUrn(i), "datasetProperties", false));
    }
    whenFirstPage(true).thenReturn(fullPage);
    // The index read for the second page fails, as findByRunIdAfter does when the search errors.
    whenPageAfter(true, fullPage.get(fullPage.size() - 1))
        .thenThrow(new IllegalStateException("page read failed"));
    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(new RollbackRunResult(new ArrayList<>(fullPage), 0, Collections.emptyList()));

    assertThrows(
        IllegalStateException.class,
        () ->
            rollbackService.rollbackIngestion(
                operationContext,
                TEST_RUN_ID,
                false, // dryRun
                true, // hardDelete
                null));

    verify(mockTimeseriesAspectService, never()).rollbackTimeseriesAspects(any(), anyString());
    assertFalse(writtenExecutionStatuses().contains(RollbackService.ROLLED_BACK_STATUS));
  }

  @Test
  public void testRollbackIngestion_InterruptBetweenPagesStopsRollback()
      throws AuthenticationException {
    List<AspectRowSummary> fullPage = new ArrayList<>();
    for (int i = 0; i < MAX_SEARCH_RESULTS; i++) {
      fullPage.add(summaryRow(datasetUrn(i), "datasetProperties", false));
    }
    List<AspectRowSummary> nextPage =
        new ArrayList<>(
            Collections.singletonList(summaryRow(datasetUrn(MAX_SEARCH_RESULTS), "status", false)));
    whenFirstPage(true).thenReturn(fullPage);
    whenPageAfter(true, fullPage.get(fullPage.size() - 1)).thenReturn(nextPage);
    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(new RollbackRunResult(new ArrayList<>(fullPage), 0, Collections.emptyList()));
    // The real pause: an interrupted thread makes its sleep throw at once, so no time passes.
    RollbackService realPauseService = newRollbackService(null);

    Thread.currentThread().interrupt();
    try {
      assertThrows(
          IllegalStateException.class,
          () ->
              realPauseService.rollbackIngestion(
                  operationContext,
                  TEST_RUN_ID,
                  false, // dryRun
                  true, // hardDelete
                  null));
      // The cancellation must stay visible to whoever runs this thread.
      assertTrue(Thread.interrupted());
    } finally {
      Thread.interrupted();
    }

    verify(mockEntityService, times(1)).rollbackRun(any(), any(), any(), anyBoolean());
    verify(mockSystemMetadataService, never())
        .findByRunIdAfter(
            any(OperationContext.class),
            anyString(),
            anyBoolean(),
            any(AspectRowSummary.class),
            any());
    verifyNoInteractions(mockTimeseriesAspectService);
  }

  private void stubTimeseriesRollback(long numDocsDeleted) {
    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(numDocsDeleted);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);
  }

  private void stubNoOtherRunsOnEntities() {
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());
  }

  @Test
  public void testRollbackIngestion_HardDryRunDoesNotCountEntitiesItWouldKeep()
      throws AuthenticationException {
    String deletedUrn = datasetUrn(1);
    String keptUrn = datasetUrn(2);
    whenFirstPage(true)
        .thenReturn(
            new ArrayList<>(
                Arrays.asList(
                    summaryRow(deletedUrn, "datasetKey", true),
                    summaryRow(keptUrn, "datasetKey", true))));
    // Only keptUrn has another run's aspect, so a hard rollback would keep it.
    stubOtherRunOnEntity(keptUrn);

    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            true, // dryRun
            true, // hardDelete
            null);

    assertEquals(response.getEntitiesDeleted(), 1);
    assertEquals(response.getUnsafeEntitiesCount(), 1);
    verify(mockEntityService, never()).rollbackRun(any(), any(), any(), anyBoolean());
  }

  @Test
  public void testRollbackIngestion_SoftDryRunCountsEveryEntity() throws AuthenticationException {
    String deletedUrn = datasetUrn(1);
    String keptUrn = datasetUrn(2);
    whenFirstPage(false)
        .thenReturn(
            new ArrayList<>(
                Arrays.asList(
                    summaryRow(deletedUrn, "datasetKey", true),
                    summaryRow(keptUrn, "datasetKey", true))));
    stubOtherRunOnEntity(keptUrn);

    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            true, // dryRun
            false, // hardDelete
            null);

    // A soft rollback hides every entity, other runs' aspects or not.
    assertEquals(response.getEntitiesDeleted(), 2);
    assertEquals(response.getUnsafeEntitiesCount(), 1);
  }

  private void stubOtherRunOnEntity(String urn) {
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), eq(urn), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(unsafeRow(urn, "other-run"));
  }

  private static List<String> unsafeEntityUrns(RollbackResponse response) {
    return response.getUnsafeEntities().stream()
        .map(UnsafeEntityInfo::getUrn)
        .collect(Collectors.toList());
  }

  private static String datasetUrn(int index) {
    return String.format("urn:li:dataset:(urn:li:dataPlatform:hive,page-dataset-%d,PROD)", index);
  }

  private static AspectRowSummary summaryRow(String urn, String aspectName, boolean isKeyAspect) {
    AspectRowSummary row = new AspectRowSummary();
    row.setUrn(urn);
    row.setAspectName(aspectName);
    row.setRunId(TEST_RUN_ID);
    row.setKeyAspect(isKeyAspect);
    return row;
  }

  @Test
  public void testRollbackIngestion_DefaultRunId() {
    // Act & Assert
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          rollbackService.rollbackIngestion(
              operationContext,
              DEFAULT_RUN_ID, // Default run ID
              true,
              false,
              null);
        });
  }

  @Test
  public void testUpdateExecutionRequestStatus() throws Exception {
    // Arrange
    String status = "TEST_STATUS";

    EnvelopedAspect testAspect = new EnvelopedAspect();
    testAspect.setName(Constants.STATUS_ASPECT_NAME);
    testAspect.setValue(new Aspect(new Status().setRemoved(false).data()));

    when(mockEntityService.getLatestEnvelopedAspect(
            any(OperationContext.class), anyString(), any(Urn.class), anyString()))
        .thenReturn(testAspect);

    // Act
    rollbackService.updateExecutionRequestStatus(operationContext, TEST_RUN_ID, status);

    // Assert
    ArgumentCaptor<OperationContext> contextCaptor =
        ArgumentCaptor.forClass(OperationContext.class);
    ArgumentCaptor<Urn> urnCaptor = ArgumentCaptor.forClass(Urn.class);
    verify(mockEntityService)
        .getLatestEnvelopedAspect(
            contextCaptor.capture(), anyString(), urnCaptor.capture(), anyString());

    verify(mockEntityService).ingestProposal(eq(operationContext), any(), any(), eq(false));
  }

  @Test
  public void testEnvelopedAspect_Handling() throws Exception {
    // Test that we're correctly handling EnvelopedAspect
    // Arrange
    String status = RollbackService.ROLLING_BACK_STATUS;

    // Create a real EnvelopedAspect with required properties
    EnvelopedAspect testAspect = new EnvelopedAspect();
    testAspect.setName(Constants.STATUS_ASPECT_NAME);
    testAspect.setValue(new Aspect(new Status().setRemoved(false).data()));

    when(mockEntityService.getLatestEnvelopedAspect(
            any(OperationContext.class), anyString(), any(Urn.class), anyString()))
        .thenReturn(testAspect);

    // Act
    rollbackService.updateExecutionRequestStatus(operationContext, TEST_RUN_ID, status);

    // Assert
    verify(mockEntityService).ingestProposal(eq(operationContext), any(), any(), eq(false));
  }

  @Test
  public void testRollbackIngestion_WithDirectEntityDeletion() throws AuthenticationException {
    // Tests the scenario where rows are deleted directly as part of entity deletion
    // Arrange
    List<AspectRowSummary> testAspects = createTestAspectRows(true);

    whenFirstPage(true).thenReturn(testAspects).thenReturn(new ArrayList<>());

    // Create result with rows deleted from entity deletion
    RollbackRunResult mockRollbackResult =
        new RollbackRunResult(
            testAspects,
            10, // 10 rows deleted from entity deletion
            new ArrayList<>());

    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(mockRollbackResult);

    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(5L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);

    // Mock empty lists for unsafe entities calculation
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    // Act
    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            true, // hardDelete
            null);

    // Assert
    verify(mockEntityService).rollbackRun(operationContext, testAspects, TEST_RUN_ID, true);
    assertEquals(response.getEntitiesDeleted(), 2);
    // 4 from aspects + 10 from entity deletion + 5 from timeseries
    assertEquals(response.getAspectsReverted(), 19);
  }

  @Test
  public void testRollbackIngestion_WithDetailedRollbackResults() throws AuthenticationException {
    // Test handling of detailed rollback results
    // Arrange
    List<AspectRowSummary> testAspects = createTestAspectRows(false);

    whenFirstPage(true).thenReturn(testAspects).thenReturn(new ArrayList<>());

    // Create realistic RollbackResult objects
    List<RollbackResult> rollbackResults = new ArrayList<>();

    // Successful rollback with URN, aspectName, changeType
    Urn testUrn1 = UrnUtils.getUrn(TEST_URN_1);
    RollbackResult success1 =
        createRollbackResult(testUrn1, "dataset", "status", ChangeType.UPSERT, false, 0);

    // Failed rollback that reported additional affected rows
    Urn testUrn2 = UrnUtils.getUrn(TEST_URN_2);
    RollbackResult rollbackWithAffectedRows =
        createRollbackResult(
            testUrn2,
            "dataset",
            "datasetProperties",
            ChangeType.DELETE,
            true,
            5 // 5 additional rows affected
            );

    rollbackResults.add(success1);
    rollbackResults.add(rollbackWithAffectedRows);

    RollbackRunResult mockRollbackResult =
        new RollbackRunResult(
            // Only return successful rows
            Collections.singletonList(testAspects.get(0)), 0, rollbackResults);

    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(mockRollbackResult);

    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(0L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);

    // Mock empty lists for unsafe entities calculation
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    // Act
    RollbackResponse response =
        rollbackService.rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            true, // hardDelete
            null);

    // Assert - should only count successfully rolled back rows
    verify(mockEntityService).rollbackRun(operationContext, testAspects, TEST_RUN_ID, true);
    assertEquals(response.getAspectsReverted(), 1);
  }

  @Test
  public void testRollbackTargetAspects_EmptyResults() {
    // Test handling empty results
    // Arrange
    whenFirstPage(true).thenReturn(new ArrayList<>());

    // Act
    List<AspectRowSummary> result =
        rollbackService.rollbackTargetAspects(operationContext, TEST_RUN_ID, true);

    // Assert
    assertTrue(result.isEmpty());
    verifyFirstPageRead(true);
  }

  @Test
  public void testRollbackIngestion_FailedTimeseries() throws AuthenticationException {
    // Test handling failed timeseries rollback
    // Arrange
    List<AspectRowSummary> testAspects = createTestAspectRows(false);

    whenFirstPage(true).thenReturn(testAspects).thenReturn(new ArrayList<>());

    RollbackRunResult mockRollbackResult = new RollbackRunResult(testAspects, 0, new ArrayList<>());

    when(mockEntityService.rollbackRun(
            eq(operationContext), eq(testAspects), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(mockRollbackResult);

    // Simulate failure in timeseries rollback
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenThrow(new RuntimeException("Timeseries rollback failed"));

    // Mock empty lists for unsafe entities calculation
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    // Act & Assert
    assertThrows(
        RuntimeException.class,
        () -> {
          rollbackService.rollbackIngestion(operationContext, TEST_RUN_ID, false, true, null);
        });
  }

  @Test
  public void testRollbackIngestion_DispatcherTakesRollback_NothingRunsInProcess()
      throws Exception {
    stubExecutionRequestResult();
    whenFirstPage(true).thenReturn(createTestAspectRows(true));
    IngestionRollbackDispatcher dispatcher = mock(IngestionRollbackDispatcher.class);
    when(dispatcher.dispatch(operationContext, TEST_RUN_ID, true)).thenReturn(true);

    RollbackResponse response =
        newRollbackService(dispatcher)
            .rollbackIngestion(
                operationContext,
                TEST_RUN_ID,
                false, // dryRun
                true, // hardDelete
                null);

    verify(dispatcher, times(1)).dispatch(operationContext, TEST_RUN_ID, true);
    verify(mockEntityService, never()).rollbackRun(any(), any(), any(), anyBoolean());
    verifyNoInteractions(mockTimeseriesAspectService);
    // The process that runs it marks the run; a write here could land after its end status.
    assertTrue(writtenExecutionStatuses().isEmpty());
    assertEquals(response.getEntitiesAffected(), 0);
    assertEquals(response.getAspectsAffected(), 0);
    assertEquals(response.getEntitiesDeleted(), 0);
    assertEquals(response.getAspectsReverted(), 0);
    assertEquals(response.getUnsafeEntitiesCount(), 0);
    assertTrue(response.getAspectRowSummaries().isEmpty());
    assertTrue(response.getUnsafeEntities().isEmpty());
  }

  @Test
  public void testRollbackIngestion_DispatcherDeclines_RunsInProcess() throws Exception {
    stubExecutionRequestResult();
    List<AspectRowSummary> testAspects = createTestAspectRows(true);
    whenFirstPage(true).thenReturn(testAspects).thenReturn(new ArrayList<>());
    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(new RollbackRunResult(testAspects, 0, Collections.emptyList()));
    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(0L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());
    IngestionRollbackDispatcher dispatcher = mock(IngestionRollbackDispatcher.class);
    when(dispatcher.dispatch(operationContext, TEST_RUN_ID, true)).thenReturn(false);

    newRollbackService(dispatcher)
        .rollbackIngestion(
            operationContext,
            TEST_RUN_ID,
            false, // dryRun
            true, // hardDelete
            null);

    verify(dispatcher, times(1)).dispatch(operationContext, TEST_RUN_ID, true);
    verify(mockEntityService).rollbackRun(operationContext, testAspects, TEST_RUN_ID, true);
    assertEquals(
        writtenExecutionStatuses(),
        List.of(RollbackService.ROLLING_BACK_STATUS, RollbackService.ROLLED_BACK_STATUS));
  }

  @Test
  public void testRollbackIngestion_DispatcherRefuses_PropagatesAndWritesNoStatus()
      throws Exception {
    stubExecutionRequestResult();
    whenFirstPage(true).thenReturn(createTestAspectRows(true));
    RollbackNotHandedOffException refusal = new RollbackNotHandedOffException("not taken");
    IngestionRollbackDispatcher dispatcher = mock(IngestionRollbackDispatcher.class);
    when(dispatcher.dispatch(operationContext, TEST_RUN_ID, true)).thenThrow(refusal);
    RollbackService service = newRollbackService(dispatcher);

    RollbackNotHandedOffException thrown =
        expectThrows(
            RollbackNotHandedOffException.class,
            () ->
                service.rollbackIngestion(
                    operationContext,
                    TEST_RUN_ID,
                    false, // dryRun
                    true, // hardDelete
                    null));

    assertSame(thrown, refusal);
    verify(mockEntityService, never()).rollbackRun(any(), any(), any(), anyBoolean());
    verifyNoInteractions(mockTimeseriesAspectService);
    // Neither ROLLING_BACK nor ROLLBACK_FAILED: the run that already holds the job keeps its
    // status.
    assertEquals(writtenExecutionStatuses(), Collections.emptyList());
  }

  @Test
  public void testRollbackIngestion_NoDispatcher_MarksRollingBackBeforeReadingRows()
      throws Exception {
    stubExecutionRequestResult();
    whenFirstPage(true).thenReturn(new ArrayList<>());
    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(new RollbackRunResult(Collections.emptyList(), 0, Collections.emptyList()));
    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(0L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);

    rollbackService.rollbackIngestion(
        operationContext,
        TEST_RUN_ID,
        false, // dryRun
        true, // hardDelete
        null);

    InOrder statusThenRead = inOrder(mockEntityService, mockSystemMetadataService);
    statusThenRead
        .verify(mockEntityService)
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), any(), anyBoolean());
    statusThenRead
        .verify(mockSystemMetadataService)
        .findByRunIdAfter(
            any(OperationContext.class), eq(TEST_RUN_ID), eq(true), isNull(), anyInt());
  }

  @Test
  public void testRollbackIngestion_DryRunIsNeverOfferedToDispatcher() throws Exception {
    stubExecutionRequestResult();
    whenFirstPage(false).thenReturn(createTestAspectRows(true));
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());
    IngestionRollbackDispatcher dispatcher = mock(IngestionRollbackDispatcher.class);
    when(dispatcher.dispatch(any(), anyString(), anyBoolean())).thenReturn(true);

    RollbackResponse response =
        newRollbackService(dispatcher)
            .rollbackIngestion(
                operationContext,
                TEST_RUN_ID,
                true, // dryRun
                false, // hardDelete
                null);

    verifyNoInteractions(dispatcher);
    assertEquals(response.getEntitiesDeleted(), 2);
    assertEquals(writtenExecutionStatuses(), Collections.emptyList());
  }

  @Test
  public void testRollbackIngestion_UnauthorizedIsNeverOfferedToDispatcher() throws Exception {
    // Authorizer.EMPTY denies everything; REST API authorization is a process-wide flag that is
    // off by default in unit tests, so turn it on for this case only.
    OperationContext deniedContext =
        TestOperationContexts.userContextNoSearchAuthorization(
            Authorizer.EMPTY, UrnUtils.getUrn("urn:li:corpuser:denied-user"));
    whenFirstPage(true).thenReturn(createTestAspectRows(true));
    IngestionRollbackDispatcher dispatcher = mock(IngestionRollbackDispatcher.class);
    // Would hand the rollback to a process that runs as system if the offer preceded authorization.
    when(dispatcher.dispatch(any(), anyString(), anyBoolean())).thenReturn(true);
    RollbackService service = newRollbackService(dispatcher);

    boolean previous = isRestApiAuthorizationEnabled();
    setRestApiAuthorizationEnabled(true);
    try {
      assertThrows(
          AuthenticationException.class,
          () ->
              service.rollbackIngestion(
                  deniedContext,
                  TEST_RUN_ID,
                  false, // dryRun
                  true, // hardDelete
                  null));
    } finally {
      setRestApiAuthorizationEnabled(previous);
    }

    verifyNoInteractions(dispatcher);
    verify(mockEntityService, never()).rollbackRun(any(), any(), any(), anyBoolean());
  }

  /** Lets updateExecutionRequestStatus find a result aspect to update, so its writes show up. */
  private void stubExecutionRequestResult() throws Exception {
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setName(Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME);
    aspect.setValue(new Aspect(new ExecutionRequestResult().setStatus("RUNNING").data()));
    when(mockEntityService.getLatestEnvelopedAspect(
            any(OperationContext.class), anyString(), any(Urn.class), anyString()))
        .thenReturn(aspect);
  }

  /** The execution request statuses written so far, in order. */
  private List<String> writtenExecutionStatuses() {
    ArgumentCaptor<MetadataChangeProposal> proposals =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService, atLeast(0))
        .ingestProposal(
            any(OperationContext.class), proposals.capture(), any(AuditStamp.class), anyBoolean());
    return proposals.getAllValues().stream()
        .map(
            proposal ->
                GenericRecordUtils.deserializeAspect(
                        proposal.getAspect().getValue(),
                        proposal.getAspect().getContentType(),
                        ExecutionRequestResult.class)
                    .getStatus())
        .collect(Collectors.toList());
  }

  private static boolean isRestApiAuthorizationEnabled() throws Exception {
    Field field = AuthUtil.class.getDeclaredField("isRestApiAuthorizationEnabled");
    field.setAccessible(true);
    return field.getBoolean(null);
  }

  private static void setRestApiAuthorizationEnabled(boolean enabled) throws Exception {
    Field field = AuthUtil.class.getDeclaredField("isRestApiAuthorizationEnabled");
    field.setAccessible(true);
    field.setBoolean(null, enabled);
  }

  private List<AspectRowSummary> createTestAspectRows(boolean includeKeyAspects) {
    List<AspectRowSummary> rows = new ArrayList<>();

    // Create two entities with two aspects each (one key and one normal aspect)
    AspectRowSummary row1 = new AspectRowSummary();
    row1.setUrn(TEST_URN_1);
    row1.setAspectName("datasetKey");
    row1.setRunId(TEST_RUN_ID);
    row1.setKeyAspect(true);

    AspectRowSummary row2 = new AspectRowSummary();
    row2.setUrn(TEST_URN_1);
    row2.setAspectName("datasetProperties");
    row2.setRunId(TEST_RUN_ID);
    row2.setKeyAspect(false);

    AspectRowSummary row3 = new AspectRowSummary();
    row3.setUrn(TEST_URN_2);
    row3.setAspectName("datasetKey");
    row3.setRunId(TEST_RUN_ID);
    row3.setKeyAspect(true);

    AspectRowSummary row4 = new AspectRowSummary();
    row4.setUrn(TEST_URN_2);
    row4.setAspectName("status");
    row4.setRunId(TEST_RUN_ID);
    row4.setKeyAspect(false);

    if (includeKeyAspects) {
      rows.add(row1);
      rows.add(row3);
    }

    rows.add(row2);
    rows.add(row4);

    return rows;
  }

  private List<AspectRowSummary> createMoreTestAspectRows() {
    String testUrn3 = "urn:li:dataset:(urn:li:dataPlatform:hive,test-dataset-3,PROD)";

    AspectRowSummary row5 = new AspectRowSummary();
    row5.setUrn(testUrn3);
    row5.setAspectName("datasetKey");
    row5.setRunId(TEST_RUN_ID);
    row5.setKeyAspect(true);

    AspectRowSummary row6 = new AspectRowSummary();
    row6.setUrn(testUrn3);
    row6.setAspectName("status");
    row6.setRunId(TEST_RUN_ID);
    row6.setKeyAspect(false);

    AspectRowSummary row7 = new AspectRowSummary();
    row7.setUrn(testUrn3);
    row7.setAspectName("datasetProperties");
    row7.setRunId(TEST_RUN_ID);
    row7.setKeyAspect(false);

    return new ArrayList<>(Arrays.asList(row5, row6, row7));
  }

  @Test
  public void testRollbackIngestion_NoOp() throws AuthenticationException {
    // Testing a case where rollback is a no-op (old value equals new value)
    // Arrange
    List<AspectRowSummary> testAspects = createTestAspectRows(true);

    whenFirstPage(true).thenReturn(testAspects).thenReturn(new ArrayList<>());

    // Create rollback results where isNoOp() would return true
    Urn testUrn = UrnUtils.getUrn(TEST_URN_1);
    RollbackResult noOpResult =
        new RollbackResult(
            testUrn,
            "dataset",
            "status",
            new Status().setRemoved(false), // Same instance for old and new
            new Status().setRemoved(false), // Same instance for old and new
            null,
            null,
            ChangeType.UPSERT,
            false,
            0);

    List<RollbackResult> rollbackResults = Collections.singletonList(noOpResult);

    RollbackRunResult mockRollbackResult = new RollbackRunResult(testAspects, 0, rollbackResults);

    when(mockEntityService.rollbackRun(eq(operationContext), anyList(), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(mockRollbackResult);

    DeleteAspectValuesResult timeseriesResult = new DeleteAspectValuesResult();
    timeseriesResult.setNumDocsDeleted(0L);
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenReturn(timeseriesResult);

    // Mock empty lists for unsafe entities calculation
    when(mockSystemMetadataService.findByUrn(
            any(OperationContext.class), anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    // Act
    RollbackResponse response =
        rollbackService.rollbackIngestion(operationContext, TEST_RUN_ID, false, true, null);

    // Assert
    verify(mockEntityService).rollbackRun(any(), any(), eq(TEST_RUN_ID), eq(true));
    // RollbackService will count all AspectRowSummary items returned by SystemMetadataService
    assertEquals(response.getAspectsReverted(), 4);
  }

  /** Creates a realistic RollbackResult for testing purposes */
  private RollbackResult createRollbackResult(
      Urn urn,
      String entityName,
      String aspectName,
      ChangeType changeType,
      Boolean keyAffected,
      Integer additionalRowsAffected) {

    return new RollbackResult(
        urn,
        entityName,
        aspectName,
        null, // oldValue
        null, // newValue
        null, // oldSystemMetadata
        null, // newSystemMetadata
        changeType,
        keyAffected,
        additionalRowsAffected);
  }

  /**
   * The run status names who asked for the rollback, not the system. Before, every ROLLING_BACK,
   * ROLLED_BACK and FAILED write was stamped as the system whoever ran it.
   */
  @Test
  public void testExecutionStatusIsStampedWithTheCaller() throws Exception {
    final Urn user = UrnUtils.getUrn("urn:li:corpuser:alice");
    final OperationContext userContext =
        TestOperationContexts.userContextNoSearchAuthorization(user);
    stubExecutionRequestResult();

    rollbackService.updateExecutionRequestStatus(
        userContext, TEST_RUN_ID, RollbackService.ROLLING_BACK_STATUS);

    final ArgumentCaptor<AuditStamp> stamp = ArgumentCaptor.forClass(AuditStamp.class);
    verify(mockEntityService)
        .ingestProposal(any(OperationContext.class), any(), stamp.capture(), eq(false));
    assertEquals(stamp.getValue().getActor(), user);
  }
}
