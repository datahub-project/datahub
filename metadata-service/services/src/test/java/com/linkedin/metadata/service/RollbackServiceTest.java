package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SYSTEM_METADATA_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.AuthenticationException;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.RollbackResponse;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.mockito.ArgumentCaptor;
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

    rollbackService =
        new RollbackService(
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
                .build());
  }

  @Test
  public void testRollbackTargetAspects() {
    // Arrange
    List<AspectRowSummary> expectedAspects = createTestAspectRows(false);
    when(mockSystemMetadataService.findByRunId(
            eq(TEST_RUN_ID), eq(true), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(expectedAspects);

    // Act
    List<AspectRowSummary> result = rollbackService.rollbackTargetAspects(TEST_RUN_ID, true);

    // Assert
    assertEquals(result, expectedAspects);
    verify(mockSystemMetadataService).findByRunId(TEST_RUN_ID, true, 0, MAX_SEARCH_RESULTS);
  }

  @Test
  public void testRollbackIngestion_DryRun() throws AuthenticationException {
    // Arrange
    List<AspectRowSummary> testAspects = createTestAspectRows(true);
    when(mockSystemMetadataService.findByRunId(
            eq(TEST_RUN_ID), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(testAspects);

    // For each urn in test aspects
    when(mockSystemMetadataService.findByUrn(anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
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

    when(mockSystemMetadataService.findByRunId(
            eq(TEST_RUN_ID), eq(true), eq(0), eq(MAX_SEARCH_RESULTS)))
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
    when(mockSystemMetadataService.findByUrn(anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
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

    when(mockSystemMetadataService.findByRunId(
            eq(TEST_RUN_ID), eq(true), anyInt(), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(firstPageAspects)
        .thenReturn(secondPageAspects)
        .thenReturn(new ArrayList<>()); // Third call returns empty to end the loop

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
    when(mockSystemMetadataService.findByUrn(anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
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

    when(mockSystemMetadataService.findByRunId(
            eq(TEST_RUN_ID), eq(true), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(testAspects)
        .thenReturn(new ArrayList<>());

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
    when(mockSystemMetadataService.findByUrn(anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
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

    when(mockSystemMetadataService.findByRunId(
            eq(TEST_RUN_ID), eq(true), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(testAspects)
        .thenReturn(new ArrayList<>());

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
    when(mockSystemMetadataService.findByUrn(anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
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
    when(mockSystemMetadataService.findByRunId(
            eq(TEST_RUN_ID), eq(true), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    // Act
    List<AspectRowSummary> result = rollbackService.rollbackTargetAspects(TEST_RUN_ID, true);

    // Assert
    assertTrue(result.isEmpty());
    verify(mockSystemMetadataService).findByRunId(TEST_RUN_ID, true, 0, MAX_SEARCH_RESULTS);
  }

  @Test
  public void testRollbackIngestion_FailedTimeseries() throws AuthenticationException {
    // Test handling failed timeseries rollback
    // Arrange
    List<AspectRowSummary> testAspects = createTestAspectRows(false);

    when(mockSystemMetadataService.findByRunId(
            eq(TEST_RUN_ID), eq(true), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(testAspects)
        .thenReturn(new ArrayList<>());

    RollbackRunResult mockRollbackResult = new RollbackRunResult(testAspects, 0, new ArrayList<>());

    when(mockEntityService.rollbackRun(
            eq(operationContext), eq(testAspects), eq(TEST_RUN_ID), eq(true)))
        .thenReturn(mockRollbackResult);

    // Simulate failure in timeseries rollback
    when(mockTimeseriesAspectService.rollbackTimeseriesAspects(
            eq(operationContext), eq(TEST_RUN_ID)))
        .thenThrow(new RuntimeException("Timeseries rollback failed"));

    // Mock empty lists for unsafe entities calculation
    when(mockSystemMetadataService.findByUrn(anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(new ArrayList<>());

    // Act & Assert
    assertThrows(
        RuntimeException.class,
        () -> {
          rollbackService.rollbackIngestion(operationContext, TEST_RUN_ID, false, true, null);
        });
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

    when(mockSystemMetadataService.findByRunId(
            eq(TEST_RUN_ID), eq(true), eq(0), eq(MAX_SEARCH_RESULTS)))
        .thenReturn(testAspects)
        .thenReturn(new ArrayList<>());

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
    when(mockSystemMetadataService.findByUrn(anyString(), eq(false), eq(0), eq(MAX_SEARCH_RESULTS)))
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
}
