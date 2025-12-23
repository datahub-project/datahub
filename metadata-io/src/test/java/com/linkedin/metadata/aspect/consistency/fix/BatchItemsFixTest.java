package com.linkedin.metadata.aspect.consistency.fix;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for BatchItemsFix implementation. */
public class BatchItemsFixTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private OperationContext mockOpContext;
  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private EntitySpec mockEntitySpec;
  @Mock private ChangeItemImpl mockChangeItem;
  @Mock private DeleteItemImpl mockDeleteItem;

  private BatchItemsFix batchItemsFix;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    // Setup the context chain for AspectsBatchImpl building
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockEntityRegistry.getEntitySpec(anyString())).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getName()).thenReturn("dataset");

    batchItemsFix = new BatchItemsFix(mockEntityService);
  }

  // ============================================================================
  // Fix Type Tests
  // ============================================================================

  @Test
  public void testGetType() {
    assertEquals(batchItemsFix.getType(), ConsistencyFixType.UPSERT);
  }

  @Test
  public void testSupportsAllMcpTypes() {
    assertTrue(batchItemsFix.supports(ConsistencyFixType.CREATE));
    assertTrue(batchItemsFix.supports(ConsistencyFixType.UPSERT));
    assertTrue(batchItemsFix.supports(ConsistencyFixType.PATCH));
    assertTrue(batchItemsFix.supports(ConsistencyFixType.SOFT_DELETE));
    assertTrue(batchItemsFix.supports(ConsistencyFixType.DELETE_ASPECT));
  }

  @Test
  public void testDoesNotSupportNonMcpTypes() {
    assertFalse(batchItemsFix.supports(ConsistencyFixType.HARD_DELETE));
    assertFalse(batchItemsFix.supports(ConsistencyFixType.DELETE_INDEX_DOCUMENTS));
    assertFalse(batchItemsFix.supports(ConsistencyFixType.TRIM_UPSERT));
  }

  // ============================================================================
  // Empty Batch Items Tests
  // ============================================================================

  @Test
  public void testApplyFailsWhenNoBatchItems() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(null)
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertTrue(result.getErrorMessage().contains("requires batchItems"));
  }

  @Test
  public void testApplyFailsWhenEmptyBatchItems() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of())
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertTrue(result.getErrorMessage().contains("requires batchItems"));
  }

  // ============================================================================
  // Dry Run Tests
  // ============================================================================

  @Test
  public void testApplyDryRunDoesNotIngest() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockChangeItem.getAspectName()).thenReturn("status");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, true);

    assertTrue(result.isSuccess());
    assertNotNull(result.getDetails());
    assertTrue(result.getDetails().contains("Ingested 1 item(s)"));

    // Verify entityService was NOT called in dry run mode
    verify(mockEntityService, never()).ingestProposal(any(), any(), anyBoolean());
  }

  // ============================================================================
  // Ingest Items Tests (Dry Run Only - real ingest requires full context)
  // ============================================================================

  @Test
  public void testApplyDryRunIngestChangeItems() {
    // Test that dry run mode works correctly for ingest items
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockChangeItem.getAspectName()).thenReturn("status");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem))
            .build();

    // Dry run should succeed without actually calling ingestProposal
    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, true);

    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("Ingested 1 item(s)"));

    // Verify entityService was NOT called in dry run
    verify(mockEntityService, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testMultipleChangeItemsDryRun() {
    // Test dry run with multiple change items
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    ChangeItemImpl mockChangeItem2 = mock(ChangeItemImpl.class);
    when(mockChangeItem.getAspectName()).thenReturn("status");
    when(mockChangeItem2.getAspectName()).thenReturn("datasetProperties");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem, mockChangeItem2))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, true);

    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("Ingested 2 item(s)"));
  }

  // ============================================================================
  // Non-Dry Run Ingest Tests
  // ============================================================================

  @Test
  public void testApplyIngestSucceeds() {
    // Test non-dry run ingest via ingestProposal
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockChangeItem.getAspectName()).thenReturn("status");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    assertNotNull(result.getDetails());
    assertTrue(result.getDetails().contains("Ingested 1 item(s)"));

    // Verify entityService.ingestProposal was called in non-dry run mode
    verify(mockEntityService).ingestProposal(eq(mockOpContext), any(), eq(false));
  }

  @Test
  public void testApplyIngestMultipleItemsSucceeds() {
    // Test non-dry run with multiple change items
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    ChangeItemImpl mockChangeItem2 = mock(ChangeItemImpl.class);
    when(mockChangeItem.getAspectName()).thenReturn("status");
    when(mockChangeItem2.getAspectName()).thenReturn("datasetProperties");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem, mockChangeItem2))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("Ingested 2 item(s)"));

    // Verify ingestProposal was called
    verify(mockEntityService).ingestProposal(eq(mockOpContext), any(), eq(false));
  }

  @Test
  public void testApplyIngestThrowsException() {
    // Test exception handling when ingestProposal fails
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockChangeItem.getAspectName()).thenReturn("status");

    // ingestProposal throws an exception
    doThrow(new RuntimeException("Ingest failed due to validation error"))
        .when(mockEntityService)
        .ingestProposal(any(), any(), anyBoolean());

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertNotNull(result.getErrorMessage());
    assertTrue(result.getErrorMessage().contains("Failed to ingest items"));
    assertTrue(result.getErrorMessage().contains("Ingest failed due to validation error"));
  }

  // ============================================================================
  // Partial Success Tests
  // ============================================================================

  @Test
  public void testPartialSuccessWhenIngestSucceedsButDeleteFails() {
    // Test partial success: ingest works but delete fails
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockChangeItem.getAspectName()).thenReturn("status");
    when(mockDeleteItem.getUrn()).thenReturn(urn);
    when(mockDeleteItem.getAspectName()).thenReturn("testAspect");

    // ingestProposal succeeds but deleteAspect fails
    doThrow(new RuntimeException("Delete failed"))
        .when(mockEntityService)
        .deleteAspect(any(), any(), any(), any(), anyBoolean());

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem, mockDeleteItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    // Should be partial success (not fully successful due to delete failure)
    assertFalse(result.isSuccess());
    assertTrue(result.getDetails().contains("Partial success"));
    assertTrue(result.getDetails().contains("Ingested 1 item(s)"));
    assertTrue(result.getErrorMessage().contains("Failed to delete aspect"));

    // Verify both operations were attempted
    verify(mockEntityService).ingestProposal(eq(mockOpContext), any(), eq(false));
    verify(mockEntityService)
        .deleteAspect(eq(mockOpContext), eq(urn.toString()), eq("testAspect"), isNull(), eq(true));
  }

  @Test
  public void testPartialSuccessWhenIngestFailsButDeleteSucceeds() {
    // Test partial success: ingest fails but delete works
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockChangeItem.getAspectName()).thenReturn("status");
    when(mockDeleteItem.getUrn()).thenReturn(urn);
    when(mockDeleteItem.getAspectName()).thenReturn("testAspect");

    // ingestProposal fails but deleteAspect succeeds
    doThrow(new RuntimeException("Ingest validation failed"))
        .when(mockEntityService)
        .ingestProposal(any(), any(), anyBoolean());

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem, mockDeleteItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    // Should be partial success (ingest failed, delete succeeded)
    assertFalse(result.isSuccess());
    assertTrue(result.getDetails().contains("Partial success"));
    assertTrue(result.getDetails().contains("Deleted aspect testAspect"));
    assertTrue(result.getErrorMessage().contains("Failed to ingest items"));

    // Verify both operations were attempted
    verify(mockEntityService).ingestProposal(eq(mockOpContext), any(), eq(false));
    verify(mockEntityService)
        .deleteAspect(eq(mockOpContext), eq(urn.toString()), eq("testAspect"), isNull(), eq(true));
  }

  @Test
  public void testCompleteFailureWhenBothIngestAndDeleteFail() {
    // Test complete failure: both ingest and delete fail
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockChangeItem.getAspectName()).thenReturn("status");
    when(mockDeleteItem.getUrn()).thenReturn(urn);
    when(mockDeleteItem.getAspectName()).thenReturn("testAspect");

    // Both operations fail
    doThrow(new RuntimeException("Ingest failed"))
        .when(mockEntityService)
        .ingestProposal(any(), any(), anyBoolean());
    doThrow(new RuntimeException("Delete failed"))
        .when(mockEntityService)
        .deleteAspect(any(), any(), any(), any(), anyBoolean());

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem, mockDeleteItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    // Should be complete failure
    assertFalse(result.isSuccess());
    // No partial success message since nothing succeeded
    assertFalse(result.getDetails() != null && result.getDetails().contains("Partial success"));
    assertNotNull(result.getErrorMessage());
    assertTrue(result.getErrorMessage().contains("Failed to ingest items"));
    assertTrue(result.getErrorMessage().contains("Failed to delete aspect"));
  }

  // ============================================================================
  // Delete Items Tests
  // ============================================================================

  @Test
  public void testApplyDeletesAspects() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockDeleteItem.getUrn()).thenReturn(urn);
    when(mockDeleteItem.getAspectName()).thenReturn("testAspect");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.DELETE_ASPECT)
            .description("Test issue")
            .batchItems(List.of(mockDeleteItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("Deleted aspect testAspect"));

    // Verify deleteAspect was called
    verify(mockEntityService)
        .deleteAspect(eq(mockOpContext), eq(urn.toString()), eq("testAspect"), isNull(), eq(true));
  }

  @Test
  public void testApplyDryRunDeleteDoesNotDelete() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockDeleteItem.getUrn()).thenReturn(urn);
    when(mockDeleteItem.getAspectName()).thenReturn("testAspect");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.DELETE_ASPECT)
            .description("Test issue")
            .batchItems(List.of(mockDeleteItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, true);

    assertTrue(result.isSuccess());

    // Verify deleteAspect was NOT called in dry run mode
    verify(mockEntityService, never()).deleteAspect(any(), any(), any(), any(), anyBoolean());
  }

  @Test
  public void testApplyHandlesDeleteException() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockDeleteItem.getUrn()).thenReturn(urn);
    when(mockDeleteItem.getAspectName()).thenReturn("testAspect");
    doThrow(new RuntimeException("Delete failed"))
        .when(mockEntityService)
        .deleteAspect(any(), any(), any(), any(), anyBoolean());

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.DELETE_ASPECT)
            .description("Test issue")
            .batchItems(List.of(mockDeleteItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertTrue(result.getErrorMessage().contains("Failed to delete aspect"));
  }

  // ============================================================================
  // Mixed Items Tests
  // ============================================================================

  @Test
  public void testApplyMixedItemsDryRun() {
    // Test dry run with mixed change and delete items
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockChangeItem.getAspectName()).thenReturn("status");
    when(mockDeleteItem.getUrn()).thenReturn(urn);
    when(mockDeleteItem.getAspectName()).thenReturn("testAspect");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem, mockDeleteItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, true);

    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("Ingested 1 item(s)"));
    assertTrue(result.getDetails().contains("Deleted aspect testAspect"));

    // Neither operation should be called in dry run
    verify(mockEntityService, never()).ingestProposal(any(), any(), anyBoolean());
    verify(mockEntityService, never()).deleteAspect(any(), any(), any(), any(), anyBoolean());
  }

  @Test
  public void testApplyDeleteFailure() {
    // Test that delete failures are properly reported
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockDeleteItem.getUrn()).thenReturn(urn);
    when(mockDeleteItem.getAspectName()).thenReturn("testAspect");

    // Delete fails
    doThrow(new RuntimeException("Delete failed"))
        .when(mockEntityService)
        .deleteAspect(any(), any(), any(), any(), anyBoolean());

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.DELETE_ASPECT)
            .description("Test issue")
            .batchItems(List.of(mockDeleteItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    // Should fail with error message
    assertFalse(result.isSuccess());
    assertTrue(result.getErrorMessage().contains("Failed to delete aspect"));
  }

  @Test
  public void testApplyMixedItemsNonDryRun() {
    // Test non-dry run with mixed change and delete items - both succeed
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    when(mockChangeItem.getAspectName()).thenReturn("status");
    when(mockDeleteItem.getUrn()).thenReturn(urn);
    when(mockDeleteItem.getAspectName()).thenReturn("testAspect");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(List.of(mockChangeItem, mockDeleteItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("Ingested 1 item(s)"));
    assertTrue(result.getDetails().contains("Deleted aspect testAspect"));

    // Both operations should be called
    verify(mockEntityService).ingestProposal(eq(mockOpContext), any(), eq(false));
    verify(mockEntityService)
        .deleteAspect(eq(mockOpContext), eq(urn.toString()), eq("testAspect"), isNull(), eq(true));
  }

  @Test
  public void testMultipleDeleteItemsWithPartialFailure() {
    // Test that when one delete fails, others continue processing
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    DeleteItemImpl mockDeleteItem2 = mock(DeleteItemImpl.class);
    when(mockDeleteItem.getUrn()).thenReturn(urn);
    when(mockDeleteItem.getAspectName()).thenReturn("aspect1");
    when(mockDeleteItem2.getUrn()).thenReturn(urn);
    when(mockDeleteItem2.getAspectName()).thenReturn("aspect2");

    // First delete (aspect1) fails, second delete (aspect2) succeeds
    doThrow(new RuntimeException("First delete failed"))
        .when(mockEntityService)
        .deleteAspect(eq(mockOpContext), eq(urn.toString()), eq("aspect1"), isNull(), eq(true));
    // Default behavior for aspect2 will be success (no exception)

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("test-check")
            .fixType(ConsistencyFixType.DELETE_ASPECT)
            .description("Test issue")
            .batchItems(List.of(mockDeleteItem, mockDeleteItem2))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    // Should be partial success
    assertFalse(result.isSuccess());
    assertTrue(result.getDetails().contains("Partial success"));
    assertTrue(result.getDetails().contains("Deleted aspect aspect2"));
    assertTrue(result.getErrorMessage().contains("Failed to delete aspect aspect1"));

    // Verify both deletes were attempted
    verify(mockEntityService, times(2)).deleteAspect(any(), any(), any(), any(), anyBoolean());
  }
}
