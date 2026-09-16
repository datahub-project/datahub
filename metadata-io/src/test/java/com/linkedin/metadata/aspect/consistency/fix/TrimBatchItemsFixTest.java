package com.linkedin.metadata.aspect.consistency.fix;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for TrimBatchItemsFix. */
public class TrimBatchItemsFixTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private OperationContext mockOpContext;
  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private EntitySpec mockEntitySpec;

  private BatchItemsFix batchItemsFix;
  private TrimBatchItemsFix trimFix;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockEntityRegistry.getEntitySpec(anyString())).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getName()).thenReturn("testEntity");

    batchItemsFix = new BatchItemsFix(mockEntityService);
    trimFix = new TrimBatchItemsFix(batchItemsFix);
  }

  // ============================================================================
  // Basic Properties
  // ============================================================================

  @Test
  public void testGetType() {
    assertEquals(trimFix.getType(), ConsistencyFixType.TRIM_UPSERT);
  }

  // ============================================================================
  // Error Cases
  // ============================================================================

  @Test
  public void testApplyWithNullBatchItems() {
    Urn urn = UrnUtils.getUrn("urn:li:testEntity:test");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("testEntity")
            .checkId("test-check")
            .fixType(ConsistencyFixType.TRIM_UPSERT)
            .description("Aspect has unknown fields")
            .batchItems(null)
            .build();

    ConsistencyFixDetail result = trimFix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertEquals(result.getUrn(), urn);
    assertEquals(result.getAction(), ConsistencyFixType.TRIM_UPSERT);
    assertTrue(result.getErrorMessage().contains("requires batchItems"));
  }

  @Test
  public void testApplyWithEmptyBatchItems() {
    Urn urn = UrnUtils.getUrn("urn:li:testEntity:test");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("testEntity")
            .checkId("test-check")
            .fixType(ConsistencyFixType.TRIM_UPSERT)
            .description("Aspect has unknown fields")
            .batchItems(List.of())
            .build();

    ConsistencyFixDetail result = trimFix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertTrue(result.getErrorMessage().contains("requires batchItems"));
  }

  // ============================================================================
  // Dry Run Tests
  // ============================================================================

  @Test
  public void testApplyDryRunWithChangeItem() {
    Urn urn = UrnUtils.getUrn("urn:li:testEntity:test");
    Status status = new Status().setRemoved(false);
    AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();

    // Create a mock ChangeItemImpl
    ChangeItemImpl changeItem = mock(ChangeItemImpl.class);
    ChangeItemImpl.ChangeItemImplBuilder mockBuilder =
        mock(ChangeItemImpl.ChangeItemImplBuilder.class);

    when(changeItem.getRecordTemplate()).thenReturn(status);
    when(changeItem.getAspectName()).thenReturn("status");
    when(changeItem.getUrn()).thenReturn(urn);
    when(changeItem.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(changeItem.toBuilder()).thenReturn(mockBuilder);
    when(mockBuilder.metadataChangeProposal(any())).thenReturn(mockBuilder);
    when(mockBuilder.build(any(AspectRetriever.class))).thenReturn(changeItem);

    // Mock getMetadataChangeProposal for the trimmed item
    com.linkedin.mxe.MetadataChangeProposal mcp =
        mock(com.linkedin.mxe.MetadataChangeProposal.class);
    when(changeItem.getMetadataChangeProposal()).thenReturn(mcp);

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("testEntity")
            .checkId("aspect-schema-validation")
            .fixType(ConsistencyFixType.TRIM_UPSERT)
            .description("Test issue")
            .batchItems(List.of(changeItem))
            .build();

    ConsistencyFixDetail result = trimFix.apply(mockOpContext, issue, true);

    assertEquals(result.getAction(), ConsistencyFixType.TRIM_UPSERT);
    // In dry-run, no actual ingestion happens
    verify(mockEntityService, never()).ingestProposal(any(), any(), anyBoolean());
  }

  // ============================================================================
  // DeleteItem Passthrough Tests
  // ============================================================================

  @Test
  public void testDeleteItemsPassThrough() {
    Urn urn = UrnUtils.getUrn("urn:li:testEntity:test");

    // Create a mock DeleteItemImpl - these should pass through unchanged
    DeleteItemImpl deleteItem = mock(DeleteItemImpl.class);
    when(deleteItem.getUrn()).thenReturn(urn);
    when(deleteItem.getAspectName()).thenReturn("testAspect");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("testEntity")
            .checkId("test-check")
            .fixType(ConsistencyFixType.TRIM_UPSERT)
            .description("Test issue")
            .batchItems(List.of(deleteItem))
            .build();

    // Dry run - should process without errors
    ConsistencyFixDetail result = trimFix.apply(mockOpContext, issue, true);

    assertEquals(result.getAction(), ConsistencyFixType.TRIM_UPSERT);
    // Delete items should pass through (trim count would be 0)
    assertTrue(result.getDetails().contains("Trimmed 0 item(s)"));
  }

  // ============================================================================
  // Result Transformation Tests
  // ============================================================================

  @Test
  public void testResultContainsTrimInfo() {
    Urn urn = UrnUtils.getUrn("urn:li:testEntity:test");
    Status status = new Status().setRemoved(false);

    // Create a mock that won't cause NPE
    ChangeItemImpl changeItem = mock(ChangeItemImpl.class);
    ChangeItemImpl.ChangeItemImplBuilder mockBuilder =
        mock(ChangeItemImpl.ChangeItemImplBuilder.class);
    com.linkedin.mxe.MetadataChangeProposal mcp =
        mock(com.linkedin.mxe.MetadataChangeProposal.class);

    when(changeItem.getRecordTemplate()).thenReturn(status);
    when(changeItem.getAspectName()).thenReturn("status");
    when(changeItem.getUrn()).thenReturn(urn);
    when(changeItem.toBuilder()).thenReturn(mockBuilder);
    when(mockBuilder.metadataChangeProposal(any())).thenReturn(mockBuilder);
    when(mockBuilder.build(any(AspectRetriever.class))).thenReturn(changeItem);
    when(changeItem.getMetadataChangeProposal()).thenReturn(mcp);

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("testEntity")
            .checkId("test-check")
            .fixType(ConsistencyFixType.TRIM_UPSERT)
            .description("Test issue")
            .batchItems(List.of(changeItem))
            .build();

    ConsistencyFixDetail result = trimFix.apply(mockOpContext, issue, true);

    assertEquals(result.getAction(), ConsistencyFixType.TRIM_UPSERT);
    assertNotNull(result.getDetails());
    assertTrue(result.getDetails().contains("Trimmed"));
  }

  // ============================================================================
  // Mixed Item Types Tests
  // ============================================================================

  @Test
  public void testMixedItemTypes() {
    Urn urn = UrnUtils.getUrn("urn:li:testEntity:test");
    Status status = new Status().setRemoved(false);

    // Create a ChangeItem
    ChangeItemImpl changeItem = mock(ChangeItemImpl.class);
    ChangeItemImpl.ChangeItemImplBuilder mockBuilder =
        mock(ChangeItemImpl.ChangeItemImplBuilder.class);
    com.linkedin.mxe.MetadataChangeProposal mcp =
        mock(com.linkedin.mxe.MetadataChangeProposal.class);

    when(changeItem.getRecordTemplate()).thenReturn(status);
    when(changeItem.getAspectName()).thenReturn("status");
    when(changeItem.getUrn()).thenReturn(urn);
    when(changeItem.toBuilder()).thenReturn(mockBuilder);
    when(mockBuilder.metadataChangeProposal(any())).thenReturn(mockBuilder);
    when(mockBuilder.build(any(AspectRetriever.class))).thenReturn(changeItem);
    when(changeItem.getMetadataChangeProposal()).thenReturn(mcp);

    // Create a DeleteItem
    DeleteItemImpl deleteItem = mock(DeleteItemImpl.class);
    when(deleteItem.getUrn()).thenReturn(urn);
    when(deleteItem.getAspectName()).thenReturn("otherAspect");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("testEntity")
            .checkId("test-check")
            .fixType(ConsistencyFixType.TRIM_UPSERT)
            .description("Test issue")
            .batchItems(List.of(changeItem, deleteItem))
            .build();

    ConsistencyFixDetail result = trimFix.apply(mockOpContext, issue, true);

    assertEquals(result.getAction(), ConsistencyFixType.TRIM_UPSERT);
    // 1 item trimmed (ChangeItem), 1 passed through (DeleteItem)
    assertTrue(result.getDetails().contains("Trimmed 1 item(s)"));
  }

  // ============================================================================
  // BatchItem with null RecordTemplate Tests
  // ============================================================================

  @Test
  public void testChangeItemWithNullRecordTemplate() {
    Urn urn = UrnUtils.getUrn("urn:li:testEntity:test");

    ChangeItemImpl changeItem = mock(ChangeItemImpl.class);
    when(changeItem.getRecordTemplate()).thenReturn(null);
    when(changeItem.getAspectName()).thenReturn("status");
    when(changeItem.getUrn()).thenReturn(urn);

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("testEntity")
            .checkId("test-check")
            .fixType(ConsistencyFixType.TRIM_UPSERT)
            .description("Test issue")
            .batchItems(List.of(changeItem))
            .build();

    // Should not throw - items with null recordTemplate should pass through
    ConsistencyFixDetail result = trimFix.apply(mockOpContext, issue, true);

    assertEquals(result.getAction(), ConsistencyFixType.TRIM_UPSERT);
    // No trimming happened (null record template)
    assertTrue(result.getDetails().contains("Trimmed 0 item(s)"));
  }
}
