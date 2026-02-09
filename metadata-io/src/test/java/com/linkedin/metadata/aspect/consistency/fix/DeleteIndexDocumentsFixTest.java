package com.linkedin.metadata.aspect.consistency.fix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for DeleteIndexDocumentsFix. */
public class DeleteIndexDocumentsFixTest {

  @Mock private ESSystemMetadataDAO mockEsSystemMetadataDAO;
  @Mock private EntitySearchService mockEntitySearchService;
  @Mock private GraphService mockGraphService;
  @Mock private OperationContext mockOpContext;
  @Mock private SearchContext mockSearchContext;
  @Mock private IndexConvention mockIndexConvention;

  private DeleteIndexDocumentsFix fix;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    fix =
        new DeleteIndexDocumentsFix(
            mockEsSystemMetadataDAO, mockEntitySearchService, mockGraphService);

    // Setup mock chain for getEntityDocumentId
    when(mockOpContext.getSearchContext()).thenReturn(mockSearchContext);
    when(mockSearchContext.getIndexConvention()).thenReturn(mockIndexConvention);
    // Default: return URL-encoded URN as document ID
    when(mockIndexConvention.getEntityDocumentId(any(Urn.class)))
        .thenAnswer(
            inv -> {
              Urn urn = inv.getArgument(0);
              return URLEncoder.encode(urn.toString(), StandardCharsets.UTF_8);
            });
  }

  /** Helper to get the expected document ID for a URN */
  private String getExpectedDocId(Urn urn) {
    return URLEncoder.encode(urn.toString(), StandardCharsets.UTF_8);
  }

  // ============================================================================
  // Basic Properties
  // ============================================================================

  @Test
  public void testGetType() {
    assertEquals(fix.getType(), ConsistencyFixType.DELETE_INDEX_DOCUMENTS);
  }

  // ============================================================================
  // Deletion Order Tests
  // ============================================================================

  @Test
  public void testSystemMetadataDeletedLast() {
    // System metadata should be deleted LAST because it's how orphans are detected.
    // If we delete it first and other deletions fail, we can't detect and retry.
    Urn urn = UrnUtils.getUrn("urn:li:assertion:order-test");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .hardDeleteUrns(List.of(urn))
            .build();

    fix.apply(mockOpContext, issue, false);

    // Verify order: search first, then graph, then system metadata LAST
    InOrder inOrder = inOrder(mockEntitySearchService, mockGraphService, mockEsSystemMetadataDAO);
    inOrder
        .verify(mockEntitySearchService)
        .deleteDocument(mockOpContext, "assertion", getExpectedDocId(urn));
    inOrder.verify(mockGraphService).removeNode(mockOpContext, urn);
    inOrder.verify(mockEsSystemMetadataDAO).deleteByUrn(urn.toString());
  }

  @Test
  public void testSystemMetadataNotDeletedIfOthersAllFail() {
    // If search and graph both fail, we should still try system metadata
    // but the overall operation fails, preserving our ability to retry
    Urn urn = UrnUtils.getUrn("urn:li:assertion:order-fail-test");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .hardDeleteUrns(List.of(urn))
            .build();

    // Search and graph fail
    doThrow(new RuntimeException("Search delete failed"))
        .when(mockEntitySearchService)
        .deleteDocument(any(), eq("assertion"), eq(getExpectedDocId(urn)));
    doThrow(new RuntimeException("Graph delete failed"))
        .when(mockGraphService)
        .removeNode(any(), eq(urn));
    // System metadata succeeds

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    // Still succeeds because system metadata succeeded
    assertTrue(result.isSuccess());

    // Verify all were called in order
    InOrder inOrder = inOrder(mockEntitySearchService, mockGraphService, mockEsSystemMetadataDAO);
    inOrder
        .verify(mockEntitySearchService)
        .deleteDocument(mockOpContext, "assertion", getExpectedDocId(urn));
    inOrder.verify(mockGraphService).removeNode(mockOpContext, urn);
    inOrder.verify(mockEsSystemMetadataDAO).deleteByUrn(urn.toString());
  }

  // ============================================================================
  // Successful Deletion Tests
  // ============================================================================

  @Test
  public void testApplySuccessfulDeletion() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:orphan-1");
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .hardDeleteUrns(List.of(urn))
            .build();

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    assertEquals(result.getUrn(), urn);
    assertEquals(result.getAction(), ConsistencyFixType.DELETE_INDEX_DOCUMENTS);
    assertTrue(result.getDetails().contains("Deleted index documents"));

    // Verify all three indices were called
    verify(mockEsSystemMetadataDAO).deleteByUrn(urn.toString());
    verify(mockEntitySearchService)
        .deleteDocument(mockOpContext, "assertion", getExpectedDocId(urn));
    verify(mockGraphService).removeNode(mockOpContext, urn);
  }

  @Test
  public void testApplyDryRun() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:orphan-1");
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .hardDeleteUrns(List.of(urn))
            .build();

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, true);

    assertTrue(result.isSuccess());
    assertEquals(result.getUrn(), urn);
    assertTrue(result.getDetails().contains("Deleted index documents"));

    // Verify NO indices were called in dry run
    verifyNoInteractions(mockEsSystemMetadataDAO);
    verifyNoInteractions(mockEntitySearchService);
    verifyNoInteractions(mockGraphService);
  }

  @Test
  public void testApplyWithMultipleUrns() {
    Urn primaryUrn = UrnUtils.getUrn("urn:li:monitor:primary");
    Urn urn1 = UrnUtils.getUrn("urn:li:monitor:orphan-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:monitor:orphan-2");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(primaryUrn)
            .entityType("monitor")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index documents")
            .hardDeleteUrns(List.of(urn1, urn2))
            .build();

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("2 entities"));

    // Verify both URNs were deleted from all indices
    verify(mockEsSystemMetadataDAO).deleteByUrn(urn1.toString());
    verify(mockEsSystemMetadataDAO).deleteByUrn(urn2.toString());
    verify(mockEntitySearchService)
        .deleteDocument(mockOpContext, "monitor", getExpectedDocId(urn1));
    verify(mockEntitySearchService)
        .deleteDocument(mockOpContext, "monitor", getExpectedDocId(urn2));
    verify(mockGraphService).removeNode(mockOpContext, urn1);
    verify(mockGraphService).removeNode(mockOpContext, urn2);
  }

  @Test
  public void testApplyFallsBackToEntityUrn() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    // Issue without hardDeleteUrns
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("dataset")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .build();

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());

    // Verify entityUrn was used
    verify(mockEsSystemMetadataDAO).deleteByUrn(urn.toString());
    verify(mockEntitySearchService).deleteDocument(mockOpContext, "dataset", getExpectedDocId(urn));
    verify(mockGraphService).removeNode(mockOpContext, urn);
  }

  // ============================================================================
  // Partial Failure Tests
  // ============================================================================

  @Test
  public void testApplyWithIndexFailureStillSucceeds() {
    // The implementation catches individual index failures and continues,
    // so even if one index fails, the URN is still considered "deleted"
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:success");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:partial");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn1)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index documents")
            .hardDeleteUrns(List.of(urn1, urn2))
            .build();

    // System metadata fails for urn2, but other indices succeed
    doThrow(new RuntimeException("Delete failed"))
        .when(mockEsSystemMetadataDAO)
        .deleteByUrn(urn2.toString());

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    // Still succeeds - individual index failures are caught and logged as warnings
    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("2 entities"));
  }

  @Test
  public void testApplyAllIndicesFailReportsFailure() {
    // When ALL indices fail, the URN is marked as failed
    Urn urn = UrnUtils.getUrn("urn:li:assertion:all-fail");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .hardDeleteUrns(List.of(urn))
            .build();

    // All indices fail
    doThrow(new RuntimeException("System metadata delete failed"))
        .when(mockEsSystemMetadataDAO)
        .deleteByUrn(urn.toString());
    doThrow(new RuntimeException("Search delete failed"))
        .when(mockEntitySearchService)
        .deleteDocument(any(), eq("assertion"), eq(getExpectedDocId(urn)));
    doThrow(new RuntimeException("Graph delete failed"))
        .when(mockGraphService)
        .removeNode(any(), eq(urn));

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    // All indices failed → marked as failure
    assertFalse(result.isSuccess());
    assertNotNull(result.getErrorMessage());
  }

  // ============================================================================
  // Individual Index Failure Tests (should continue with other indices)
  // ============================================================================

  @Test
  public void testSystemMetadataFailureContinuesWithOthers() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .hardDeleteUrns(List.of(urn))
            .build();

    // System metadata fails
    doThrow(new RuntimeException("System metadata delete failed"))
        .when(mockEsSystemMetadataDAO)
        .deleteByUrn(urn.toString());

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    // Still succeeds because we still tried the other indices
    assertTrue(result.isSuccess());

    // Verify other indices were still called
    verify(mockEntitySearchService)
        .deleteDocument(mockOpContext, "assertion", getExpectedDocId(urn));
    verify(mockGraphService).removeNode(mockOpContext, urn);
  }

  @Test
  public void testSearchFailureContinuesWithOthers() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .hardDeleteUrns(List.of(urn))
            .build();

    // Search fails
    doThrow(new RuntimeException("Search delete failed"))
        .when(mockEntitySearchService)
        .deleteDocument(any(), eq("assertion"), eq(getExpectedDocId(urn)));

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    // Still succeeds
    assertTrue(result.isSuccess());

    // Verify other indices were called
    verify(mockEsSystemMetadataDAO).deleteByUrn(urn.toString());
    verify(mockGraphService).removeNode(mockOpContext, urn);
  }

  @Test
  public void testGraphFailureContinuesWithOthers() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .hardDeleteUrns(List.of(urn))
            .build();

    // Graph fails
    doThrow(new RuntimeException("Graph delete failed"))
        .when(mockGraphService)
        .removeNode(any(), eq(urn));

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    // Still succeeds
    assertTrue(result.isSuccess());

    // Verify other indices were called first
    verify(mockEsSystemMetadataDAO).deleteByUrn(urn.toString());
    verify(mockEntitySearchService)
        .deleteDocument(mockOpContext, "assertion", getExpectedDocId(urn));
  }

  // ============================================================================
  // Empty hardDeleteUrns Tests
  // ============================================================================

  @Test
  public void testApplyWithEmptyHardDeleteUrns() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .hardDeleteUrns(List.of()) // Empty list
            .build();

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());

    // Should fall back to entityUrn
    verify(mockEsSystemMetadataDAO).deleteByUrn(urn.toString());
  }

  // ============================================================================
  // Complete Failure Tests (all URNs fail)
  // ============================================================================

  @Test
  public void testApplyCompleteFailureAllIndicesFail() {
    // When ALL indices fail for a URN, the URN should be marked as failed
    Urn urn = UrnUtils.getUrn("urn:li:assertion:complete-fail");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(urn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index document")
            .hardDeleteUrns(List.of(urn))
            .build();

    // All indices fail - this should now throw from deleteFromAllIndices
    doThrow(new RuntimeException("System metadata delete failed"))
        .when(mockEsSystemMetadataDAO)
        .deleteByUrn(urn.toString());
    doThrow(new RuntimeException("Search delete failed"))
        .when(mockEntitySearchService)
        .deleteDocument(any(), eq("assertion"), eq(getExpectedDocId(urn)));
    doThrow(new RuntimeException("Graph delete failed"))
        .when(mockGraphService)
        .removeNode(any(), eq(urn));

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    // Should be a failure since all indices failed
    assertFalse(result.isSuccess());
    assertEquals(result.getUrn(), urn);
    assertEquals(result.getAction(), ConsistencyFixType.DELETE_INDEX_DOCUMENTS);
    assertNotNull(result.getErrorMessage());
    assertTrue(result.getErrorMessage().contains("Failed to delete"));
    assertTrue(result.getErrorMessage().contains(urn.toString()));
  }

  @Test
  public void testApplyCompleteFailureMultipleUrns() {
    // When multiple URNs all fail completely
    Urn primaryUrn = UrnUtils.getUrn("urn:li:assertion:primary");
    Urn urn1 = UrnUtils.getUrn("urn:li:assertion:fail-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:assertion:fail-2");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(primaryUrn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index documents")
            .hardDeleteUrns(List.of(urn1, urn2))
            .build();

    // All indices fail for both URNs
    doThrow(new RuntimeException("Delete failed")).when(mockEsSystemMetadataDAO).deleteByUrn(any());
    doThrow(new RuntimeException("Delete failed"))
        .when(mockEntitySearchService)
        .deleteDocument(any(), any(), any());
    doThrow(new RuntimeException("Delete failed")).when(mockGraphService).removeNode(any(), any());

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertNotNull(result.getErrorMessage());
    assertTrue(result.getErrorMessage().contains("2 entities"));
  }

  // ============================================================================
  // Partial Success Tests (some URNs succeed, some fail)
  // ============================================================================

  @Test
  public void testApplyPartialSuccess() {
    // First URN succeeds, second URN fails completely
    Urn primaryUrn = UrnUtils.getUrn("urn:li:assertion:primary");
    Urn successUrn = UrnUtils.getUrn("urn:li:assertion:success");
    Urn failUrn = UrnUtils.getUrn("urn:li:assertion:fail");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(primaryUrn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index documents")
            .hardDeleteUrns(List.of(successUrn, failUrn))
            .build();

    // All indices fail for failUrn only
    doThrow(new RuntimeException("Delete failed"))
        .when(mockEsSystemMetadataDAO)
        .deleteByUrn(failUrn.toString());
    doThrow(new RuntimeException("Delete failed"))
        .when(mockEntitySearchService)
        .deleteDocument(any(), eq("assertion"), eq(getExpectedDocId(failUrn)));
    doThrow(new RuntimeException("Delete failed"))
        .when(mockGraphService)
        .removeNode(any(), eq(failUrn));

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    // Partial success - success=false but has both details and errorMessage
    assertFalse(result.isSuccess());
    assertNotNull(result.getDetails());
    assertTrue(result.getDetails().contains("Partial success"));
    assertTrue(result.getDetails().contains(successUrn.toString()));
    assertNotNull(result.getErrorMessage());
    assertTrue(result.getErrorMessage().contains(failUrn.toString()));
  }

  @Test
  public void testApplyPartialSuccessMultipleFailures() {
    // 2 succeed, 2 fail
    Urn primaryUrn = UrnUtils.getUrn("urn:li:assertion:primary");
    Urn success1 = UrnUtils.getUrn("urn:li:assertion:success-1");
    Urn success2 = UrnUtils.getUrn("urn:li:assertion:success-2");
    Urn fail1 = UrnUtils.getUrn("urn:li:assertion:fail-1");
    Urn fail2 = UrnUtils.getUrn("urn:li:assertion:fail-2");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(primaryUrn)
            .entityType("assertion")
            .checkId("orphan-index-document")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan index documents")
            .hardDeleteUrns(List.of(success1, fail1, success2, fail2))
            .build();

    // All indices fail for fail1 and fail2
    for (Urn failUrn : List.of(fail1, fail2)) {
      doThrow(new RuntimeException("Delete failed"))
          .when(mockEsSystemMetadataDAO)
          .deleteByUrn(failUrn.toString());
      doThrow(new RuntimeException("Delete failed"))
          .when(mockEntitySearchService)
          .deleteDocument(any(), eq("assertion"), eq(getExpectedDocId(failUrn)));
      doThrow(new RuntimeException("Delete failed"))
          .when(mockGraphService)
          .removeNode(any(), eq(failUrn));
    }

    ConsistencyFixDetail result = fix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertTrue(result.getDetails().contains("2 entities")); // 2 succeeded
    assertTrue(result.getErrorMessage().contains("2 entities")); // 2 failed
  }

  // ============================================================================
  // Exception Handling Tests
  // ============================================================================

  @Test
  public void testDeleteFromAllIndicesThrowsWhenAllFail() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test");

    // All indices fail
    doThrow(new RuntimeException("System metadata delete failed"))
        .when(mockEsSystemMetadataDAO)
        .deleteByUrn(urn.toString());
    doThrow(new RuntimeException("Search delete failed"))
        .when(mockEntitySearchService)
        .deleteDocument(any(), eq("assertion"), eq(getExpectedDocId(urn)));
    doThrow(new RuntimeException("Graph delete failed"))
        .when(mockGraphService)
        .removeNode(any(), eq(urn));

    // Calling deleteFromAllIndices directly should throw
    try {
      fix.deleteFromAllIndices(mockOpContext, urn, "assertion");
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Failed to delete from all indices"));
      assertTrue(e.getMessage().contains(urn.toString()));
      assertNotNull(e.getCause());
    }
  }

  @Test
  public void testDeleteFromAllIndicesSucceedsWithPartialIndexSuccess() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test");

    // Only system metadata fails, others succeed
    doThrow(new RuntimeException("System metadata delete failed"))
        .when(mockEsSystemMetadataDAO)
        .deleteByUrn(urn.toString());

    // Should not throw - at least one index succeeded
    fix.deleteFromAllIndices(mockOpContext, urn, "assertion");

    // Verify all indices were attempted
    verify(mockEsSystemMetadataDAO).deleteByUrn(urn.toString());
    verify(mockEntitySearchService)
        .deleteDocument(mockOpContext, "assertion", getExpectedDocId(urn));
    verify(mockGraphService).removeNode(mockOpContext, urn);
  }

  @Test
  public void testDeleteFromAllIndicesSucceedsWithSingleIndexSuccess() {
    Urn urn = UrnUtils.getUrn("urn:li:assertion:test");

    // System metadata and search fail, only graph succeeds
    doThrow(new RuntimeException("System metadata delete failed"))
        .when(mockEsSystemMetadataDAO)
        .deleteByUrn(urn.toString());
    doThrow(new RuntimeException("Search delete failed"))
        .when(mockEntitySearchService)
        .deleteDocument(any(), eq("assertion"), eq(getExpectedDocId(urn)));

    // Should not throw - at least one index (graph) succeeded
    fix.deleteFromAllIndices(mockOpContext, urn, "assertion");

    verify(mockGraphService).removeNode(mockOpContext, urn);
  }
}
