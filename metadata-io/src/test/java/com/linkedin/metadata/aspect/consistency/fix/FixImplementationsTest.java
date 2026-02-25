package com.linkedin.metadata.aspect.consistency.fix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for fix implementations covering success, failure, and edge cases. */
public class FixImplementationsTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private ESSystemMetadataDAO mockEsSystemMetadataDAO;
  @Mock private EntitySearchService mockEntitySearchService;
  @Mock private GraphService mockGraphService;
  @Mock private OperationContext mockOpContext;
  @Mock private SearchContext mockSearchContext;
  @Mock private IndexConvention mockIndexConvention;

  private BatchItemsFix batchItemsFix;
  private HardDeleteEntityFix hardDeleteFix;
  private DeleteIndexDocumentsFix deleteIndexDocumentsFix;

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:assertion:test-assertion-123");
  private static final Urn TEST_URN_2 = UrnUtils.getUrn("urn:li:assertion:test-assertion-456");

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    batchItemsFix = new BatchItemsFix(mockEntityService);
    hardDeleteFix = new HardDeleteEntityFix(mockEntityService);
    deleteIndexDocumentsFix =
        new DeleteIndexDocumentsFix(
            mockEsSystemMetadataDAO, mockEntitySearchService, mockGraphService);

    // Setup mock chain for getEntityDocumentId
    when(mockOpContext.getSearchContext()).thenReturn(mockSearchContext);
    when(mockSearchContext.getIndexConvention()).thenReturn(mockIndexConvention);
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
  // BatchItemsFix Tests
  // ============================================================================

  @Test
  public void testBatchItemsFixType() {
    assertEquals(batchItemsFix.getType(), ConsistencyFixType.UPSERT);
  }

  @Test
  public void testBatchItemsFixSupports() {
    assertTrue(batchItemsFix.supports(ConsistencyFixType.CREATE));
    assertTrue(batchItemsFix.supports(ConsistencyFixType.UPSERT));
    assertTrue(batchItemsFix.supports(ConsistencyFixType.PATCH));
    assertTrue(batchItemsFix.supports(ConsistencyFixType.SOFT_DELETE));
    assertTrue(batchItemsFix.supports(ConsistencyFixType.DELETE_ASPECT));
    assertFalse(batchItemsFix.supports(ConsistencyFixType.HARD_DELETE));
    assertFalse(batchItemsFix.supports(ConsistencyFixType.TRIM_UPSERT));
  }

  @Test
  public void testBatchItemsFixNullBatchItems() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.UPSERT)
            .description("Test issue")
            .batchItems(null)
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertEquals(result.getUrn(), TEST_URN);
    assertEquals(result.getAction(), ConsistencyFixType.UPSERT);
    assertTrue(result.getErrorMessage().contains("requires batchItems"));
  }

  @Test
  public void testBatchItemsFixEmptyBatchItems() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.SOFT_DELETE)
            .description("Test issue")
            .batchItems(List.of())
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertEquals(result.getAction(), ConsistencyFixType.SOFT_DELETE);
    assertTrue(result.getErrorMessage().contains("requires batchItems"));
  }

  @Test
  public void testBatchItemsFixDryRun() {
    // Create a mock BatchItem that is NOT a DeleteItemImpl
    BatchItem mockItem = mock(BatchItem.class);
    when(mockItem.getAspectName()).thenReturn("status");

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.SOFT_DELETE)
            .description("Test issue")
            .batchItems(List.of(mockItem))
            .build();

    ConsistencyFixDetail result = batchItemsFix.apply(mockOpContext, issue, true);

    assertTrue(result.isSuccess());
    assertEquals(result.getAction(), ConsistencyFixType.SOFT_DELETE);
    // Verify no actual ingestion happened in dry-run mode
    verify(mockEntityService, never()).ingestProposal(any(), any(), anyBoolean());
  }

  // ============================================================================
  // HardDeleteEntityFix Tests
  // ============================================================================

  @Test
  public void testHardDeleteFixType() {
    assertEquals(hardDeleteFix.getType(), ConsistencyFixType.HARD_DELETE);
  }

  @Test
  public void testHardDeleteFixSingleUrn() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Test issue")
            .build();

    ConsistencyFixDetail result = hardDeleteFix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    assertEquals(result.getUrn(), TEST_URN);
    assertEquals(result.getAction(), ConsistencyFixType.HARD_DELETE);
    assertTrue(result.getDetails().contains("Hard deleted"));
    verify(mockEntityService).deleteUrn(mockOpContext, TEST_URN);
  }

  @Test
  public void testHardDeleteFixMultipleUrns() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Test issue")
            .hardDeleteUrns(List.of(TEST_URN, TEST_URN_2))
            .build();

    ConsistencyFixDetail result = hardDeleteFix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("2 entities"));
    verify(mockEntityService, times(2)).deleteUrn(eq(mockOpContext), any(Urn.class));
  }

  @Test
  public void testHardDeleteFixDryRun() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Test issue")
            .build();

    ConsistencyFixDetail result = hardDeleteFix.apply(mockOpContext, issue, true);

    assertTrue(result.isSuccess());
    // Verify no actual deletion happened in dry-run mode
    verify(mockEntityService, never()).deleteUrn(any(), any(Urn.class));
  }

  @Test
  public void testHardDeleteFixFailure() {
    doThrow(new RuntimeException("Delete failed"))
        .when(mockEntityService)
        .deleteUrn(any(), any(Urn.class));

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Test issue")
            .build();

    ConsistencyFixDetail result = hardDeleteFix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertNotNull(result.getErrorMessage());
    assertTrue(result.getErrorMessage().contains("Failed to hard-delete"));
  }

  @Test
  public void testHardDeleteFixPartialSuccess() {
    // First call succeeds, second throws
    RollbackRunResult mockResult = mock(RollbackRunResult.class);
    when(mockEntityService.deleteUrn(any(), any(Urn.class)))
        .thenReturn(mockResult)
        .thenThrow(new RuntimeException("Delete failed"));

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("test-check")
            .fixType(ConsistencyFixType.HARD_DELETE)
            .description("Test issue")
            .hardDeleteUrns(List.of(TEST_URN, TEST_URN_2))
            .build();

    ConsistencyFixDetail result = hardDeleteFix.apply(mockOpContext, issue, false);

    assertFalse(result.isSuccess());
    assertNotNull(result.getDetails()); // Partial success details
    assertNotNull(result.getErrorMessage()); // Failure info
  }

  // ============================================================================
  // DeleteIndexDocumentsFix Tests
  // ============================================================================

  @Test
  public void testDeleteIndexDocumentsFixType() {
    assertEquals(deleteIndexDocumentsFix.getType(), ConsistencyFixType.DELETE_INDEX_DOCUMENTS);
  }

  @Test
  public void testDeleteIndexDocumentsFixSingleUrn() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("orphan-check")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan document")
            .build();

    ConsistencyFixDetail result = deleteIndexDocumentsFix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    assertEquals(result.getAction(), ConsistencyFixType.DELETE_INDEX_DOCUMENTS);
    verify(mockEsSystemMetadataDAO).deleteByUrn(TEST_URN.toString());
    verify(mockEntitySearchService)
        .deleteDocument(mockOpContext, "assertion", getExpectedDocId(TEST_URN));
    verify(mockGraphService).removeNode(mockOpContext, TEST_URN);
  }

  @Test
  public void testDeleteIndexDocumentsFixMultipleUrns() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("orphan-check")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan documents")
            .hardDeleteUrns(List.of(TEST_URN, TEST_URN_2))
            .build();

    ConsistencyFixDetail result = deleteIndexDocumentsFix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("2 entities"));
    verify(mockEsSystemMetadataDAO, times(2)).deleteByUrn(anyString());
    verify(mockEntitySearchService, times(2)).deleteDocument(any(), anyString(), anyString());
    verify(mockGraphService, times(2)).removeNode(any(), any(Urn.class));
  }

  @Test
  public void testDeleteIndexDocumentsFixDryRun() {
    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("orphan-check")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan document")
            .build();

    ConsistencyFixDetail result = deleteIndexDocumentsFix.apply(mockOpContext, issue, true);

    assertTrue(result.isSuccess());
    // Verify no actual deletions in dry-run mode
    verify(mockEsSystemMetadataDAO, never()).deleteByUrn(anyString());
    verify(mockEntitySearchService, never()).deleteDocument(any(), anyString(), anyString());
    verify(mockGraphService, never()).removeNode(any(), any(Urn.class));
  }

  @Test
  public void testDeleteIndexDocumentsFixContinuesOnPartialFailure() {
    // System metadata delete succeeds
    BulkByScrollResponse mockResponse = mock(BulkByScrollResponse.class);
    when(mockEsSystemMetadataDAO.deleteByUrn(anyString())).thenReturn(mockResponse);
    // Search delete fails
    doThrow(new RuntimeException("Search delete failed"))
        .when(mockEntitySearchService)
        .deleteDocument(any(), anyString(), anyString());
    // Graph delete fails
    doThrow(new RuntimeException("Graph delete failed"))
        .when(mockGraphService)
        .removeNode(any(), any(Urn.class));

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("orphan-check")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan document")
            .build();

    // Should still succeed because system metadata delete worked
    // The fix continues on individual index failures
    ConsistencyFixDetail result = deleteIndexDocumentsFix.apply(mockOpContext, issue, false);

    assertTrue(result.isSuccess());
    verify(mockEsSystemMetadataDAO).deleteByUrn(TEST_URN.toString());
  }

  @Test
  public void testDeleteIndexDocumentsFixAllFailuresReportsFailure() {
    // When ALL index operations fail, the fix reports failure
    doThrow(new RuntimeException("System metadata delete failed"))
        .when(mockEsSystemMetadataDAO)
        .deleteByUrn(anyString());
    doThrow(new RuntimeException("Search delete failed"))
        .when(mockEntitySearchService)
        .deleteDocument(any(), anyString(), anyString());
    doThrow(new RuntimeException("Graph delete failed"))
        .when(mockGraphService)
        .removeNode(any(), any(Urn.class));

    ConsistencyIssue issue =
        ConsistencyIssue.builder()
            .entityUrn(TEST_URN)
            .entityType("assertion")
            .checkId("orphan-check")
            .fixType(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
            .description("Orphan document")
            .build();

    ConsistencyFixDetail result = deleteIndexDocumentsFix.apply(mockOpContext, issue, false);

    // When all indices fail, the operation should report failure
    assertFalse(result.isSuccess());
    assertNotNull(result.getErrorMessage());
  }
}
