package com.linkedin.metadata.aspect.consistency.fix;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Fix implementation that deletes orphaned documents directly from indices.
 *
 * <p>This fix is used when entities exist in ES indices (system metadata, entity search, graph) but
 * NOT in SQL. These are orphaned documents that cannot be cleaned up via normal MCP operations.
 *
 * <p>Deletion order is important - system metadata is deleted LAST because it's how orphans are
 * detected. If we delete system metadata first and other deletions fail, we lose the ability to
 * detect and retry the cleanup.
 *
 * <p>Deletes from (in order):
 *
 * <ol>
 *   <li>Entity Search Index - via EntitySearchService.deleteDocument()
 *   <li>Graph Index - via GraphService.removeNode()
 *   <li>System Metadata Index - via ESSystemMetadataDAO.deleteByUrn() (LAST)
 * </ol>
 *
 * <p>Uses the following fields from Issue:
 *
 * <ul>
 *   <li>hardDeleteUrns - List of URNs to delete from indices (if provided)
 *   <li>entityUrn - Falls back to single entity URN if hardDeleteUrns is empty
 *   <li>entityType - Used to identify the correct search index
 * </ul>
 */
@Slf4j
@Component
public class DeleteIndexDocumentsFix implements ConsistencyFix {

  private final ESSystemMetadataDAO esSystemMetadataDAO;
  private final EntitySearchService entitySearchService;
  private final GraphService graphService;

  public DeleteIndexDocumentsFix(
      @Qualifier("esSystemMetadataDAO") ESSystemMetadataDAO esSystemMetadataDAO,
      @Qualifier("entitySearchService") EntitySearchService entitySearchService,
      @Qualifier("graphService") GraphService graphService) {
    this.esSystemMetadataDAO = esSystemMetadataDAO;
    this.entitySearchService = entitySearchService;
    this.graphService = graphService;
  }

  @Override
  @Nonnull
  public ConsistencyFixType getType() {
    return ConsistencyFixType.DELETE_INDEX_DOCUMENTS;
  }

  @Override
  @Nonnull
  public ConsistencyFixDetail apply(
      @Nonnull OperationContext opContext, @Nonnull ConsistencyIssue issue, boolean dryRun) {
    Urn primaryUrn = issue.getEntityUrn();
    String entityType = issue.getEntityType();

    // Get URNs to delete - use hardDeleteUrns if provided, otherwise fall back to entityUrn
    List<Urn> urnsToDelete =
        (issue.getHardDeleteUrns() != null && !issue.getHardDeleteUrns().isEmpty())
            ? issue.getHardDeleteUrns()
            : List.of(primaryUrn);

    List<String> deletedUrns = new ArrayList<>();
    List<String> failedUrns = new ArrayList<>();

    for (Urn urnToDelete : urnsToDelete) {
      try {
        if (!dryRun) {
          deleteFromAllIndices(opContext, urnToDelete, entityType);
        }
        deletedUrns.add(urnToDelete.toString());
        log.info(
            "[{}] DELETE_INDEX_DOCUMENTS urn={} entityType={}",
            dryRun ? "DRY-RUN" : "APPLY",
            urnToDelete,
            entityType);
      } catch (Exception e) {
        failedUrns.add(urnToDelete.toString());
        log.error(
            "Failed to delete index documents for {}: {}. Continuing with remaining entities.",
            urnToDelete,
            e.getMessage(),
            e);
      }
    }

    // Build result based on success/failure counts
    return buildResult(primaryUrn, deletedUrns, failedUrns);
  }

  /**
   * Delete the entity from all indices.
   *
   * <p>Attempts to delete from all three indices. Individual index failures are logged as warnings
   * and don't prevent attempts on other indices. If ALL indices fail, throws an exception so the
   * URN is marked as failed.
   *
   * <p><b>Important:</b> System metadata is deleted LAST because it's how orphans are detected. If
   * we delete it first and other deletions fail, we lose the ability to detect and retry.
   *
   * @param opContext operation context
   * @param urn URN to delete
   * @param entityType entity type for search index
   * @throws RuntimeException if all index deletions fail
   */
  void deleteFromAllIndices(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String entityType) {

    String urnStr = urn.toString();
    // Document ID in search index is URL-encoded
    String docId = opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn);
    int successCount = 0;
    Exception lastException = null;

    // 1. Delete from entity search index FIRST
    try {
      entitySearchService.deleteDocument(opContext, entityType, docId);
      log.debug(
          "Deleted from entity search index: {} (type={}, docId={})", urnStr, entityType, docId);
      successCount++;
    } catch (Exception e) {
      log.warn(
          "Failed to delete from entity search index {} (type={}, docId={}): {}",
          urnStr,
          entityType,
          docId,
          e.getMessage());
      lastException = e;
    }

    // 2. Delete from graph index (removes node and all edges)
    try {
      graphService.removeNode(opContext, urn);
      log.debug("Deleted from graph index: {}", urnStr);
      successCount++;
    } catch (Exception e) {
      log.warn("Failed to delete from graph index {}: {}", urnStr, e.getMessage());
      lastException = e;
    }

    // 3. Delete from system metadata index LAST
    // This is intentionally last because system metadata is how orphans are detected.
    // If we delete it first and other deletions fail, we can't detect and retry.
    try {
      esSystemMetadataDAO.deleteByUrn(urnStr);
      log.debug("Deleted from system metadata index: {}", urnStr);
      successCount++;
    } catch (Exception e) {
      log.warn("Failed to delete from system metadata index {}: {}", urnStr, e.getMessage());
      lastException = e;
    }

    // If all indices failed, throw so the URN is marked as failed
    if (successCount == 0 && lastException != null) {
      throw new RuntimeException("Failed to delete from all indices for " + urnStr, lastException);
    }
  }

  /**
   * Build the fix result based on success/failure counts.
   *
   * @param primaryUrn the primary URN for the issue
   * @param deletedUrns successfully deleted URNs
   * @param failedUrns failed URNs
   * @return fix detail
   */
  private ConsistencyFixDetail buildResult(
      @Nonnull Urn primaryUrn, List<String> deletedUrns, List<String> failedUrns) {

    if (failedUrns.isEmpty()) {
      // Complete success
      String details =
          deletedUrns.size() == 1
              ? "Deleted index documents for " + deletedUrns.get(0)
              : String.format(
                  "Deleted index documents for %d entities: %s",
                  deletedUrns.size(), String.join(", ", deletedUrns));
      return ConsistencyFixDetail.builder()
          .urn(primaryUrn)
          .action(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
          .success(true)
          .details(details)
          .build();
    } else if (deletedUrns.isEmpty()) {
      // Complete failure
      return ConsistencyFixDetail.builder()
          .urn(primaryUrn)
          .action(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
          .success(false)
          .errorMessage(
              String.format(
                  "Failed to delete index documents for %d entities: %s",
                  failedUrns.size(), String.join(", ", failedUrns)))
          .build();
    } else {
      // Partial success
      return ConsistencyFixDetail.builder()
          .urn(primaryUrn)
          .action(ConsistencyFixType.DELETE_INDEX_DOCUMENTS)
          .success(false)
          .details(
              String.format(
                  "Partial success: Deleted index documents for %d entities: %s",
                  deletedUrns.size(), String.join(", ", deletedUrns)))
          .errorMessage(
              String.format(
                  "Failed to delete index documents for %d entities: %s",
                  failedUrns.size(), String.join(", ", failedUrns)))
          .build();
    }
  }
}
