package com.linkedin.metadata.aspect.consistency.fix;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Fix implementation that processes BatchItems from an Issue.
 *
 * <p>This fix handles all MCP-based operations:
 *
 * <ul>
 *   <li>CREATE: ChangeItemImpl with CREATE changeType
 *   <li>UPSERT: ChangeItemImpl with UPSERT changeType
 *   <li>PATCH: PatchItemImpl
 *   <li>SOFT_DELETE: ChangeItemImpl setting status.removed=true
 *   <li>DELETE_ASPECT: DeleteItemImpl
 * </ul>
 *
 * <p>The fix separates items by type:
 *
 * <ul>
 *   <li>ChangeItemImpl/PatchItemImpl are processed via entityService.ingestProposal()
 *   <li>DeleteItemImpl items are processed via entityService.deleteAspect()
 * </ul>
 *
 * <p><b>Conditional Writes:</b> BatchItems should include conditional write headers ({@code
 * If-Version-Match}) set by the check when creating the items via {@link
 * AbstractEntityCheck#createUpsertItem}. This ensures we don't overwrite data that has changed
 * since the issue was detected.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchItemsFix implements ConsistencyFix {

  /** Fix types that this implementation handles */
  public static final Set<ConsistencyFixType> SUPPORTED_TYPES =
      EnumSet.of(
          ConsistencyFixType.CREATE,
          ConsistencyFixType.UPSERT,
          ConsistencyFixType.PATCH,
          ConsistencyFixType.SOFT_DELETE,
          ConsistencyFixType.DELETE_ASPECT);

  private final EntityService<?> entityService;

  /**
   * Returns UPSERT as the primary type, but this fix handles multiple types. Use {@link
   * #SUPPORTED_TYPES} to check if a type is supported.
   */
  @Override
  @Nonnull
  public ConsistencyFixType getType() {
    return ConsistencyFixType.UPSERT;
  }

  /**
   * Check if this fix supports the given type.
   *
   * @param type the fix type to check
   * @return true if supported
   */
  public boolean supports(ConsistencyFixType type) {
    return SUPPORTED_TYPES.contains(type);
  }

  @Override
  @Nonnull
  public ConsistencyFixDetail apply(
      @Nonnull OperationContext opContext, @Nonnull ConsistencyIssue issue, boolean dryRun) {
    Urn entityUrn = issue.getEntityUrn();
    ConsistencyFixType fixType = issue.getFixType();
    List<BatchItem> batchItems = issue.getBatchItems();

    // Validate that we have batch items
    if (batchItems == null || batchItems.isEmpty()) {
      return ConsistencyFixDetail.builder()
          .urn(entityUrn)
          .action(fixType)
          .success(false)
          .errorMessage(fixType + " fix requires batchItems")
          .build();
    }

    // Separate delete items from other items
    List<DeleteItemImpl> deleteItems =
        batchItems.stream()
            .filter(item -> item instanceof DeleteItemImpl)
            .map(item -> (DeleteItemImpl) item)
            .collect(Collectors.toList());

    List<BatchItem> ingestItems =
        batchItems.stream()
            .filter(item -> !(item instanceof DeleteItemImpl))
            .collect(Collectors.toList());

    List<String> details = new ArrayList<>();
    List<String> errors = new ArrayList<>();
    boolean hasFailure = false;

    // Process ingest items (ChangeItemImpl, PatchItemImpl) via ingestProposal
    // Note: Conditional write headers should already be set by the check when creating items
    if (!ingestItems.isEmpty()) {
      try {
        if (!dryRun) {
          AspectsBatchImpl aspectsBatch =
              AspectsBatchImpl.builder()
                  .retrieverContext(opContext.getRetrieverContext())
                  .items(ingestItems)
                  .build(opContext);

          entityService.ingestProposal(opContext, aspectsBatch, false);
        }

        String ingestSummary =
            String.format(
                "Ingested %d item(s): %s",
                ingestItems.size(),
                ingestItems.stream()
                    .map(item -> item.getAspectName())
                    .distinct()
                    .collect(Collectors.joining(", ")));
        details.add(ingestSummary);
        log.info(
            "[{}] {} entity={} {}",
            dryRun ? "DRY-RUN" : "APPLY",
            fixType,
            entityUrn,
            ingestSummary);
      } catch (Exception e) {
        hasFailure = true;
        String errorMsg = String.format("Failed to ingest items: %s", e.getMessage());
        errors.add(errorMsg);
        log.error(
            "Failed to ingest {} item(s) for {} fix on {}: {}",
            ingestItems.size(),
            fixType,
            entityUrn,
            e.getMessage(),
            e);
      }
    }

    // Process delete items via deleteAspect - each individually to allow partial success
    for (DeleteItemImpl deleteItem : deleteItems) {
      try {
        if (!dryRun) {
          entityService.deleteAspect(
              opContext, deleteItem.getUrn().toString(), deleteItem.getAspectName(), null, true);
        }

        String deleteSummary =
            String.format(
                "Deleted aspect %s from %s", deleteItem.getAspectName(), deleteItem.getUrn());
        details.add(deleteSummary);
        log.info("[{}] DELETE_ASPECT {}", dryRun ? "DRY-RUN" : "APPLY", deleteSummary);
      } catch (Exception e) {
        hasFailure = true;
        String errorMsg =
            String.format(
                "Failed to delete aspect %s from %s: %s",
                deleteItem.getAspectName(), deleteItem.getUrn(), e.getMessage());
        errors.add(errorMsg);
        log.error(
            "Failed to delete aspect {} from {}: {}. Continuing with remaining deletes.",
            deleteItem.getAspectName(),
            deleteItem.getUrn(),
            e.getMessage(),
            e);
      }
    }

    // Build result - partial success if some operations succeeded and some failed
    String allDetails = String.join("; ", details);
    if (hasFailure) {
      String allErrors = String.join("; ", errors);
      if (!details.isEmpty()) {
        // Partial success
        return ConsistencyFixDetail.builder()
            .urn(entityUrn)
            .action(fixType)
            .success(false)
            .details("Partial success: " + allDetails)
            .errorMessage(allErrors)
            .build();
      } else {
        // Complete failure
        return ConsistencyFixDetail.builder()
            .urn(entityUrn)
            .action(fixType)
            .success(false)
            .errorMessage(allErrors)
            .build();
      }
    }

    return ConsistencyFixDetail.builder()
        .urn(entityUrn)
        .action(fixType)
        .success(true)
        .details(allDetails)
        .build();
  }
}
