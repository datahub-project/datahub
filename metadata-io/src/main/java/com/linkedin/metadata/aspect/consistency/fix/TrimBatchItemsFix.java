package com.linkedin.metadata.aspect.consistency.fix;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.transform.filter.request.MaskTree;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.restli.internal.server.util.RestUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Fix implementation that trims unknown fields from RecordTemplates before upserting.
 *
 * <p>This fix preprocesses BatchItems by:
 *
 * <ol>
 *   <li>Deserializing the RecordTemplate from each ChangeItem
 *   <li>Trimming unknown fields using {@link RestUtils#trimRecordTemplate}
 *   <li>Rebuilding the ChangeItem with the trimmed aspect (preserving headers)
 *   <li>Delegating to {@link BatchItemsFix} for actual ingestion
 * </ol>
 *
 * <p>Use this fix when data may contain unknown fields that need to be cleaned up before
 * persisting, following the same pattern as {@link
 * com.linkedin.metadata.aspect.hooks.IgnoreUnknownMutator}.
 *
 * <p><b>Conditional Writes:</b> BatchItems should include conditional write headers ({@code
 * If-Version-Match}) set by the check when creating items. These headers are preserved when
 * rebuilding items with trimmed content.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TrimBatchItemsFix implements ConsistencyFix {

  private final BatchItemsFix batchItemsFix;

  @Override
  @Nonnull
  public ConsistencyFixType getType() {
    return ConsistencyFixType.TRIM_UPSERT;
  }

  @Override
  @Nonnull
  public ConsistencyFixDetail apply(
      @Nonnull OperationContext opContext, @Nonnull ConsistencyIssue issue, boolean dryRun) {
    Urn entityUrn = issue.getEntityUrn();
    List<BatchItem> batchItems = issue.getBatchItems();

    // Validate that we have batch items
    if (batchItems == null || batchItems.isEmpty()) {
      return ConsistencyFixDetail.builder()
          .urn(entityUrn)
          .action(ConsistencyFixType.TRIM_UPSERT)
          .success(false)
          .errorMessage("TRIM_UPSERT fix requires batchItems")
          .build();
    }

    // Trim unknown fields from each ChangeItem
    List<BatchItem> trimmedItems = new ArrayList<>();
    int trimCount = 0;

    for (BatchItem item : batchItems) {
      if (item instanceof ChangeMCP) {
        ChangeMCP changeItem = (ChangeMCP) item;
        RecordTemplate recordTemplate = changeItem.getRecordTemplate();

        if (recordTemplate != null) {
          // Trim unknown fields from the record template
          RestUtils.trimRecordTemplate(recordTemplate, new MaskTree(), false);
          trimCount++;

          // Rebuild the ChangeItem with the trimmed aspect
          if (item instanceof ChangeItemImpl) {
            ChangeItemImpl originalItem = (ChangeItemImpl) item;
            ChangeItemImpl trimmedItem =
                originalItem.toBuilder()
                    .metadataChangeProposal(null) // Clear cached MCP so it's rebuilt
                    .build(opContext.getRetrieverContext().getAspectRetriever());

            // Update the MCP with the trimmed aspect
            trimmedItem
                .getMetadataChangeProposal()
                .setAspect(GenericRecordUtils.serializeAspect(recordTemplate));

            trimmedItems.add(trimmedItem);
            log.debug(
                "Trimmed aspect {} on entity {}", changeItem.getAspectName(), changeItem.getUrn());
          } else {
            // For other ChangeMCP types, update the MCP directly
            changeItem
                .getMetadataChangeProposal()
                .setAspect(GenericRecordUtils.serializeAspect(recordTemplate));
            trimmedItems.add(item);
          }
        } else {
          trimmedItems.add(item);
        }
      } else {
        // Non-ChangeMCP items (like DeleteItemImpl) pass through unchanged
        trimmedItems.add(item);
      }
    }

    log.info(
        "[{}] TRIM_UPSERT entity={} trimmed {} of {} items",
        dryRun ? "DRY-RUN" : "APPLY",
        entityUrn,
        trimCount,
        batchItems.size());

    // Create a new issue with the trimmed items and delegate to BatchItemsFix
    ConsistencyIssue trimmedIssue =
        ConsistencyIssue.builder()
            .checkId(issue.getCheckId())
            .entityUrn(entityUrn)
            .entityType(issue.getEntityType())
            .fixType(ConsistencyFixType.UPSERT) // Use UPSERT for the underlying fix
            .description(issue.getDescription())
            .details(issue.getDetails())
            .relatedUrns(issue.getRelatedUrns())
            .batchItems(trimmedItems)
            .build();

    // Delegate to BatchItemsFix for actual ingestion
    ConsistencyFixDetail result = batchItemsFix.apply(opContext, trimmedIssue, dryRun);

    // Return result with TRIM_UPSERT action type and add trim info to details
    String trimDetails =
        String.format(
            "Trimmed %d item(s). %s",
            trimCount, result.getDetails() != null ? result.getDetails() : "");

    return ConsistencyFixDetail.builder()
        .urn(result.getUrn())
        .action(ConsistencyFixType.TRIM_UPSERT)
        .success(result.isSuccess())
        .details(trimDetails.trim())
        .errorMessage(result.getErrorMessage())
        .build();
  }
}
