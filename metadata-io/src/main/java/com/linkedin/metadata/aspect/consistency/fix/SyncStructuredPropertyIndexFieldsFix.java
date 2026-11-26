package com.linkedin.metadata.aspect.consistency.fix;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.StructuredPropertyIndexMismatchCheck;
import com.linkedin.metadata.aspect.consistency.check.StructuredPropertyIndexMismatchCheck.FieldChange;
import com.linkedin.metadata.aspect.consistency.check.StructuredPropertyIndexMismatchCheck.TargetDiff;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Writes the current SQL projection onto mapped structured-property fields that differ, and nulls
 * mapped fields the aspect does not project.
 *
 * <p>The issue is not the source of the write. SQL and the index are read again immediately before
 * the update, so a race that has already converged writes nothing. One index failing does not skip
 * the others. A missing search document is left alone: a partial upsert would create a stub that
 * has no {@code urn}.
 */
@Slf4j
@Component("syncStructuredPropertyIndexFieldsFix")
public class SyncStructuredPropertyIndexFieldsFix implements ConsistencyFix {

  private final StructuredPropertyIndexMismatchCheck check;
  private final ElasticSearchService elasticSearchService;

  public SyncStructuredPropertyIndexFieldsFix(
      @Nonnull StructuredPropertyIndexMismatchCheck check,
      @Nonnull ElasticSearchService elasticSearchService) {
    this.check = check;
    this.elasticSearchService = elasticSearchService;
  }

  @Override
  @Nonnull
  public ConsistencyFixType getType() {
    return ConsistencyFixType.SYNC_STRUCTURED_PROPERTY_INDEX_FIELDS;
  }

  @Override
  @Nonnull
  public ConsistencyFixDetail apply(
      @Nonnull OperationContext opContext, @Nonnull ConsistencyIssue issue, boolean dryRun) {
    if (!check.mappingReadsEnabled()) {
      return ConsistencyFixDetail.builder()
          .urn(issue.getEntityUrn())
          .action(getType())
          .success(true)
          .details("skipped: elasticsearch.index.structuredPropertyRestateEnabled is false")
          .build();
    }
    List<TargetDiff> diffs;
    try {
      diffs = check.recompute(opContext, issue.getEntityUrn());
    } catch (Exception e) {
      log.error(
          "Failed to recompute structured properties for {}: {}",
          issue.getEntityUrn(),
          e.getMessage(),
          e);
      return failed(issue, e.getMessage());
    }
    if (diffs.isEmpty()) {
      return ConsistencyFixDetail.builder()
          .urn(issue.getEntityUrn())
          .action(getType())
          .success(true)
          .details("no differing mapped fields")
          .build();
    }

    List<String> updated = new ArrayList<>();
    List<String> failed = new ArrayList<>();
    int set = 0;
    int cleared = 0;
    for (TargetDiff diff : diffs) {
      if (diff.error() != null) {
        failed.add(diff.indexName() + ": " + diff.error());
        continue;
      }
      FieldChange change = diff.change();
      if (change == null || change.isEmpty()) {
        continue;
      }
      try {
        if (!dryRun) {
          elasticSearchService.upsertDocumentByIndexName(
              opContext, diff.indexName(), partialDocument(change), diff.documentId());
        }
        updated.add(diff.indexName());
        set += change.missing() + change.different();
        cleared += change.extra();
      } catch (Exception e) {
        failed.add(diff.indexName() + ": " + e.getMessage());
        log.warn(
            "Failed to sync structured properties on {} for {}: {}",
            diff.indexName(),
            issue.getEntityUrn(),
            e.getMessage());
      }
    }
    String details =
        String.format(
            "set %d, null %d on %s%s",
            set,
            cleared,
            updated.isEmpty() ? "no index" : String.join(", ", updated),
            failed.isEmpty() ? "" : "; failed " + String.join("; ", failed));
    if (!failed.isEmpty()) {
      return ConsistencyFixDetail.builder()
          .urn(issue.getEntityUrn())
          .action(getType())
          .success(false)
          .errorMessage(String.join("; ", failed))
          .details(details)
          .build();
    }
    return ConsistencyFixDetail.builder()
        .urn(issue.getEntityUrn())
        .action(getType())
        .success(true)
        .details(details)
        .build();
  }

  @Nonnull
  private ConsistencyFixDetail failed(@Nonnull ConsistencyIssue issue, @Nullable String message) {
    return ConsistencyFixDetail.builder()
        .urn(issue.getEntityUrn())
        .action(getType())
        .success(false)
        .errorMessage(message)
        .build();
  }

  /** Dotted structured-property keys only. Matching and unrelated fields are omitted. */
  @Nonnull
  static String partialDocument(@Nonnull FieldChange change) {
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    for (var entry : change.values().entrySet()) {
      document.set(entry.getKey(), entry.getValue());
    }
    for (String field : change.clear()) {
      document.putNull(field);
    }
    return document.toString();
  }
}
