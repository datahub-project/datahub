package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Locale;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;

/**
 * Shared accounting for V3 bulk writes. Two item failures on `_v3` indices are expected outcomes,
 * not indexing failures: a whole-document write that conflicts with the document already there (a
 * fill that only creates absent documents), and an update or delete of a document that is absent (a
 * merge or run-id append that must not create one in an index a fill is building, or a delete of an
 * entity the index never held).
 */
public final class V3BulkWriteAccounting {
  public static final String SUCCESSFUL_WRITES_METRIC = "v3_successful_writes";
  public static final String STALE_VERSION_REJECTED_METRIC = "v3_stale_version_rejected";

  private static final String INDEX_OPERATION = "index";
  private static final String CREATE_OPERATION = "create";
  private static final String UPDATE_OPERATION = "update";
  private static final String DELETE_OPERATION = "delete";
  private static final int HTTP_NOT_FOUND = 404;
  private static final int HTTP_CONFLICT = 409;
  private static final LongAdder SUCCESSFUL_V3_WRITES = new LongAdder();
  private static final LongAdder STALE_VERSION_REJECTED = new LongAdder();

  private V3BulkWriteAccounting() {}

  public static Snapshot snapshot() {
    return new Snapshot(SUCCESSFUL_V3_WRITES.sum(), STALE_VERSION_REJECTED.sum());
  }

  public static BulkWriteOutcome recordOpenSearchResponse(
      @Nonnull final BulkResponse response, @Nullable final MetricUtils metricUtils) {
    long successfulV3Writes = 0;
    long staleVersionRejected = 0;
    long missingDocumentSkipped = 0;
    long unexpectedFailures = 0;

    for (BulkItemResponse item : response.getItems()) {
      final String operationType = String.valueOf(item.getOpType());
      final int status = item.status().getStatus();
      if (isSuccessfulV3IndexWrite(item.getIndex(), operationType, status)) {
        successfulV3Writes++;
      } else if (item.isFailed()) {
        if (isExpectedV3StaleConflict(item.getIndex(), operationType, status)) {
          staleVersionRejected++;
        } else if (isExpectedV3MissingDocument(item.getIndex(), operationType, status)) {
          missingDocumentSkipped++;
        } else {
          unexpectedFailures++;
        }
      }
    }

    recordSuccessfulV3Writes(successfulV3Writes, metricUtils);
    recordStaleVersionRejected(staleVersionRejected, metricUtils);
    return new BulkWriteOutcome(
        successfulV3Writes, staleVersionRejected, missingDocumentSkipped, unexpectedFailures);
  }

  public static BulkWriteOutcome recordEs8Response(
      @Nonnull final co.elastic.clients.elasticsearch.core.BulkResponse response,
      @Nullable final MetricUtils metricUtils) {
    long successfulV3Writes = 0;
    long staleVersionRejected = 0;
    long missingDocumentSkipped = 0;
    long unexpectedFailures = 0;

    for (co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem item : response.items()) {
      final String operationType =
          item.operationType() == null ? null : item.operationType().name();
      if (isSuccessfulV3IndexWrite(item.index(), operationType, item.status())) {
        successfulV3Writes++;
      } else if (item.error() != null) {
        if (isExpectedV3StaleConflict(item.index(), operationType, item.status())) {
          staleVersionRejected++;
        } else if (isExpectedV3MissingDocument(item.index(), operationType, item.status())) {
          missingDocumentSkipped++;
        } else {
          unexpectedFailures++;
        }
      }
    }

    recordSuccessfulV3Writes(successfulV3Writes, metricUtils);
    recordStaleVersionRejected(staleVersionRejected, metricUtils);
    return new BulkWriteOutcome(
        successfulV3Writes, staleVersionRejected, missingDocumentSkipped, unexpectedFailures);
  }

  @Nonnull
  public static BulkResponseClassifier.ExpectedItemRejects classifyOpenSearchResponse(
      @Nonnull final BulkResponse response, @Nullable final MetricUtils metricUtils) {
    return expectedRejects(recordOpenSearchResponse(response, metricUtils));
  }

  @Nonnull
  public static BulkResponseClassifier.ExpectedItemRejects classifyEs8Response(
      @Nonnull final co.elastic.clients.elasticsearch.core.BulkResponse response,
      @Nullable final MetricUtils metricUtils) {
    return expectedRejects(recordEs8Response(response, metricUtils));
  }

  @Nonnull
  private static BulkResponseClassifier.ExpectedItemRejects expectedRejects(
      @Nonnull final BulkWriteOutcome outcome) {
    return new BulkResponseClassifier.ExpectedItemRejects(
        outcome.staleVersionRejected(),
        outcome.missingDocumentSkipped(),
        outcome.hasOnlyExpectedFailures());
  }

  public static void recordSuccessfulV3Writes(
      final long count, @Nullable final MetricUtils metricUtils) {
    if (count <= 0) {
      return;
    }
    SUCCESSFUL_V3_WRITES.add(count);
    if (metricUtils != null) {
      metricUtils.increment(BulkListener.class, SUCCESSFUL_WRITES_METRIC, count);
    }
  }

  public static void recordStaleVersionRejected(
      final long count, @Nullable final MetricUtils metricUtils) {
    if (count <= 0) {
      return;
    }
    STALE_VERSION_REJECTED.add(count);
    if (metricUtils != null) {
      metricUtils.increment(BulkListener.class, STALE_VERSION_REJECTED_METRIC, count);
    }
  }

  /** A whole-document V3 write found a document it must not replace. */
  public static boolean isExpectedV3StaleConflict(
      @Nullable final String indexName, @Nullable final String operationType, final int status) {
    return isV3Index(indexName)
        && isWholeDocumentOperation(operationType)
        && status == HTTP_CONFLICT;
  }

  /**
   * A V3 update found no document and was not allowed to create one, or a V3 delete found the
   * document already gone.
   */
  public static boolean isExpectedV3MissingDocument(
      @Nullable final String indexName, @Nullable final String operationType, final int status) {
    return isV3Index(indexName)
        && (isOperation(operationType, UPDATE_OPERATION)
            || isOperation(operationType, DELETE_OPERATION))
        && status == HTTP_NOT_FOUND;
  }

  public static boolean isSuccessfulV3IndexWrite(
      @Nullable final String indexName, @Nullable final String operationType, final int status) {
    // A live merge is an update. A 2xx update is a completed write; a 404 stays a missing document.
    return isV3Index(indexName)
        && (isWholeDocumentOperation(operationType)
            || isOperation(operationType, UPDATE_OPERATION)
            || isOperation(operationType, DELETE_OPERATION))
        && status >= 200
        && status < 300;
  }

  public static void resetForTests() {
    SUCCESSFUL_V3_WRITES.reset();
    STALE_VERSION_REJECTED.reset();
  }

  private static boolean isV3Index(@Nullable final String indexName) {
    return EntityIndexVersion.V3.matches(indexName);
  }

  private static boolean isWholeDocumentOperation(@Nullable final String operationType) {
    return isOperation(operationType, INDEX_OPERATION)
        || isOperation(operationType, CREATE_OPERATION);
  }

  private static boolean isOperation(
      @Nullable final String operationType, @Nonnull final String expected) {
    return operationType != null && expected.equals(operationType.toLowerCase(Locale.ROOT));
  }

  public record Snapshot(long successfulV3Writes, long staleVersionRejected) {
    public Snapshot deltaSince(@Nonnull final Snapshot before) {
      return new Snapshot(
          successfulV3Writes - before.successfulV3Writes,
          staleVersionRejected - before.staleVersionRejected);
    }
  }

  public record BulkWriteOutcome(
      long successfulV3Writes,
      long staleVersionRejected,
      long missingDocumentSkipped,
      long unexpectedFailures) {
    public boolean hasOnlyExpectedFailures() {
      return staleVersionRejected + missingDocumentSkipped > 0 && unexpectedFailures == 0;
    }
  }
}
