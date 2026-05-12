package com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;

/**
 * Thrown when document count validation fails during index finalization.
 *
 * <p>This indicates that the reindexed index does not contain the expected number of documents
 * within the configured tolerance (default 0.1%). This could indicate: - Data loss during
 * reindexing - Incomplete reindex operation - Timing issues with document refresh
 */
public class DocCountMismatchException extends BaseReindexException {
  private final long expectedCount;
  private final long actualCount;

  public DocCountMismatchException(String message) {
    super(message);
    this.expectedCount = 0;
    this.actualCount = 0;
  }

  public DocCountMismatchException(String message, long expectedCount, long actualCount) {
    super(message);
    this.expectedCount = expectedCount;
    this.actualCount = actualCount;
  }

  public DocCountMismatchException(String message, Throwable cause) {
    super(message, cause);
    this.expectedCount = 0;
    this.actualCount = 0;
  }

  public long getExpectedCount() {
    return expectedCount;
  }

  public long getActualCount() {
    return actualCount;
  }

  public ReindexResult getFailureResult() {
    return ReindexResult.FAILED_DOC_COUNT_MISMATCH;
  }
}
