package com.linkedin.metadata.search.elasticsearch.indexbuilder;

/**
 * Result status of a reindex operation.
 *
 * <p>Values reflect the outcome of a reindex attempt: whether it succeeded, was skipped, or failed
 * due to timeouts, document mismatches, or operational errors. Includes intermediate states
 * (REINDEXING, REINDEXED_WITH_FAILURES) for partial success scenarios.
 */
public enum ReindexResult {
  CREATED_NEW(false),

  // mappings/settings didnt require reindex
  NOT_REINDEXED_NOTHING_APPLIED(false),

  // no reindex, but mappings/settings were applied
  NOT_REQUIRED_MAPPINGS_SETTINGS_APPLIED(false),

  // reindexing already ongoing
  REINDEXING_ALREADY(false),

  // reindexing skipped, 0 docs
  REINDEXED_SKIPPED_0DOCS(false),

  // reindex launched
  REINDEXING(false),

  // reindex completed successfully
  REINDEXED(false),

  // reindex completed with some failures
  REINDEXED_WITH_FAILURES(false),

  // reindex failed due to timeout
  FAILED_TIMEOUT(true),

  // reindex failed due to document count mismatch
  FAILED_DOC_COUNT_MISMATCH(true),

  // reindex failed due to replica health check failure
  FAILED_REPLICA_HEALTH(true),

  // reindex failed due to monitoring error
  FAILED_MONITORING_ERROR(true),

  // reindex failed during task submission
  FAILED_SUBMISSION(true),

  // reindex failed due to IO error during submission
  FAILED_SUBMISSION_IO(true);

  private final boolean failure;

  ReindexResult(boolean failure) {
    this.failure = failure;
  }

  /**
   * Indicates whether this result represents a failure state.
   *
   * @return true if the reindex operation failed, false otherwise
   */
  public boolean isFailure() {
    return failure;
  }
}
