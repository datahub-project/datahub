package com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;

/**
 * Thrown when a reindex operation exceeds its configured timeout.
 *
 * <p>This indicates that the reindex task did not complete within the allowed time window. Could
 * indicate: - Cluster is overloaded or slow - Reindex rate limiting is too aggressive - Cluster
 * health issues preventing progress
 */
public class ReindexTimeoutException extends BaseReindexException {
  private final long timeoutSeconds;

  public ReindexTimeoutException(String message) {
    super(message);
    this.timeoutSeconds = 0;
  }

  public ReindexTimeoutException(String message, long timeoutSeconds) {
    super(message);
    this.timeoutSeconds = timeoutSeconds;
  }

  public ReindexTimeoutException(String message, Throwable cause) {
    super(message, cause);
    this.timeoutSeconds = 0;
  }

  public long getTimeoutSeconds() {
    return timeoutSeconds;
  }

  public ReindexResult getFailureResult() {
    return ReindexResult.FAILED_TIMEOUT;
  }
}
