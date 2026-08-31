package com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions;

/**
 * Thrown when submitting a reindex task to Elasticsearch fails.
 *
 * <p>This indicates failures during the initial reindex task submission, such as: - Insufficient
 * cluster resources - Invalid reindex parameters - Cluster API errors (non-IO related)
 */
public class ReindexSubmissionException extends BaseReindexException {
  public ReindexSubmissionException(String message) {
    super(message);
  }

  public ReindexSubmissionException(String message, Throwable cause) {
    super(message, cause);
  }

  public ReindexSubmissionException(Throwable cause) {
    super(cause);
  }
}
