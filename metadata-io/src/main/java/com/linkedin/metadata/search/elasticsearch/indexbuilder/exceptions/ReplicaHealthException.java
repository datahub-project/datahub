package com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;

/**
 * Thrown when replica synchronization or health checks fail during index finalization.
 *
 * <p>This indicates failures in: - Restoring replicas to minimum required count - Waiting for index
 * to reach GREEN health status - Replica allocation or shard initialization
 *
 * <p>These failures block index promotion to prevent data loss.
 */
public class ReplicaHealthException extends BaseReindexException {
  public ReplicaHealthException(String message) {
    super(message);
  }

  public ReplicaHealthException(String message, Throwable cause) {
    super(message, cause);
  }

  public ReplicaHealthException(Throwable cause) {
    super(cause);
  }

  public ReindexResult getFailureResult() {
    return ReindexResult.FAILED_REPLICA_HEALTH;
  }
}
