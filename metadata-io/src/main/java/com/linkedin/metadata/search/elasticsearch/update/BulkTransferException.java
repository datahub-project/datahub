package com.linkedin.metadata.search.elasticsearch.update;

/** Thrown when flush-and-wait observes unrecovered bulk transfer failures. */
public class BulkTransferException extends RuntimeException {
  private final long failureCount;

  public BulkTransferException(long failureCount, String message) {
    super(message);
    this.failureCount = failureCount;
  }

  public long getFailureCount() {
    return failureCount;
  }
}
