package com.linkedin.metadata.utils.elasticsearch;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Accounting for operations captured by a bulk processor flush boundary. */
public record BulkProcessorFlushResult(
    long successfulV3Writes,
    long staleVersionRejected,
    long unexpectedItemFailures,
    long transportFailedOperations,
    long incompleteOperations,
    boolean timedOut,
    @Nullable Throwable flushException) {

  @Nonnull
  public static BulkProcessorFlushResult empty() {
    return new BulkProcessorFlushResult(0, 0, 0, 0, 0, false, null);
  }

  @Nonnull
  public BulkProcessorFlushResult plus(@Nonnull final BulkProcessorFlushResult other) {
    return new BulkProcessorFlushResult(
        successfulV3Writes + other.successfulV3Writes,
        staleVersionRejected + other.staleVersionRejected,
        unexpectedItemFailures + other.unexpectedItemFailures,
        transportFailedOperations + other.transportFailedOperations,
        incompleteOperations + other.incompleteOperations,
        timedOut || other.timedOut,
        flushException != null ? flushException : other.flushException);
  }

  public boolean hasFailures() {
    return unexpectedItemFailures > 0
        || transportFailedOperations > 0
        || incompleteOperations > 0
        || timedOut
        || flushException != null;
  }
}
