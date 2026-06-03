package com.linkedin.metadata.pgqueue;

/**
 * Thresholds that govern when a {@link PgQueueBatchAccumulator} flushes. A flush fires when
 * <em>any</em> limit is reached.
 *
 * @param maxMessages maximum number of messages before flush
 * @param maxBytes maximum cumulative raw payload bytes before flush
 * @param maxAgeMs maximum linger time (ms) for the oldest buffered message before flush
 */
public record PgQueueBatchPolicy(int maxMessages, long maxBytes, long maxAgeMs) {

  public PgQueueBatchPolicy {
    if (maxMessages < 1) {
      throw new IllegalArgumentException("maxMessages must be >= 1");
    }
    if (maxBytes < 1) {
      throw new IllegalArgumentException("maxBytes must be >= 1");
    }
    if (maxAgeMs < 1) {
      throw new IllegalArgumentException("maxAgeMs must be >= 1");
    }
  }
}
