package com.linkedin.metadata.pgqueue;

import com.linkedin.metadata.queue.QueueReceivedMessage;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Buffers {@link QueueReceivedMessage}s across poll iterations and signals when the accumulated
 * batch should be flushed. A flush is triggered when <em>any</em> of three thresholds is reached:
 *
 * <ul>
 *   <li><b>Message count</b> — buffer contains {@code maxMessages} messages
 *   <li><b>Payload bytes</b> — cumulative raw payload bytes reach {@code maxBytes}
 *   <li><b>Age / linger</b> — the oldest message in the buffer has been waiting {@code maxAgeMs}
 *       milliseconds
 * </ul>
 *
 * <p>This class is <em>not</em> thread-safe; it is designed to be used by a single {@link
 * PgQueuePollWorker} thread.
 */
public final class PgQueueBatchAccumulator {

  private final int maxMessages;
  private final long maxBytes;
  private final long maxAgeMs;
  private final Clock clock;

  private final List<QueueReceivedMessage> buffer = new ArrayList<>();
  private long currentBytes;
  private long oldestMessageTimeMs;

  public PgQueueBatchAccumulator(int maxMessages, long maxBytes, long maxAgeMs) {
    this(maxMessages, maxBytes, maxAgeMs, Clock.systemUTC());
  }

  /** Visible for testing — allows injecting a controllable clock. */
  public PgQueueBatchAccumulator(
      int maxMessages, long maxBytes, long maxAgeMs, @Nonnull Clock clock) {
    if (maxMessages < 1) {
      throw new IllegalArgumentException("maxMessages must be >= 1, got " + maxMessages);
    }
    if (maxBytes < 1) {
      throw new IllegalArgumentException("maxBytes must be >= 1, got " + maxBytes);
    }
    if (maxAgeMs < 1) {
      throw new IllegalArgumentException("maxAgeMs must be >= 1, got " + maxAgeMs);
    }
    this.maxMessages = maxMessages;
    this.maxBytes = maxBytes;
    this.maxAgeMs = maxAgeMs;
    this.clock = clock;
  }

  /** Appends messages to the buffer. */
  public void addAll(@Nonnull List<QueueReceivedMessage> messages) {
    if (messages.isEmpty()) {
      return;
    }
    if (buffer.isEmpty()) {
      oldestMessageTimeMs = clock.millis();
    }
    for (QueueReceivedMessage msg : messages) {
      buffer.add(msg);
      currentBytes += msg.payload().length;
    }
  }

  /**
   * Returns {@code true} when any threshold (count, bytes, or age) is met and the buffer is
   * non-empty.
   */
  public boolean shouldFlush() {
    if (buffer.isEmpty()) {
      return false;
    }
    return buffer.size() >= maxMessages || currentBytes >= maxBytes || isExpired();
  }

  /** Returns {@code true} when the buffer is non-empty and the age threshold has been exceeded. */
  public boolean isExpired() {
    return !buffer.isEmpty() && (clock.millis() - oldestMessageTimeMs) >= maxAgeMs;
  }

  /** Drains the buffer and resets all counters. */
  @Nonnull
  public List<QueueReceivedMessage> drain() {
    List<QueueReceivedMessage> result = new ArrayList<>(buffer);
    buffer.clear();
    currentBytes = 0;
    oldestMessageTimeMs = 0;
    return result;
  }

  public boolean isEmpty() {
    return buffer.isEmpty();
  }

  public int messageCount() {
    return buffer.size();
  }

  public long byteCount() {
    return currentBytes;
  }
}
