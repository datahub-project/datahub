package com.linkedin.metadata.pgqueue;

/**
 * Exponential idle backoff for empty pgQueue polls. First empty poll sleeps {@code minMillis}; each
 * subsequent empty poll doubles, capped at {@code maxMillis}. {@link #reset()} after a non-empty
 * poll.
 */
public final class PgQueueEmptyPollBackoff {

  private final long minMillis;
  private final long maxMillis;
  private long currentMillis;

  public PgQueueEmptyPollBackoff(long minMillis, long maxMillis) {
    if (minMillis < 1) {
      throw new IllegalArgumentException("empty poll min sleep must be >= 1, got " + minMillis);
    }
    if (maxMillis < 1) {
      throw new IllegalArgumentException("empty poll max sleep must be >= 1, got " + maxMillis);
    }
    this.maxMillis = maxMillis;
    this.minMillis = Math.min(minMillis, maxMillis);
    this.currentMillis = this.minMillis;
  }

  /** Current idle interval without growing it. */
  public long peekSleepMillis() {
    return currentMillis;
  }

  /** Sleep for this empty poll, then grow the next interval. */
  public long nextSleepMillis() {
    long sleep = currentMillis;
    currentMillis = multiplyCapped(currentMillis);
    return sleep;
  }

  public void reset() {
    currentMillis = minMillis;
  }

  private long multiplyCapped(long value) {
    if (value >= maxMillis) {
      return maxMillis;
    }
    if (value > Long.MAX_VALUE / 2) {
      return maxMillis;
    }
    return Math.min(maxMillis, value * 2);
  }
}
