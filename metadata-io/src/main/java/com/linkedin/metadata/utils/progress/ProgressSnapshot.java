package com.linkedin.metadata.utils.progress;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Immutable view of progress for logging or persistence. */
@Value
@Builder
public class ProgressSnapshot {

  @Nonnull String label;

  /** Units processed so far (may include resume offset). */
  long processed;

  /**
   * Estimated total units, or empty when unknown. When present, progress percent and ETA may be
   * computed.
   */
  @Nullable Long total;

  /** Overall throughput in units per second since tracker start (0 if no elapsed time). */
  double ratePerSecond;

  /**
   * Estimated remaining time in seconds when total is known, rate &gt; 0, and past warmup;
   * otherwise null.
   */
  @Nullable Long etaSeconds;

  /**
   * Human-readable form of {@link #etaSeconds} (e.g. {@code 1h 2m}, {@code 45s}); null when ETA is
   * unavailable.
   */
  @Nullable String etaHuman;

  /** Progress percent capped at 99 until finished; null when total unknown. */
  @Nullable Integer percentComplete;

  /** Whether this snapshot is considered finished (processed &gt;= total when total known). */
  boolean finished;

  /** Compact single-line message suitable for INFO logs. */
  @Nonnull String message;
}
