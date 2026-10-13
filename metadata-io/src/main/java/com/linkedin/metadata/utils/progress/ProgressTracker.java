package com.linkedin.metadata.utils.progress;

import java.time.Clock;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;

/**
 * Tracks processed work units and estimates remaining time for long-running scans.
 *
 * <p>ETA is only available when a total is known, throughput is positive, and warmup has elapsed.
 * Reporting is time-throttled so callers may invoke {@link #maybeReport} every batch without
 * flooding logs.
 *
 * <p>Rate and ETA use only work completed since tracker construction ({@link #record} deltas), not
 * {@code initialProcessed}, so resume after checkpoint does not inflate throughput.
 *
 * <p>{@link #snapshot()} is always cheap and suitable for silent checkpoint persistence.
 */
public class ProgressTracker {

  public static final long DEFAULT_REPORT_INTERVAL_MS = 60_000L;
  public static final long DEFAULT_WARMUP_MS = 30_000L;

  private final String label;
  @Nullable private final Long total;
  private final long reportIntervalMs;
  private final long warmupMs;
  private final Clock clock;
  private final long startTimeMs;
  private final long rateBaselineProcessed;

  private long processed;
  private long lastReportTimeMs;
  private boolean hasReported;

  @Builder
  private ProgressTracker(
      @Nonnull String label,
      @Nullable Long total,
      long initialProcessed,
      long reportIntervalMs,
      @Nullable Long warmupMs,
      @Nullable Clock clock) {
    this.label = label;
    this.total = total != null && total < 0 ? null : total;
    this.processed = Math.max(0, initialProcessed);
    this.rateBaselineProcessed = this.processed;
    this.reportIntervalMs = reportIntervalMs > 0 ? reportIntervalMs : DEFAULT_REPORT_INTERVAL_MS;
    if (warmupMs == null) {
      this.warmupMs = DEFAULT_WARMUP_MS;
    } else if (warmupMs < 0) {
      this.warmupMs = DEFAULT_WARMUP_MS;
    } else {
      this.warmupMs = warmupMs;
    }
    this.clock = clock != null ? clock : Clock.systemUTC();
    this.startTimeMs = this.clock.millis();
    this.lastReportTimeMs = 0L;
    this.hasReported = false;
  }

  /** Record additional completed work units. */
  public void record(long delta) {
    if (delta > 0) {
      processed += delta;
    }
  }

  public long getProcessed() {
    return processed;
  }

  @Nonnull
  public Optional<Long> getTotal() {
    return Optional.ofNullable(total);
  }

  /**
   * Build a snapshot for persistence or logging. Does not apply throttling and does not side-effect
   * report state.
   */
  @Nonnull
  public ProgressSnapshot snapshot() {
    long now = clock.millis();
    long elapsedMs = Math.max(0L, now - startTimeMs);
    double elapsedSec = elapsedMs / 1000.0;
    long processedSinceStart = Math.max(0L, processed - rateBaselineProcessed);
    double rate = elapsedSec > 0 ? processedSinceStart / elapsedSec : 0.0;

    boolean finished = total != null && processed >= total;
    Integer percent = null;
    Long etaSeconds = null;

    if (total != null) {
      if (total == 0 || finished) {
        percent = 100;
      } else {
        percent = (int) Math.min(99, (processed * 100L) / total);
      }
    }

    boolean pastWarmup = elapsedMs >= warmupMs;
    if (total != null && total > 0 && rate > 0 && pastWarmup && !finished) {
      long remaining = Math.max(0L, total - processed);
      etaSeconds = (long) Math.ceil(remaining / rate);
    }

    return ProgressSnapshot.builder()
        .label(label)
        .processed(processed)
        .total(total)
        .ratePerSecond(rate)
        .etaSeconds(etaSeconds)
        .etaHuman(etaSeconds != null ? formatDuration(etaSeconds) : null)
        .percentComplete(percent)
        .finished(finished)
        .message(formatMessage(processed, total, percent, rate, etaSeconds, finished))
        .build();
  }

  /**
   * Invoke {@code reporter} with a fresh snapshot when throttling allows.
   *
   * @return true if a report was emitted
   */
  public boolean maybeReport(@Nonnull Consumer<ProgressSnapshot> reporter) {
    long now = clock.millis();
    long elapsedMs = now - startTimeMs;
    if (elapsedMs < warmupMs && !hasReported) {
      // Still warming up — allow first report only after warmup
      return false;
    }
    if (hasReported && (now - lastReportTimeMs) < reportIntervalMs) {
      return false;
    }
    ProgressSnapshot snap = snapshot();
    reporter.accept(snap);
    lastReportTimeMs = now;
    hasReported = true;
    return true;
  }

  /** Force a report regardless of throttle (e.g. final summary). */
  public void forceReport(@Nonnull Consumer<ProgressSnapshot> reporter) {
    ProgressSnapshot snap = snapshot();
    reporter.accept(snap);
    lastReportTimeMs = clock.millis();
    hasReported = true;
  }

  @Nonnull
  private String formatMessage(
      long processed,
      @Nullable Long total,
      @Nullable Integer percent,
      double rate,
      @Nullable Long etaSeconds,
      boolean finished) {
    StringBuilder sb = new StringBuilder();
    sb.append(label).append(": ");
    if (total != null) {
      sb.append(processed).append('/').append(total);
      if (percent != null) {
        sb.append(" (").append(percent).append("%)");
      }
    } else {
      sb.append(processed).append(" processed");
    }
    sb.append(String.format(" %.1f/sec", rate));
    if (etaSeconds != null && !finished) {
      sb.append(" est. ETA ").append(formatDuration(etaSeconds));
    }
    return sb.toString();
  }

  /** Format a duration for ops logs, e.g. {@code 1h 2m}, {@code 2m 5s}, {@code 45s}. */
  @Nonnull
  public static String formatDuration(long totalSeconds) {
    if (totalSeconds < 0) {
      totalSeconds = 0;
    }
    long hours = totalSeconds / 3600;
    long minutes = (totalSeconds % 3600) / 60;
    long seconds = totalSeconds % 60;
    if (hours > 0) {
      return String.format("%dh %dm", hours, minutes);
    }
    if (minutes > 0) {
      return String.format("%dm %ds", minutes, seconds);
    }
    return String.format("%ds", seconds);
  }
}
