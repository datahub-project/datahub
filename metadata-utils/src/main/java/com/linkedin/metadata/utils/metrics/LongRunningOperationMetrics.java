package com.linkedin.metadata.utils.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Metrics for one run of a long-running, page-at-a-time operation: an ES scroll, a reindex, a
 * backfill step.
 *
 * <p>Emits, under a caller-supplied prefix: {@code .launches} at start, {@code .entities_processed}
 * and {@code .pages} as work completes, {@code .errors} tagged {@code error_type}, and {@code
 * .duration} once at {@link #finish()} tagged {@code status=completed|failed} with percentiles and
 * hour-scale SLO buckets.
 *
 * <p><b>Why {@code .launches} increments at the start.</b> {@link #finish()} is meant to be called
 * from a {@code finally}, which covers exceptions but does not run when the process dies — a
 * SIGKILL, OOM kill or pod eviction mid-run produces no duration sample at all. Counting launches
 * separately is what makes those runs visible: launches minus duration-count is the
 * started-and-never-finished signal.
 *
 * <p><b>Not for bounded fan-outs.</b> Use {@link CascadeOperationContext} for work that finishes in
 * seconds; it adds a slow-log and an operation-id for correlation. This class exists because that
 * one builds its {@code Timer} internally and so cannot publish percentiles, which is the whole
 * point for multi-hour work.
 *
 * <p><b>Single-threaded.</b> One instance tracks one run. Do not share across threads — {@code
 * status} is mutated by {@link #failed} / {@link #finish} with no synchronization.
 *
 * <p>Every method is a no-op when no {@link MetricUtils} was available. Nothing here throws.
 */
public final class LongRunningOperationMetrics {

  /** Tag key for the operation family (e.g. {@code searchBasedFormAssignment}). */
  public static final String TAG_OPERATION = "operation_type";

  /** Tag key for a phase within an operation (e.g. {@code assign} / {@code unassign}). */
  public static final String TAG_PHASE = "phase";

  private static final double[] PERCENTILES = {0.5, 0.95, 0.99};
  private static final String TAG_STATUS = "status";
  private static final String TAG_ERROR_TYPE = "error_type";
  private static final String STATUS_COMPLETED = "completed";
  private static final String STATUS_FAILED = "failed";

  private final @Nullable MeterRegistry registry;
  private final String prefix;
  private final Tags tags;
  private final long startNanos;

  private String status = STATUS_COMPLETED;

  private LongRunningOperationMetrics(
      @Nullable final MeterRegistry registry, final String prefix, final Tags tags) {
    this.registry = registry;
    this.prefix = prefix;
    this.tags = tags;
    this.startNanos = System.nanoTime();
  }

  /**
   * Starts tracking and records the launch immediately.
   *
   * @param metricUtils nullable; all emission becomes a no-op when absent
   * @param prefix dot-separated metric family, e.g. {@code datahub.forms.assignment}
   * @param tags dimensions applied to every meter. Keep these low-cardinality
   */
  public static LongRunningOperationMetrics begin(
      @Nullable final MetricUtils metricUtils, final String prefix, final Tags tags) {
    final MeterRegistry registry = metricUtils == null ? null : metricUtils.getRegistry();
    final LongRunningOperationMetrics metrics =
        new LongRunningOperationMetrics(registry, prefix, tags);
    if (registry != null) {
      registry.counter(prefix + ".launches", tags).increment();
    }
    return metrics;
  }

  /** Records entities completed in one page. Call after the page's work has actually succeeded. */
  public void recordEntities(final int count) {
    if (registry != null && count > 0) {
      registry.counter(prefix + ".entities_processed", tags).increment(count);
    }
  }

  /**
   * Records one completed page. Emits immediately (same pattern as {@link #recordEntities} and
   * {@link #failed}) so a forgotten call is visible as a missing increment, not a silent zero at
   * {@link #finish()}.
   */
  public void recordPage() {
    if (registry != null) {
      registry.counter(prefix + ".pages", tags).increment();
    }
  }

  /**
   * Marks this run failed and counts the error. Must be called before {@link #finish()}, otherwise
   * the run is recorded as {@code status=completed} — a failure that reads as a success.
   *
   * @param errorType low-cardinality classification, never an exception message
   */
  public void failed(final String errorType) {
    status = STATUS_FAILED;
    if (registry != null) {
      registry.counter(prefix + ".errors", tags.and(TAG_ERROR_TYPE, errorType)).increment();
    }
  }

  /** Records duration. Call from a {@code finally} so a failed run still lands. */
  public void finish() {
    if (registry == null) {
      return;
    }
    Timer.builder(prefix + ".duration")
        .tags(tags.and(TAG_STATUS, status))
        .publishPercentiles(PERCENTILES)
        .serviceLevelObjectives(
            Duration.ofMinutes(1),
            Duration.ofMinutes(5),
            Duration.ofMinutes(15),
            Duration.ofMinutes(30),
            Duration.ofHours(1),
            Duration.ofHours(2),
            Duration.ofHours(6))
        .minimumExpectedValue(Duration.ofSeconds(1))
        .maximumExpectedValue(Duration.ofHours(6))
        .register(registry)
        .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
  }
}
