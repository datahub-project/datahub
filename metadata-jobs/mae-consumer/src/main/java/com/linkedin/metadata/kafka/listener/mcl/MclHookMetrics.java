package com.linkedin.metadata.kafka.listener.mcl;

import com.linkedin.metadata.trace.TraceServiceImpl;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

/** Shared MCL hook metrics, so the batch and non-batch listeners record queue time identically. */
@Slf4j
final class MclHookMetrics {

  private MclHookMetrics() {}

  /**
   * Records the hook queue time for an MCL event on {@link
   * MetricUtils#DATAHUB_REQUEST_HOOK_QUEUE_TIME}, tagged by hook, entity type, and change type.
   */
  static void recordHookQueueTime(
      MetricUtils metricUtils, MetadataChangeLog event, String hookName) {
    Long requestEpochMillis = TraceServiceImpl.extractTraceIdEpochMillis(event.getSystemMetadata());
    if (requestEpochMillis == null) {
      return;
    }
    long currentTimeMillis = System.currentTimeMillis();
    long queueTimeMs = currentTimeMillis - requestEpochMillis;

    // Validate timestamp is reasonable to avoid ArithmeticException from overflow when converting
    // to nanoseconds. External trace IDs (e.g., from observability tools) may not follow DataHub's
    // trace ID format and can parse as invalid timestamps. (queueTimeMs >= 0 is already implied by
    // requestEpochMillis <= currentTimeMillis.)
    if (requestEpochMillis > 0
        && requestEpochMillis <= currentTimeMillis
        && queueTimeMs < Long.MAX_VALUE / 1_000_000) {
      metricUtils
          .getRegistry()
          .timer(MetricUtils.DATAHUB_REQUEST_HOOK_QUEUE_TIME, queueTimeTags(hookName, event))
          .record(Duration.ofMillis(queueTimeMs));
    } else {
      log.debug(
          "Skipping queue time metric recording for hook {} due to invalid timestamp: requestEpochMillis={}, queueTimeMs={}",
          hookName,
          requestEpochMillis,
          queueTimeMs);
    }
  }

  /**
   * Tags for the hook queue-time timer: the hook plus the event's entity type and change type.
   * Missing fields default to {@code "unknown"} because Micrometer rejects null tag values.
   *
   * <p>aspectName is deliberately excluded: it is high-cardinality and this is a histogram timer,
   * so tagging by aspect would multiply the series/bucket count. Per-aspect slicing, if needed,
   * belongs on a counter rather than this timer.
   */
  private static String[] queueTimeTags(String hookName, MetadataChangeLog event) {
    return new String[] {
      MetricUtils.HOOK_TAG, hookName,
      MetricUtils.ENTITY_TYPE, event.hasEntityType() ? event.getEntityType() : "unknown",
      MetricUtils.CHANGE_TYPE, event.hasChangeType() ? event.getChangeType().name() : "unknown"
    };
  }
}
