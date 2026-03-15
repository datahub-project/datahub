package com.linkedin.metadata.utils.metrics;

import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.SystemMetadata;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

/**
 * Tracks cascade operation lifecycle for observability. Provides Micrometer metrics, conditional
 * logging, and optional MDC context for operations that fan out to many entities (e.g.,
 * deleteReferencesTo, PropertyDefinitionDeleteSideEffect).
 *
 * <p>This class is designed for single-level fan-out cascades (not recursive hierarchies). If
 * recursive cascading is needed in the future, add depth tracking and a {@code forChild()} pattern.
 *
 * <p>Usage with MDC (try-with-resources on the same thread):
 *
 * <pre>{@code
 * try (CascadeOperationContext cascade = CascadeOperationContext.begin(
 *         metricUtils, "deleteReferencesTo", triggerUrn, estimatedTotal)) {
 *     for (Entity entity : entities) {
 *         processEntity(entity);
 *         cascade.recordEntityProcessed();
 *     }
 * } // close() emits metrics, conditional log, restores MDC
 * }</pre>
 *
 * <p>Usage without MDC (for stream-based callers where begin/close may run on different threads):
 *
 * <pre>{@code
 * CascadeOperationContext cascade = CascadeOperationContext.beginWithoutMDC(
 *         metricUtils, "propertyDefinitionDelete", propertyUrn, -1);
 * return stream.peek(item -> cascade.recordEntityProcessed())
 *              .onClose(cascade::close);
 * }</pre>
 */
@Slf4j
public class CascadeOperationContext implements AutoCloseable {

  private static final long DEFAULT_SLOW_THRESHOLD_MS = 5000;

  /** MDC key for the cascade operation UUID. Public for use by Kafka consumers. */
  public static final String MDC_CASCADE_OPERATION_ID = "cascade.operation.id";

  public static final String MDC_CASCADE_TRIGGER_URN = "cascade.trigger.urn";
  public static final String MDC_CASCADE_OPERATION_TYPE = "cascade.operation.type";

  /** SystemMetadata properties key for cross-service correlation via Kafka. */
  public static final String SYSTEM_METADATA_CASCADE_ID_KEY = "cascadeOperationId";

  private final @Nullable MetricUtils metricUtils;
  private final String operationType;
  private final String triggerUrnType;
  private final String operationId;
  private final long startNanos;
  private final long slowThresholdMs;
  private final boolean manageMDC;
  private final AtomicInteger entitiesProcessed = new AtomicInteger(0);
  private final AtomicInteger errorCount = new AtomicInteger(0);
  private volatile String lastErrorType;

  // Saved MDC state for restore-on-close (supports nesting)
  private final @Nullable String prevOperationId;
  private final @Nullable String prevTriggerUrn;
  private final @Nullable String prevOperationType;

  private CascadeOperationContext(
      @Nullable MetricUtils metricUtils,
      String operationType,
      Urn triggerUrn,
      int estimatedTotal,
      long slowThresholdMs,
      boolean manageMDC) {
    this.metricUtils = metricUtils;
    this.operationType = operationType;
    this.triggerUrnType = triggerUrn != null ? triggerUrn.getEntityType() : "unknown";
    this.operationId = UUID.randomUUID().toString();
    this.startNanos = System.nanoTime();
    this.slowThresholdMs = slowThresholdMs;
    this.manageMDC = manageMDC;

    if (manageMDC) {
      // Save previous MDC values to restore on close (supports nesting)
      this.prevOperationId = MDC.get(MDC_CASCADE_OPERATION_ID);
      this.prevTriggerUrn = MDC.get(MDC_CASCADE_TRIGGER_URN);
      this.prevOperationType = MDC.get(MDC_CASCADE_OPERATION_TYPE);

      MDC.put(MDC_CASCADE_OPERATION_ID, operationId);
      MDC.put(MDC_CASCADE_TRIGGER_URN, triggerUrn != null ? triggerUrn.toString() : "unknown");
      MDC.put(MDC_CASCADE_OPERATION_TYPE, operationType);
    } else {
      this.prevOperationId = null;
      this.prevTriggerUrn = null;
      this.prevOperationType = null;
    }

    log.debug(
        "Cascade started: type={}, triggerUrn={}, estimatedEntities={}, manageMDC={}",
        operationType,
        triggerUrn,
        estimatedTotal,
        manageMDC);
  }

  /**
   * Begin tracking a cascade operation with MDC context. Use this when begin() and close() are
   * guaranteed to run on the same thread (i.e., try-with-resources blocks). MDC values from any
   * outer cascade context are saved and restored on close.
   *
   * @param metricUtils Micrometer metric utilities (nullable — metrics become no-ops if null)
   * @param operationType identifies the cascade type (e.g., "deleteReferencesTo")
   * @param triggerUrn the URN that triggered the cascade
   * @param estimatedTotal estimated number of entities to process (for logging only)
   * @return a new context that should be closed when the cascade completes
   */
  public static CascadeOperationContext begin(
      @Nullable MetricUtils metricUtils, String operationType, Urn triggerUrn, int estimatedTotal) {
    return new CascadeOperationContext(
        metricUtils, operationType, triggerUrn, estimatedTotal, DEFAULT_SLOW_THRESHOLD_MS, true);
  }

  /**
   * Begin tracking a cascade operation without MDC management. Use this when the context is created
   * on one thread but closed on another (e.g., stream-based callers using {@code
   * .onClose(cascade::close)}). Metrics and logging still work; only thread-local MDC is skipped.
   */
  public static CascadeOperationContext beginWithoutMDC(
      @Nullable MetricUtils metricUtils, String operationType, Urn triggerUrn, int estimatedTotal) {
    return new CascadeOperationContext(
        metricUtils, operationType, triggerUrn, estimatedTotal, DEFAULT_SLOW_THRESHOLD_MS, false);
  }

  /** Record that one entity was successfully processed. */
  public void recordEntityProcessed() {
    entitiesProcessed.incrementAndGet();
  }

  /**
   * Record an error during cascade processing.
   *
   * @param errorType a low-cardinality error type string for metric tagging
   */
  public void recordError(String errorType) {
    errorCount.incrementAndGet();
    this.lastErrorType = errorType;
  }

  /**
   * Attach the cascade operation ID to a SystemMetadata instance for cross-service correlation.
   * Mutates the SystemMetadata in-place. Safe to call on PatchItemImpl's SystemMetadata since both
   * the item and its backing MCP share the same object reference.
   *
   * @param systemMetadata the metadata to annotate (properties map created if null)
   */
  public void attachToSystemMetadata(@Nullable SystemMetadata systemMetadata) {
    if (systemMetadata == null) {
      return;
    }
    if (systemMetadata.getProperties() == null) {
      systemMetadata.setProperties(new com.linkedin.data.template.StringMap());
    }
    systemMetadata.getProperties().put(SYSTEM_METADATA_CASCADE_ID_KEY, operationId);
  }

  /** Returns the cascade operation ID for this context. */
  public String getOperationId() {
    return operationId;
  }

  /**
   * Emit metrics and conditional log, then clear MDC. Never throws — metric/logging failures are
   * caught internally to avoid breaking the cascade operation.
   */
  @Override
  public void close() {
    try {
      long durationNanos = System.nanoTime() - startNanos;
      long durationMs = TimeUnit.NANOSECONDS.toMillis(durationNanos);
      int processed = entitiesProcessed.get();
      int errors = errorCount.get();
      String status = errors > 0 ? "completed_with_errors" : "completed";

      // Emit metrics (always, regardless of log level)
      if (metricUtils != null) {
        metricUtils.recordTimer(
            "datahub.cascade.duration",
            durationNanos,
            "operation_type",
            operationType,
            "trigger_urn_type",
            triggerUrnType,
            "status",
            status);
        metricUtils.incrementMicrometer(
            "datahub.cascade.entities_processed",
            processed,
            "operation_type",
            operationType,
            "trigger_urn_type",
            triggerUrnType);
        if (errors > 0) {
          metricUtils.incrementMicrometer(
              "datahub.cascade.errors",
              errors,
              "operation_type",
              operationType,
              "trigger_urn_type",
              triggerUrnType,
              "error_type",
              lastErrorType != null ? lastErrorType : "unknown");
        }
      }

      // Conditional logging per PR #16577/#16578 guidelines
      if (errors > 0) {
        log.warn(
            "Cascade completed with errors: type={}, entities={}, errors={}, duration={}ms",
            operationType,
            processed,
            errors,
            durationMs);
      } else if (durationMs >= slowThresholdMs) {
        log.info(
            "Cascade completed (slow): type={}, entities={}, duration={}ms",
            operationType,
            processed,
            durationMs);
      } else {
        log.debug(
            "Cascade completed: type={}, entities={}, duration={}ms",
            operationType,
            processed,
            durationMs);
      }
    } catch (Exception e) {
      // Never propagate — observability failures must not break the cascade
      log.debug("Failed to emit cascade metrics", e);
    } finally {
      if (manageMDC) {
        restoreMDC(MDC_CASCADE_OPERATION_ID, prevOperationId);
        restoreMDC(MDC_CASCADE_TRIGGER_URN, prevTriggerUrn);
        restoreMDC(MDC_CASCADE_OPERATION_TYPE, prevOperationType);
      }
    }
  }

  private static void restoreMDC(String key, @Nullable String previousValue) {
    if (previousValue != null) {
      MDC.put(key, previousValue);
    } else {
      MDC.remove(key);
    }
  }
}
