package com.linkedin.metadata.usage.registry.metrics;

import com.linkedin.metadata.usage.registry.operations.ActivitySnapshot;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.RequestContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Registry-driven additive increment and Micrometer export mapping. */
public final class UsageMetricIncrementResolver {

  public static final String INPUT_BYTES_METRIC = "datahub.usage.input_bytes";
  public static final String BILLED_BYTES_METRIC = "datahub.usage.billed_bytes";
  public static final String OUTPUT_BYTES_METRIC = "datahub.usage.output_bytes";

  private UsageMetricIncrementResolver() {}

  /**
   * Returns true when the metric's {@code (emit_when, value_unit)} pair is recognized. Distinct
   * metrics and report-driven ({@link UsageMetricRegistry.EmitWhen#REPORTED}) rules are supported.
   */
  public static boolean isSupported(@Nonnull UsageMetricRegistry.MetricDefinition metric) {
    if (metric.mergeKind() == UsageMetricRegistry.MergeKind.DISTINCT) {
      return switch (metric.emitWhen()) {
        case ACTIVITY_ALLOWLIST, READER_ACTIVITY_ALLOWLIST, WRITER_ACTIVITY_ALLOWLIST -> true;
        default -> false;
      };
    }
    return resolveRequestPhaseIncrement(metric, null, null) >= 0
        || isReportDrivenMetric(metric)
        || isResponsePhaseMetric(metric);
  }

  /** Additive metrics incremented only via {@code recordReportedUsage}. */
  public static boolean isReportDrivenMetric(@Nonnull UsageMetricRegistry.MetricDefinition metric) {
    return metric.mergeKind() == UsageMetricRegistry.MergeKind.ADDITIVE
        && metric.emitWhen().isReportDriven();
  }

  /** True for additive metrics incremented during {@code recordResponse}. */
  public static boolean isResponsePhaseMetric(
      @Nonnull UsageMetricRegistry.MetricDefinition metric) {
    return metric.mergeKind() == UsageMetricRegistry.MergeKind.ADDITIVE
        && metric.emitWhen() == UsageMetricRegistry.EmitWhen.ALWAYS
        && metric.valueUnit() == ValueUnit.OUTPUT_BYTES;
  }

  /**
   * Request-phase additive increment. Returns {@code 0} when not applicable. Pass {@code null} for
   * {@code operationEntry} and {@code requestContext} to probe support only (returns {@code -1} for
   * unsupported pairs).
   */
  public static long resolveRequestPhaseIncrement(
      @Nonnull UsageMetricRegistry.MetricDefinition metric,
      @Nullable UsageOperationsRegistry.UsageOperationEntry operationEntry,
      @Nullable RequestContext requestContext) {
    if (metric.mergeKind() != UsageMetricRegistry.MergeKind.ADDITIVE) {
      return -1;
    }
    if (isResponsePhaseMetric(metric)) {
      return 0;
    }
    if (operationEntry == null || requestContext == null) {
      return isKnownRequestPhasePair(metric) ? 0 : -1;
    }

    int usageQuantity = Math.max(1, requestContext.getUsageQuantity());
    return switch (metric.emitWhen()) {
      case ALWAYS -> resolveAlwaysIncrement(
          metric.valueUnit(), operationEntry, requestContext, usageQuantity);
      case INGESTION_REQUEST -> resolveIngestionRequestIncrement(
          metric.valueUnit(), operationEntry, requestContext);
      case COST_PROFILE -> resolveCostProfileIncrement(
          metric.valueUnit(), operationEntry, usageQuantity);
      case REPORTED -> 0L;
      default -> -1;
    };
  }

  /** Response-phase increment for {@code OUTPUT_BYTES} metrics. */
  public static long resolveResponsePhaseIncrement(
      @Nonnull UsageMetricRegistry.MetricDefinition metric, long outputBytes) {
    if (!isResponsePhaseMetric(metric) || outputBytes <= 0) {
      return 0;
    }
    return outputBytes;
  }

  @Nonnull
  public static Optional<String> micrometerCounterName(
      @Nonnull UsageMetricRegistry.MetricDefinition metric) {
    if (metric.mergeKind() != UsageMetricRegistry.MergeKind.ADDITIVE) {
      return Optional.empty();
    }
    if ("billed_bytes".equals(metric.metricName())) {
      return Optional.of(BILLED_BYTES_METRIC);
    }
    if (isReportDrivenMetric(metric)) {
      // Do not share DATAHUB_REQUEST_COUNT with api_calls; export under the registry name.
      if (metric.valueUnit() == ValueUnit.COUNT) {
        return Optional.of("datahub.usage." + metric.metricName());
      }
      return Optional.empty();
    }
    return switch (metric.valueUnit()) {
      case COUNT -> Optional.of(MetricUtils.DATAHUB_REQUEST_COUNT);
      case INPUT_BYTES -> Optional.of(INPUT_BYTES_METRIC);
      case OUTPUT_BYTES -> Optional.of(OUTPUT_BYTES_METRIC);
      case COST_UNITS -> Optional.empty();
    };
  }

  public static boolean shouldEmitDistinct(
      @Nonnull UsageMetricRegistry.MetricDefinition metric,
      @Nonnull ActivitySnapshot activity,
      @Nonnull UsageOperationsRegistry.UsageOperationEntry operationEntry) {
    return switch (metric.emitWhen()) {
      case ACTIVITY_ALLOWLIST -> activity.activityAllowlist();
      case READER_ACTIVITY_ALLOWLIST -> activity.readerAllowlist();
      case WRITER_ACTIVITY_ALLOWLIST -> activity.writerAllowlist()
          && operationEntry.contributesToActiveWriters();
      default -> false;
    };
  }

  private static boolean isKnownRequestPhasePair(
      @Nonnull UsageMetricRegistry.MetricDefinition metric) {
    if (metric.mergeKind() != UsageMetricRegistry.MergeKind.ADDITIVE) {
      return false;
    }
    if (isResponsePhaseMetric(metric)) {
      return true;
    }
    return switch (metric.emitWhen()) {
      case ALWAYS -> metric.valueUnit() == ValueUnit.COUNT
          || metric.valueUnit() == ValueUnit.INPUT_BYTES;
      case INGESTION_REQUEST -> metric.valueUnit() == ValueUnit.INPUT_BYTES;
      case COST_PROFILE -> metric.valueUnit() == ValueUnit.COST_UNITS;
      case REPORTED -> true;
      default -> false;
    };
  }

  private static long resolveAlwaysIncrement(
      @Nonnull ValueUnit valueUnit,
      @Nonnull UsageOperationsRegistry.UsageOperationEntry operationEntry,
      @Nonnull RequestContext requestContext,
      int usageQuantity) {
    return switch (valueUnit) {
      case COUNT -> operationEntry.ingestionEndpoint() ? (long) usageQuantity : 1L;
      case INPUT_BYTES -> requestContext.getInputBytes() != null
          ? requestContext.getInputBytes()
          : 0L;
      case OUTPUT_BYTES -> 0L;
      case COST_UNITS -> -1L;
    };
  }

  private static long resolveIngestionRequestIncrement(
      @Nonnull ValueUnit valueUnit,
      @Nonnull UsageOperationsRegistry.UsageOperationEntry operationEntry,
      @Nonnull RequestContext requestContext) {
    if (valueUnit != ValueUnit.INPUT_BYTES || !operationEntry.ingestionEndpoint()) {
      return -1;
    }
    return requestContext.getInputBytes() != null ? requestContext.getInputBytes() : 0L;
  }

  private static long resolveCostProfileIncrement(
      @Nonnull ValueUnit valueUnit,
      @Nonnull UsageOperationsRegistry.UsageOperationEntry operationEntry,
      int usageQuantity) {
    if (valueUnit != ValueUnit.COST_UNITS) {
      return -1;
    }
    return (long) operationEntry.defaultCostUnits() * usageQuantity;
  }
}
