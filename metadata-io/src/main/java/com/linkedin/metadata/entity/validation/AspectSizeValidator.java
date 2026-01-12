package com.linkedin.metadata.entity.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.AspectSizeValidationConfig;
import com.linkedin.metadata.config.OversizedAspectRemediation;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for validating aspect sizes during pre-patch processing.
 *
 * <p>Validates existing aspects from database before patch application. When oversized aspects are
 * detected, handles remediation according to configured strategy:
 *
 * <ul>
 *   <li><b>IGNORE:</b> Logs warning, throws exception to skip write
 *   <li><b>DELETE:</b> Logs warning, adds deletion request to OperationContext, throws exception to
 *       skip write. Deletion executed through EntityService after transaction commits.
 * </ul>
 *
 * <p><b>Performance:</b> Zero overhead - validates JSON already fetched from database, no
 * additional serialization required.
 *
 * <p><b>Usage:</b> Called by SystemAspect builders (EbeanSystemAspect, CassandraSystemAspect) when
 * loading aspects for update operations.
 */
@Slf4j
public class AspectSizeValidator {

  private AspectSizeValidator() {
    // Utility class
  }

  /**
   * Categorize aspect size into buckets for distribution tracking. Simplified to reduce allocations
   * - uses direct computation instead of pre-computed labels since AspectSizeValidator is a static
   * utility.
   *
   * @param bytes aspect size in bytes
   * @param boundaries sorted list of bucket boundaries in bytes
   * @return bucket label (e.g., "0-1MB", "1MB-5MB", "10MB-15MB", "15MB+")
   */
  private static String getSizeBucket(long bytes, java.util.List<Long> boundaries) {
    if (boundaries == null || boundaries.isEmpty()) {
      return formatBytes(bytes);
    }

    // Find first boundary that exceeds the size (avoid boxing by using get(i))
    long prevBoundary = 0;
    for (int i = 0; i < boundaries.size(); i++) {
      long boundary = boundaries.get(i);
      if (bytes < boundary) {
        return formatBytes(prevBoundary) + "-" + formatBytes(boundary);
      }
      prevBoundary = boundary;
    }

    // Size exceeds all boundaries
    return formatBytes(prevBoundary) + "+";
  }

  /**
   * Format bytes as human-readable size for metric labels (e.g., 1048576 -> "1MB").
   *
   * <p>Note: We don't use {@link com.linkedin.metadata.search.utils.SizeUtils#formatBytes(long)}
   * because its format ("2.5 MB" with spaces and decimals) is designed for user-facing output,
   * while metric labels should be simple identifiers without spaces.
   *
   * @param bytes size in bytes
   * @return formatted string (e.g., "1MB", "5MB")
   */
  private static String formatBytes(long bytes) {
    if (bytes == 0) return "0";
    long mb = bytes / (1024 * 1024);
    if (mb == 0) return bytes + "B";
    return mb + "MB";
  }

  /**
   * Validates pre-patch aspect size (existing aspect from database).
   *
   * <p>This is a convenience method that calls {@link #validatePrePatchSize(String, Urn, String,
   * AspectSizeValidationConfig, io.datahubproject.metadata.context.OperationContext, MetricUtils)}
   * with null context and null metrics.
   *
   * @param rawMetadata serialized aspect JSON from database (may be null for new aspects)
   * @param urn entity URN
   * @param aspectName aspect name
   * @param config aspect size validation configuration (may be null if validation disabled)
   * @throws AspectSizeExceededException if aspect exceeds configured size threshold
   */
  public static void validatePrePatchSize(
      @Nullable String rawMetadata,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nullable AspectSizeValidationConfig config) {
    validatePrePatchSize(rawMetadata, urn, aspectName, config, null, null);
  }

  /**
   * Validates pre-patch aspect size (existing aspect from database).
   *
   * <p>If aspect is oversized:
   *
   * <ul>
   *   <li>Logs WARNING with URN, aspect name, size, and threshold
   *   <li>For DELETE remediation: adds deletion request to OperationContext for later execution
   *   <li>Throws AspectSizeExceededException to skip the aspect write
   * </ul>
   *
   * @param rawMetadata serialized aspect JSON from database (may be null for new aspects)
   * @param urn entity URN
   * @param aspectName aspect name
   * @param config aspect size validation configuration (may be null if validation disabled)
   * @param opContext optional operation context (may contain remediation deletion flag to skip
   *     validation)
   * @param metricUtils optional metrics utility for emitting validation metrics
   * @throws AspectSizeExceededException if aspect exceeds configured size threshold
   */
  public static void validatePrePatchSize(
      @Nullable String rawMetadata,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nullable AspectSizeValidationConfig config,
      @Nullable io.datahubproject.metadata.context.OperationContext opContext,
      @Nullable MetricUtils metricUtils) {

    // Skip validation if this is a remediation deletion to avoid circular validation
    if (opContext != null
        && opContext.getValidationContext() != null
        && opContext.getValidationContext().isRemediationDeletion()) {
      log.debug(
          "Skipping pre-patch size validation for remediation deletion: urn={}, aspect={}",
          urn,
          aspectName);
      return;
    }

    // Validation disabled
    if (config == null || config.getPrePatch() == null || !config.getPrePatch().isEnabled()) {
      return;
    }

    // No metadata to validate (new aspect)
    if (rawMetadata == null) {
      return;
    }

    long actualSize = rawMetadata.length();
    long threshold = config.getPrePatch().getMaxSizeBytes();

    // Emit bucketed counter for size distribution tracking
    if (metricUtils != null) {
      // Get size buckets from config or use defaults (1MB, 5MB, 10MB, 15MB)
      java.util.List<Long> sizeBuckets =
          (config.getMetrics() != null && config.getMetrics().getSizeBuckets() != null)
              ? config.getMetrics().getSizeBuckets()
              : java.util.Arrays.asList(1048576L, 5242880L, 10485760L, 15728640L);

      metricUtils.incrementMicrometer(
          "aspectSizeValidation.prePatch.sizeDistribution",
          1,
          "aspectName",
          aspectName,
          "sizeBucket",
          getSizeBucket(actualSize, sizeBuckets));
    }

    if (actualSize > threshold) {
      OversizedAspectRemediation remediation = config.getPrePatch().getOversizedRemediation();

      log.warn(
          "Oversized pre-patch aspect remediation={}: urn={}, aspect={}, size={} serialized bytes, threshold={} serialized bytes",
          remediation != null ? remediation.logLabel : "null",
          urn,
          aspectName,
          actualSize,
          threshold);

      // Emit oversized counter metric
      if (metricUtils != null) {
        metricUtils.incrementMicrometer(
            "aspectSizeValidation.prePatch.oversized",
            1,
            "aspectName",
            aspectName,
            "remediation",
            remediation != null ? remediation.name() : "null");
      }

      // For DELETE remediation, collect deletion request for execution after transaction
      if (remediation == OversizedAspectRemediation.DELETE && opContext != null) {
        opContext.addPendingDeletion(
            AspectDeletionRequest.builder()
                .urn(urn)
                .aspectName(aspectName)
                .validationPoint("PRE_DB_PATCH")
                .aspectSize(actualSize)
                .threshold(threshold)
                .build());
      }

      // Always throw to skip the write (for both IGNORE and DELETE)
      throw new AspectSizeExceededException(
          "PRE_DB_PATCH", actualSize, threshold, urn.toString(), aspectName);
    } else if (config.getPrePatch().getWarnSizeBytes() != null
        && actualSize > config.getPrePatch().getWarnSizeBytes()) {
      // Exceeded warning threshold but under max - log without blocking
      log.warn(
          "Large pre-patch aspect (above warning threshold): urn={}, aspect={}, size={} serialized bytes, warnThreshold={}, maxThreshold={}",
          urn,
          aspectName,
          actualSize,
          config.getPrePatch().getWarnSizeBytes(),
          threshold);

      // Emit warning counter metric
      if (metricUtils != null) {
        metricUtils.incrementMicrometer(
            "aspectSizeValidation.prePatch.warning", 1, "aspectName", aspectName);
      }
      // No throw - write proceeds
    }
  }
}
