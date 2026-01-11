package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.SystemAspectValidator;
import com.linkedin.metadata.config.AspectSizeValidationConfig;
import com.linkedin.metadata.config.OversizedAspectRemediation;
import com.linkedin.metadata.entity.validation.AspectDeletionRequest;
import com.linkedin.metadata.entity.validation.AspectSizeExceededException;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Payload validator that validates aspect size after serialization but before database write.
 * Throws AspectSizeExceededException if the aspect exceeds configured thresholds.
 *
 * <p>This validation applies to ALL aspect writes (REST, GraphQL, MCP), configured via
 * datahub.validation.aspectSize.postPatch.
 *
 * <p><b>Why This Validator Exists:</b> This validator implements aspect size validation at zero
 * performance cost by reusing serialization that is ALREADY REQUIRED before database writes.
 * Aspects must be serialized to JSON before being written to the database - this is not optional.
 * By implementing AspectPayloadValidator, we can inspect the already-serialized JSON string without
 * introducing a second serialization into the hot path. Since JSON serialization is one of the most
 * expensive parts of MCP processing, adding a second serialization just for size validation would
 * effectively double this cost. The validator pattern makes size validation essentially free.
 *
 * <p><b>Why Size Validation is Necessary:</b> Oversized aspects have been observed in production
 * databases, and any deployment can potentially create them. Without validation, aspects can grow
 * beyond deserialization limits, causing unrecoverable failures when reading those aspects back.
 * This validation provides a sanity check to prevent creating aspects that cannot be read later.
 *
 * <p><b>DELETE Remediation:</b> When DELETE remediation is configured, oversized aspects are
 * collected in OperationContext during validation. After the database transaction commits,
 * EntityServiceImpl processes these deletions through proper EntityService flow, ensuring all side
 * effects are handled (Elasticsearch, graph, consumer hooks).
 */
@Slf4j
public class AspectSizePayloadValidator implements SystemAspectValidator {

  private final AspectSizeValidationConfig config;
  private final MetricUtils metricUtils;

  public AspectSizePayloadValidator(
      @Nonnull AspectSizeValidationConfig config, @Nullable MetricUtils metricUtils) {
    this.config = config;
    this.metricUtils = metricUtils;
  }

  /**
   * Categorize aspect size into buckets for distribution tracking. Buckets are configured to align
   * with typical validation thresholds.
   *
   * @param bytes aspect size in bytes
   * @return bucket label (e.g., "1-5MB", "10-15MB")
   */
  private static String getSizeBucket(long bytes) {
    long mb = bytes / (1024 * 1024);
    if (mb < 1) return "0-1MB";
    if (mb < 5) return "1-5MB";
    if (mb < 10) return "5-10MB";
    if (mb < 15) return "10-15MB";
    return "15MB+";
  }

  @Override
  public void validatePayload(
      @Nonnull SystemAspect systemAspect, @Nonnull EntityAspect serializedAspect) {

    if (config.getPostPatch() == null || !config.getPostPatch().isEnabled()) {
      return; // Validation disabled
    }

    String metadata = serializedAspect.getMetadata();
    if (metadata == null) {
      return; // No metadata to validate
    }

    long actualSize = metadata.length();
    long threshold = config.getPostPatch().getMaxSizeBytes();

    // Emit bucketed counter for size distribution tracking
    if (metricUtils != null) {
      metricUtils.incrementMicrometer(
          "aspectSizeValidation.postPatch.aspectSize",
          1,
          "aspectName",
          systemAspect.getAspectSpec().getName(),
          "sizeBucket",
          getSizeBucket(actualSize));
    }

    if (actualSize > threshold) {
      OversizedAspectRemediation remediation = config.getPostPatch().getOversizedRemediation();

      log.warn(
          "Oversized post-patch aspect remediation={}: urn={}, aspect={}, size={} serialized bytes, threshold={} serialized bytes",
          remediation != null ? remediation.logLabel : "null",
          systemAspect.getUrn(),
          systemAspect.getAspectSpec().getName(),
          actualSize,
          threshold);

      // Emit oversized counter metric
      if (metricUtils != null) {
        metricUtils.incrementMicrometer(
            "aspectSizeValidation.postPatch.oversized",
            1,
            "aspectName",
            systemAspect.getAspectSpec().getName(),
            "remediation",
            remediation != null ? remediation.name() : "null");
      }

      // For DELETE remediation, collect deletion request for execution after transaction
      if (remediation == OversizedAspectRemediation.DELETE) {
        Object opContextObj = systemAspect.getOperationContext();
        if (opContextObj instanceof io.datahubproject.metadata.context.OperationContext) {
          io.datahubproject.metadata.context.OperationContext opContext =
              (io.datahubproject.metadata.context.OperationContext) opContextObj;
          AspectDeletionRequest request =
              AspectDeletionRequest.builder()
                  .urn(systemAspect.getUrn())
                  .aspectName(systemAspect.getAspectSpec().getName())
                  .validationPoint("POST_DB_PATCH")
                  .aspectSize(actualSize)
                  .threshold(threshold)
                  .build();
          opContext.addPendingDeletion(request);
        }
      }

      // Always throw to prevent this write (for both DELETE and IGNORE)
      throw new AspectSizeExceededException(
          "POST_DB_PATCH",
          actualSize,
          threshold,
          systemAspect.getUrn().toString(),
          systemAspect.getAspectSpec().getName());
    } else if (config.getPostPatch().getWarnSizeBytes() != null
        && actualSize > config.getPostPatch().getWarnSizeBytes()) {
      // Exceeded warning threshold but under max - log without blocking
      log.warn(
          "Large post-patch aspect (above warning threshold): urn={}, aspect={}, size={} serialized bytes, warnThreshold={}, maxThreshold={}",
          systemAspect.getUrn(),
          systemAspect.getAspectSpec().getName(),
          actualSize,
          config.getPostPatch().getWarnSizeBytes(),
          threshold);

      // Emit warning counter metric
      if (metricUtils != null) {
        metricUtils.incrementMicrometer(
            "aspectSizeValidation.postPatch.warning",
            1,
            "aspectName",
            systemAspect.getAspectSpec().getName());
      }
      // No throw - write proceeds
    }
  }
}
