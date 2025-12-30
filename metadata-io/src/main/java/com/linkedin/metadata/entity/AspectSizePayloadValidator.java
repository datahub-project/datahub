package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.AspectPayloadValidator;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.AspectSizeValidationConfig;
import com.linkedin.metadata.config.OversizedAspectRemediation;
import com.linkedin.metadata.entity.validation.AspectDeletionRequest;
import com.linkedin.metadata.entity.validation.AspectSizeExceededException;
import com.linkedin.metadata.entity.validation.AspectValidationContext;
import com.linkedin.metadata.entity.validation.ValidationPoint;
import javax.annotation.Nonnull;
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
 * collected in ThreadLocal storage during validation. After the database transaction commits,
 * EntityServiceImpl processes these deletions through proper EntityService flow, ensuring all side
 * effects are handled (Elasticsearch, graph, consumer hooks).
 */
@Slf4j
public class AspectSizePayloadValidator implements AspectPayloadValidator {

  private final AspectSizeValidationConfig config;

  public AspectSizePayloadValidator(@Nonnull AspectSizeValidationConfig config) {
    this.config = config;
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

    if (actualSize > threshold) {
      OversizedAspectRemediation remediation = config.getPostPatch().getOversizedRemediation();

      log.warn(
          "Oversized post-patch aspect remediation={}: urn={}, aspect={}, size={} serialized bytes, threshold={} serialized bytes",
          remediation != null ? remediation.logLabel : "null",
          systemAspect.getUrn(),
          systemAspect.getAspectSpec().getName(),
          actualSize,
          threshold);

      // For DELETE remediation, collect deletion request for execution after transaction
      if (remediation == OversizedAspectRemediation.DELETE) {
        AspectValidationContext.addPendingDeletion(
            AspectDeletionRequest.builder()
                .urn(systemAspect.getUrn())
                .aspectName(systemAspect.getAspectSpec().getName())
                .validationPoint(ValidationPoint.POST_DB_PATCH)
                .aspectSize(actualSize)
                .threshold(threshold)
                .build());
      }

      // Always throw to prevent this write (for both DELETE and IGNORE)
      throw new AspectSizeExceededException(
          ValidationPoint.POST_DB_PATCH,
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
      // No throw - write proceeds
    }
  }
}
