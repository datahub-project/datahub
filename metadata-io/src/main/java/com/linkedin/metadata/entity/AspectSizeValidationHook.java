package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.AspectSerializationHook;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.AspectSizeValidationConfig;
import com.linkedin.metadata.config.OversizedAspectRemediation;
import com.linkedin.metadata.entity.validation.AspectSizeExceededException;
import com.linkedin.metadata.entity.validation.ValidationPoint;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Hook that validates aspect size after serialization but before database write. Throws
 * AspectSizeExceededException if the aspect exceeds configured thresholds.
 *
 * <p>This validation applies to ALL aspect writes (REST, GraphQL, MCP), configured via
 * datahub.validation.aspectSize.postPatch.
 *
 * <p><b>Why This Hook Exists:</b> This hook implements aspect size validation at zero performance
 * cost by reusing serialization that is ALREADY REQUIRED before database writes. Aspects must be
 * serialized to JSON before being written to the database - this is not optional. By implementing
 * AspectSerializationHook, we can inspect the already-serialized JSON string without introducing a
 * second serialization into the hot path. Since JSON serialization is one of the most expensive
 * parts of MCP processing, adding a second serialization just for size validation would effectively
 * double this cost. The hook design makes size validation essentially free.
 *
 * <p><b>Why Size Validation is Necessary:</b> Oversized aspects have been observed in production
 * databases, and any deployment can potentially create them. Without validation, aspects can grow
 * beyond deserialization limits, causing unrecoverable failures when reading those aspects back.
 * This validation provides a sanity check to prevent creating aspects that cannot be read later.
 */
@Slf4j
public class AspectSizeValidationHook implements AspectSerializationHook {

  private AspectDao aspectDao;
  private final AspectSizeValidationConfig config;

  public AspectSizeValidationHook(
      @Nullable AspectDao aspectDao, @Nonnull AspectSizeValidationConfig config) {
    this.aspectDao = aspectDao;
    this.config = config;
  }

  @Override
  public void afterSerialization(
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
          remediation.logLabel,
          systemAspect.getUrn(),
          systemAspect.getAspectSpec().getName(),
          actualSize,
          threshold);

      // Handle oversized aspect according to remediation strategy
      if (remediation == OversizedAspectRemediation.DELETE) {
        // Hard delete the oversized aspect from database
        // Note: If aspect doesn't exist, delete is no-op (no exception thrown)
        // Any exceptions indicate serious database issues and should propagate
        aspectDao.deleteAspect(systemAspect.getUrn(), systemAspect.getAspectSpec().getName(), 0L);
        log.warn(
            "Hard deleted oversized post-patch aspect from database: urn={}, aspect={}",
            systemAspect.getUrn(),
            systemAspect.getAspectSpec().getName());
      }

      // For both DELETE and IGNORE: throw exception to prevent this write
      throw new AspectSizeExceededException(
          ValidationPoint.POST_DB_PATCH,
          actualSize,
          threshold,
          systemAspect.getUrn().toString(),
          systemAspect.getAspectSpec().getName());
    }
  }
}
