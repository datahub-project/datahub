package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.AspectSizeValidationConfig;
import com.linkedin.metadata.config.OversizedAspectRemediation;
import com.linkedin.metadata.entity.validation.AspectSizeExceededException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Hook that validates aspect size after serialization but before database write. Throws
 * AspectSizeExceededException if the aspect exceeds configured thresholds.
 *
 * <p>This validation applies to ALL aspect writes (REST, GraphQL, MCP), configured via
 * datahub.validation.aspectSize.postPatch.
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

  public void setAspectDao(@Nonnull AspectDao aspectDao) {
    this.aspectDao = aspectDao;
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
          "Oversized post-patch aspect {}: urn={}, aspect={}, size={} chars, threshold={} chars, measurement=serialized_json_character_count",
          remediation,
          systemAspect.getUrn(),
          systemAspect.getAspectSpec().getName(),
          actualSize,
          threshold);

      // Handle oversized aspect according to remediation strategy
      if (remediation == OversizedAspectRemediation.DELETE) {
        // Hard delete the oversized aspect from database
        try {
          aspectDao.deleteAspect(
              systemAspect.getUrn().toString(), systemAspect.getAspectSpec().getName(), 0L, true);
          log.warn(
              "Hard deleted oversized post-patch aspect from database: urn={}, aspect={}",
              systemAspect.getUrn(),
              systemAspect.getAspectSpec().getName());
        } catch (Exception e) {
          log.error(
              "Failed to delete oversized post-patch aspect: urn={}, aspect={}",
              systemAspect.getUrn(),
              systemAspect.getAspectSpec().getName(),
              e);
        }
      }

      // For both DELETE and IGNORE: throw exception to prevent this write
      throw new AspectSizeExceededException(
          "postPatch",
          actualSize,
          threshold,
          systemAspect.getUrn().toString(),
          systemAspect.getAspectSpec().getName());
    }
  }
}
