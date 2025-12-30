package com.linkedin.metadata.entity.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.AspectSizeValidationConfig;
import com.linkedin.metadata.config.OversizedAspectRemediation;
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
 *   <li><b>DELETE:</b> Logs warning, adds deletion request to ThreadLocal, throws exception to skip
 *       write. Deletion executed through EntityService after transaction commits.
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
   * Validates pre-patch aspect size (existing aspect from database).
   *
   * <p>If aspect is oversized:
   *
   * <ul>
   *   <li>Logs WARNING with URN, aspect name, size, and threshold
   *   <li>For DELETE remediation: adds deletion request to ThreadLocal for later execution
   *   <li>Throws AspectSizeExceededException to skip the aspect write
   * </ul>
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

    if (actualSize > threshold) {
      OversizedAspectRemediation remediation = config.getPrePatch().getOversizedRemediation();

      log.warn(
          "Oversized pre-patch aspect remediation={}: urn={}, aspect={}, size={} serialized bytes, threshold={} serialized bytes",
          remediation != null ? remediation.logLabel : "null",
          urn,
          aspectName,
          actualSize,
          threshold);

      // For DELETE remediation, collect deletion request for execution after transaction
      if (remediation == OversizedAspectRemediation.DELETE) {
        AspectValidationContext.addPendingDeletion(
            AspectDeletionRequest.builder()
                .urn(urn)
                .aspectName(aspectName)
                .validationPoint(ValidationPoint.PRE_DB_PATCH)
                .aspectSize(actualSize)
                .threshold(threshold)
                .build());
      }

      // Always throw to skip the write (for both IGNORE and DELETE)
      throw new AspectSizeExceededException(
          ValidationPoint.PRE_DB_PATCH, actualSize, threshold, urn.toString(), aspectName);
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
      // No throw - write proceeds
    }
  }
}
