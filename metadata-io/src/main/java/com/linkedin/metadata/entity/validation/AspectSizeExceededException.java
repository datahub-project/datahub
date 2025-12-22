package com.linkedin.metadata.entity.validation;

import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Exception thrown when an aspect exceeds configured size limits during validation.
 *
 * <p>Includes the validation point (PRE_DB_PATCH or POST_DB_PATCH), actual size, threshold, and
 * aspect identity for debugging.
 */
@Getter
public class AspectSizeExceededException extends RuntimeException {
  private final ValidationPoint validationPoint;
  private final long actualSize;
  private final long threshold;
  private final String urn;
  private final String aspectName;

  public AspectSizeExceededException(
      @Nonnull ValidationPoint validationPoint,
      long actualSize,
      long threshold,
      @Nonnull String urn,
      @Nonnull String aspectName) {
    super(
        String.format(
            "Size validation failed at %s: %d bytes exceeds threshold of %d bytes for urn=%s, aspect=%s",
            validationPoint, actualSize, threshold, urn, aspectName));
    this.validationPoint = validationPoint;
    this.actualSize = actualSize;
    this.threshold = threshold;
    this.urn = urn;
    this.aspectName = aspectName;
  }
}
