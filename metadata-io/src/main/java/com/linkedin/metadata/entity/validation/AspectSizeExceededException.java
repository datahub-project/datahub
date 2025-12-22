package com.linkedin.metadata.entity.validation;

import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Exception thrown when a message or aspect exceeds configured size limits.
 *
 * <p>validationPoint indicates where validation occurred: "mcp_incoming" (from Kafka),
 * "aspect_update" (after patch), or "mcl_outgoing" (to Kafka).
 */
@Getter
public class AspectSizeExceededException extends RuntimeException {
  private final String validationPoint;
  private final long actualSize;
  private final long threshold;
  private final String urn;
  private final String aspectName;

  public AspectSizeExceededException(
      @Nonnull String validationPoint,
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
