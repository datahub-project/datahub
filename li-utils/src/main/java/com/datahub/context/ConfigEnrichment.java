package com.datahub.context;

import javax.annotation.Nonnull;

/**
 * Per-operation configuration resolution, stamped as an {@link Enrichment} at ingress; where none
 * is stamped, reads serve the value bound at startup. Implementations must be thread-safe, never
 * throw, and have a constant {@code toString()}.
 */
public interface ConfigEnrichment extends Enrichment {

  /** Resolves {@code key} for {@code operation}, falling back to {@code defaultValue}. */
  @Nonnull
  <T> T resolve(
      @Nonnull OperationFingerprint operation, @Nonnull String key, @Nonnull T defaultValue);

  /** Stored under this interface so common code finds any distribution's implementation. */
  @Override
  @Nonnull
  default Class<? extends Enrichment> enrichmentType() {
    return ConfigEnrichment.class;
  }
}
