package com.linkedin.metadata.config.resolver;

import com.datahub.context.OperationFingerprint;
import javax.annotation.Nonnull;

/**
 * Central read path for configuration values that may vary per operation. Every read resolves to
 * the statically bound value the caller passed — the value Spring already validated and bound at
 * startup.
 */
public class ConfigResolution {

  private ConfigResolution() {}

  /**
   * Called from configuration-class getters; {@code key} is a {@link ConfigKeyConstants} constant.
   */
  @Nonnull
  public static <T> T resolve(
      @Nonnull OperationFingerprint operation, @Nonnull String key, @Nonnull T staticValue) {
    return staticValue;
  }
}
