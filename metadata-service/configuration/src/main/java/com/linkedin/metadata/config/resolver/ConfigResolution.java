package com.linkedin.metadata.config.resolver;

import com.datahub.context.OperationFingerprint;
import javax.annotation.Nonnull;

/**
 * Central read path for configuration values that may vary per operation. Every read resolves to
 * the statically bound value the caller passed — the value Spring already validated and bound at
 * startup.
 */
public final class ConfigResolution {

  private static volatile ConfigValueProvider provider = new DefaultConfigValueProvider();

  private ConfigResolution() {}

  public static void setProvider(@Nonnull ConfigValueProvider configValueProvider) {
    provider = configValueProvider;
  }

  @Nonnull
  public static <T> T resolve(
      @Nonnull OperationFingerprint operation, @Nonnull String key, @Nonnull T staticValue) {
    return provider.resolve(operation, key, staticValue);
  }
}
