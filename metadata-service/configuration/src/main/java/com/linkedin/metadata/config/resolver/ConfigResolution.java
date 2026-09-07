package com.linkedin.metadata.config.resolver;

import com.datahub.context.OperationFingerprint;
import javax.annotation.Nonnull;

/**
 * Central read path for configuration values that may vary per operation. Reads go through the
 * installed {@link ConfigValueProvider} — by default {@link DefaultConfigValueProvider}, which
 * returns the statically bound value; {@link #setProvider} installs an override.
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
