package com.linkedin.metadata.config.resolver;

import com.datahub.context.OperationFingerprint;
import dev.openfeature.sdk.FeatureProvider;
import javax.annotation.Nonnull;

/** Resolves a configuration key for an operation, falling back to {@code staticValue}. */
public interface ConfigValueProvider extends FeatureProvider {

  @Nonnull
  <T> T resolve(
      @Nonnull OperationFingerprint operation, @Nonnull String key, @Nonnull T staticValue);
}
