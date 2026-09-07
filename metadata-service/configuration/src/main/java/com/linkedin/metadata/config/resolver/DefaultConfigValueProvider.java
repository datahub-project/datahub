package com.linkedin.metadata.config.resolver;

import com.datahub.context.OperationFingerprint;
import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.Metadata;
import dev.openfeature.sdk.ProviderEvaluation;
import dev.openfeature.sdk.Reason;
import dev.openfeature.sdk.Value;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** The default {@link ConfigValueProvider}: every read resolves to the statically bound value. */
public final class DefaultConfigValueProvider implements ConfigValueProvider {

  @Override
  @Nonnull
  public <T> T resolve(
      @Nonnull OperationFingerprint operation, @Nonnull String key, @Nonnull T staticValue) {
    return staticValue;
  }

  @Override
  public Metadata getMetadata() {
    return () -> "DefaultConfigValueProvider";
  }

  @Override
  public ProviderEvaluation<Boolean> getBooleanEvaluation(
      String key, Boolean defaultValue, EvaluationContext ctx) {
    return usingDefault(defaultValue);
  }

  @Override
  public ProviderEvaluation<String> getStringEvaluation(
      String key, String defaultValue, EvaluationContext ctx) {
    return usingDefault(defaultValue);
  }

  @Override
  public ProviderEvaluation<Integer> getIntegerEvaluation(
      String key, Integer defaultValue, EvaluationContext ctx) {
    return usingDefault(defaultValue);
  }

  @Override
  public ProviderEvaluation<Double> getDoubleEvaluation(
      String key, Double defaultValue, EvaluationContext ctx) {
    return usingDefault(defaultValue);
  }

  @Override
  public ProviderEvaluation<Value> getObjectEvaluation(
      String key, Value defaultValue, EvaluationContext ctx) {
    return usingDefault(defaultValue);
  }

  private static <T> ProviderEvaluation<T> usingDefault(@Nullable T defaultValue) {
    return ProviderEvaluation.<T>builder()
        .value(defaultValue)
        .reason(Reason.DEFAULT.name())
        .build();
  }
}
