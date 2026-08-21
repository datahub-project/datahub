package com.linkedin.metadata.config.flags;

import com.datahub.context.OperationFingerprint;
import dev.openfeature.sdk.FeatureProvider;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Caller-facing entry point for reading configuration that may vary per operation.
 *
 * <pre>{@code
 * if (flags.getBoolean(opContext, "featureFlags.showBrowseV2", false)) { ... }
 * }</pre>
 *
 * <p>Callers never see which {@link FeatureProvider} is active, never branch on deployment shape,
 * and never learn what identity the value was targeted on — {@link FlagEvaluationContextResolver}
 * owns that translation.
 *
 * <p>Reads never throw, and never return {@code null}: a third-party provider may hand back a
 * null-valued evaluation, and three of these four getters return primitives, so an unguarded null
 * would unbox to an NPE inside this facade. A provider that misbehaves costs the caller its
 * default, not the operation, because flags are read from Kafka consumer and request threads. That
 * makes the default part of the contract — pass the value the deployment should behave as if
 * configured. <b>Where absence is genuinely a bug</b> — an identity or credential the process
 * cannot run without — keep the value on {@code @Value} instead, which fails startup loudly and in
 * one place.
 *
 * <p>Holds the {@link FeatureProvider} directly rather than an OpenFeature {@code Client}, because
 * a DataHub process registers one provider and uses neither client hooks nor multiple domains; do
 * not reintroduce {@code OpenFeatureAPI} for its own sake.
 *
 * <p>Before a real key is read, see the prerequisites on {@link EnvironmentFeatureProvider} — the
 * key space is open today, so any property in the process is readable through this facade.
 */
@Slf4j
public class FeatureFlagProvider {

  private final FeatureProvider provider;
  private final FlagEvaluationContextResolver contextResolver;

  public FeatureFlagProvider(
      @Nonnull final FeatureProvider provider,
      @Nonnull final FlagEvaluationContextResolver contextResolver) {
    this.provider = provider;
    this.contextResolver = contextResolver;
  }

  public boolean getBoolean(
      @Nonnull final OperationFingerprint operation,
      @Nonnull final String key,
      final boolean defaultValue) {
    try {
      final Boolean value =
          provider
              .getBooleanEvaluation(key, defaultValue, contextResolver.resolve(operation))
              .getValue();
      return value == null ? defaultValue : value;
    } catch (RuntimeException e) {
      return warnAndDefault(key, defaultValue, e);
    }
  }

  @Nonnull
  public String getString(
      @Nonnull final OperationFingerprint operation,
      @Nonnull final String key,
      @Nonnull final String defaultValue) {
    try {
      final String value =
          provider
              .getStringEvaluation(key, defaultValue, contextResolver.resolve(operation))
              .getValue();
      return value == null ? defaultValue : value;
    } catch (RuntimeException e) {
      return warnAndDefault(key, defaultValue, e);
    }
  }

  public int getInteger(
      @Nonnull final OperationFingerprint operation,
      @Nonnull final String key,
      final int defaultValue) {
    try {
      final Integer value =
          provider
              .getIntegerEvaluation(key, defaultValue, contextResolver.resolve(operation))
              .getValue();
      return value == null ? defaultValue : value;
    } catch (RuntimeException e) {
      return warnAndDefault(key, defaultValue, e);
    }
  }

  public double getDouble(
      @Nonnull final OperationFingerprint operation,
      @Nonnull final String key,
      final double defaultValue) {
    try {
      final Double value =
          provider
              .getDoubleEvaluation(key, defaultValue, contextResolver.resolve(operation))
              .getValue();
      return value == null ? defaultValue : value;
    } catch (RuntimeException e) {
      return warnAndDefault(key, defaultValue, e);
    }
  }

  /**
   * The exception's message is deliberately not logged. This catch is broad because the provider
   * may be third-party and the property lookup is caller-supplied, so an arbitrary message could
   * carry a configuration value — and configuration can carry credentials.
   *
   * <p>TODO deduplicate per key: a persistently broken provider warns on every read, on request and
   * MCL threads.
   */
  private <T> T warnAndDefault(
      @Nonnull final String key, @Nonnull final T defaultValue, @Nonnull final RuntimeException e) {
    log.warn(
        "Flag provider {} failed evaluating '{}' ({}); using caller default",
        provider.getMetadata().getName(),
        key,
        e.getClass().getSimpleName());
    return defaultValue;
  }
}
