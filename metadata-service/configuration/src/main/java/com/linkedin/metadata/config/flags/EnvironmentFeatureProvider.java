package com.linkedin.metadata.config.flags;

import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.FeatureProvider;
import dev.openfeature.sdk.Metadata;
import dev.openfeature.sdk.ProviderEvaluation;
import dev.openfeature.sdk.Reason;
import dev.openfeature.sdk.Value;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * OSS / default {@link FeatureProvider}: serves the deployment's own configuration.
 *
 * <p>Takes a plain {@code key → value} function rather than Spring's {@code Environment}, so this
 * module keeps Spring out of its public API and tests can pass a map. Production passes {@code
 * environment::getProperty}. A {@code null} return means the property is not set.
 *
 * <p><b>Flag keys are property names.</b> {@code getBooleanEvaluation("featureFlags.showBrowseV2",
 * …)} resolves the same {@code featureFlags.showBrowseV2} that {@code application.yaml} and the
 * {@code SHOW_BROWSE_V2} environment variable already feed. Do not introduce a second key
 * namespace: a flag whose OpenFeature key differs from its property name has two disagreeing
 * sources of truth.
 *
 * <p>Ignores the {@link EvaluationContext}: one deployment, one set of values. Per-namespace
 * targeting arrives as a {@code @Primary} provider from an extension module, never as a branch in
 * here.
 *
 * <p><b>Three things are required before a real key is read through this, and none of them are in
 * this skeleton.</b> Each turns a missing value into a <em>wrong</em> value, which is the failure a
 * configuration system can least afford:
 *
 * <ol>
 *   <li><b>Normalise the lookup</b>, <em>inside this constructor</em> so every construction site
 *       inherits it — the provider is built in more than one place, and normalising at the call
 *       sites instead would let one be fixed while another silently kept the bugs. {@code
 *       Environment::getProperty} is not what {@code @Value} uses. Of DataHub's own properties, 33
 *       are declared {@code ${VAR:#{null}}} and yield the literal string {@code "#{null}"} when
 *       unset — {@code #{...}} is SpEL, evaluated on the {@code @Value} path and nowhere else; 110
 *       are declared {@code ${VAR:}} and yield {@code ""}; and 4 are declared {@code ${VAR}} with
 *       no fallback, on which {@code getProperty} throws. Counts are as of writing.
 *   <li><b>Close the key space.</b> The {@code Environment} holds every property in the process, so
 *       an open key space makes {@code getString("ebean.password", "")} a working read of the
 *       database password. {@code authentication.tokenService.signingKey}, {@code .salt} and {@code
 *       authentication.systemClientSecret} are all reachable the same way. A remote provider needs
 *       a second, narrower set again — being readable locally is not the same permission as being
 *       settable per namespace by an external service.
 *   <li><b>Coerce strictly, but accept Spring's spellings.</b> The bare {@code valueOf} calls below
 *       are placeholders. {@code Boolean.valueOf("ture")} is {@code false}, so a typo silently
 *       disables a feature; and Spring's {@code StringToBooleanConverter} accepts {@code on}/{@code
 *       off}, {@code yes}/{@code no} and {@code 1}/{@code 0}, so an operator with {@code
 *       SOME_FLAG=1} in their Helm values gets {@code true} from {@code @Value} today and would
 *       have that feature flip off on migrating to this seam. Numeric {@code valueOf} throws rather
 *       than reporting.
 * </ol>
 */
public class EnvironmentFeatureProvider implements FeatureProvider {

  public static final String PROVIDER_NAME = "EnvironmentFeatureProvider";

  private final Function<String, String> properties;

  /**
   * @param properties raw deployment-configuration source; {@code null} means unset. Prerequisite 1
   *     on this class is implemented by wrapping it here, so that callers cannot forget to.
   */
  public EnvironmentFeatureProvider(@Nonnull final Function<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public Metadata getMetadata() {
    return () -> PROVIDER_NAME;
  }

  @Override
  public ProviderEvaluation<String> getStringEvaluation(
      @Nonnull final String key, final String defaultValue, final EvaluationContext ctx) {
    final String raw = properties.apply(key);
    return raw == null ? useDefault(defaultValue) : configured(raw);
  }

  @Override
  public ProviderEvaluation<Boolean> getBooleanEvaluation(
      @Nonnull final String key, final Boolean defaultValue, final EvaluationContext ctx) {
    final String raw = properties.apply(key);
    // TODO strict coercion — see prerequisite 3 on this class. This is the only one of the four
    // that
    // cannot fail: Boolean.valueOf never throws, so a malformed value resolves to false and is
    // reported
    // as Reason.STATIC, indistinguishable from a configured false. A flag whose caller default is
    // true is
    // therefore silently flipped. Do not ship a featureFlags.* read through this until it is
    // strict.
    return raw == null ? useDefault(defaultValue) : configured(Boolean.valueOf(raw.trim()));
  }

  @Override
  public ProviderEvaluation<Integer> getIntegerEvaluation(
      @Nonnull final String key, final Integer defaultValue, final EvaluationContext ctx) {
    final String raw = properties.apply(key);
    // TODO strict coercion — see prerequisite 3 on this class.
    return raw == null ? useDefault(defaultValue) : configured(Integer.valueOf(raw.trim()));
  }

  @Override
  public ProviderEvaluation<Double> getDoubleEvaluation(
      @Nonnull final String key, final Double defaultValue, final EvaluationContext ctx) {
    final String raw = properties.apply(key);
    // TODO strict coercion — see prerequisite 3 on this class.
    return raw == null ? useDefault(defaultValue) : configured(Double.valueOf(raw.trim()));
  }

  /** Configuration properties are flat scalars; a structured value has no representation here. */
  @Override
  public ProviderEvaluation<Value> getObjectEvaluation(
      @Nonnull final String key, @Nullable final Value defaultValue, final EvaluationContext ctx) {
    return useDefault(defaultValue);
  }

  private static <T> ProviderEvaluation<T> configured(@Nonnull final T value) {
    return ProviderEvaluation.<T>builder().value(value).reason(Reason.STATIC.name()).build();
  }

  private static <T> ProviderEvaluation<T> useDefault(@Nullable final T defaultValue) {
    return ProviderEvaluation.<T>builder()
        .value(defaultValue)
        .reason(Reason.DEFAULT.name())
        .build();
  }
}
