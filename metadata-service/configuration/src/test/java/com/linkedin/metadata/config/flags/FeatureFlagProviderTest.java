package com.linkedin.metadata.config.flags;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.FeatureProvider;
import dev.openfeature.sdk.ImmutableContext;
import dev.openfeature.sdk.Metadata;
import dev.openfeature.sdk.ProviderEvaluation;
import dev.openfeature.sdk.Value;
import java.util.Map;
import org.testng.annotations.Test;

public class FeatureFlagProviderTest {

  private static final String KEY = "example.someFlag";
  private static final String TARGET = "namespace-a";

  /**
   * Stands in for an extension-module resolver: targets on an identity derived from the operation.
   */
  private static final FlagEvaluationContextResolver TARGETING_RESOLVER =
      operation -> new ImmutableContext(TARGET);

  private static FeatureFlagProvider flagsOver(final Map<String, String> properties) {
    return new FeatureFlagProvider(
        new EnvironmentFeatureProvider(properties::get), TARGETING_RESOLVER);
  }

  @Test
  public void testConfiguredValuesAreReturnedByType() {
    assertTrue(flagsOver(Map.of(KEY, "true")).getBoolean(OperationFingerprint.EMPTY, KEY, false));
    assertEquals(
        flagsOver(Map.of(KEY, "configured")).getString(OperationFingerprint.EMPTY, KEY, "dflt"),
        "configured");
    assertEquals(flagsOver(Map.of(KEY, "5")).getInteger(OperationFingerprint.EMPTY, KEY, 1), 5);
  }

  @Test
  public void testUnsetKeyYieldsCallerDefault() {
    final FeatureFlagProvider flags = flagsOver(Map.of());

    assertFalse(flags.getBoolean(OperationFingerprint.EMPTY, KEY, false));
    assertTrue(flags.getBoolean(OperationFingerprint.EMPTY, KEY, true));
    assertEquals(flags.getString(OperationFingerprint.EMPTY, KEY, "dflt"), "dflt");
  }

  /**
   * The point of the facade: the targeting identity the resolver derived from the operation is what
   * the provider evaluates against. Without this, a per-namespace provider would serve every caller
   * the same values.
   */
  @Test
  public void testResolverTargetingReachesTheProvider() {
    final CapturingProvider provider = new CapturingProvider();
    new FeatureFlagProvider(provider, TARGETING_RESOLVER)
        .getBoolean(OperationFingerprint.EMPTY, KEY, false);

    assertEquals(provider.lastContext.getTargetingKey(), TARGET);
  }

  /**
   * Reads happen on Kafka consumer and request threads, so an exception escaping the facade would
   * fail work that has nothing to do with configuration.
   */
  @Test
  public void testProviderFaultCostsTheCallerItsDefaultNotAnException() {
    final FeatureFlagProvider flags =
        new FeatureFlagProvider(new ThrowingProvider(), TARGETING_RESOLVER);

    assertTrue(flags.getBoolean(OperationFingerprint.EMPTY, KEY, true));
    assertEquals(flags.getString(OperationFingerprint.EMPTY, KEY, "dflt"), "dflt");
    assertEquals(flags.getInteger(OperationFingerprint.EMPTY, KEY, 7), 7);
  }

  /** A resolver that cannot derive an identity must not take the caller's read down with it. */
  @Test
  public void testResolverFaultCostsTheCallerItsDefaultNotAnException() {
    final FeatureFlagProvider flags =
        new FeatureFlagProvider(
            new CapturingProvider(),
            operation -> {
              throw new IllegalStateException("no identity available");
            });

    assertTrue(flags.getBoolean(OperationFingerprint.EMPTY, KEY, true));
  }

  /**
   * Three of the four getters return primitives, so a provider handing back a null-valued
   * evaluation would unbox to an NPE inside the facade rather than at the provider.
   */
  @Test
  public void testNullProviderValueYieldsCallerDefault() {
    final FeatureFlagProvider flags =
        new FeatureFlagProvider(new NullValuedProvider(), TARGETING_RESOLVER);

    assertTrue(flags.getBoolean(OperationFingerprint.EMPTY, KEY, true));
    assertEquals(flags.getInteger(OperationFingerprint.EMPTY, KEY, 3), 3);
    assertEquals(flags.getString(OperationFingerprint.EMPTY, KEY, "dflt"), "dflt");
  }

  private static class NullValuedProvider implements FeatureProvider {

    @Override
    public Metadata getMetadata() {
      return () -> "NullValuedProvider";
    }

    @Override
    public ProviderEvaluation<String> getStringEvaluation(
        final String key, final String defaultValue, final EvaluationContext ctx) {
      return nullValued();
    }

    @Override
    public ProviderEvaluation<Boolean> getBooleanEvaluation(
        final String key, final Boolean defaultValue, final EvaluationContext ctx) {
      return nullValued();
    }

    @Override
    public ProviderEvaluation<Integer> getIntegerEvaluation(
        final String key, final Integer defaultValue, final EvaluationContext ctx) {
      return nullValued();
    }

    @Override
    public ProviderEvaluation<Double> getDoubleEvaluation(
        final String key, final Double defaultValue, final EvaluationContext ctx) {
      return nullValued();
    }

    @Override
    public ProviderEvaluation<Value> getObjectEvaluation(
        final String key, final Value defaultValue, final EvaluationContext ctx) {
      return nullValued();
    }

    private static <T> ProviderEvaluation<T> nullValued() {
      return ProviderEvaluation.<T>builder().value(null).build();
    }
  }

  private static class CapturingProvider implements FeatureProvider {

    private volatile EvaluationContext lastContext;

    @Override
    public Metadata getMetadata() {
      return () -> "CapturingProvider";
    }

    @Override
    public ProviderEvaluation<String> getStringEvaluation(
        final String key, final String defaultValue, final EvaluationContext ctx) {
      lastContext = ctx;
      return ProviderEvaluation.<String>builder().value(defaultValue).build();
    }

    @Override
    public ProviderEvaluation<Boolean> getBooleanEvaluation(
        final String key, final Boolean defaultValue, final EvaluationContext ctx) {
      lastContext = ctx;
      return ProviderEvaluation.<Boolean>builder().value(defaultValue).build();
    }

    @Override
    public ProviderEvaluation<Integer> getIntegerEvaluation(
        final String key, final Integer defaultValue, final EvaluationContext ctx) {
      lastContext = ctx;
      return ProviderEvaluation.<Integer>builder().value(defaultValue).build();
    }

    @Override
    public ProviderEvaluation<Double> getDoubleEvaluation(
        final String key, final Double defaultValue, final EvaluationContext ctx) {
      lastContext = ctx;
      return ProviderEvaluation.<Double>builder().value(defaultValue).build();
    }

    @Override
    public ProviderEvaluation<Value> getObjectEvaluation(
        final String key, final Value defaultValue, final EvaluationContext ctx) {
      lastContext = ctx;
      return ProviderEvaluation.<Value>builder().value(defaultValue).build();
    }
  }

  private static class ThrowingProvider implements FeatureProvider {

    @Override
    public Metadata getMetadata() {
      return () -> "ThrowingProvider";
    }

    @Override
    public ProviderEvaluation<String> getStringEvaluation(
        final String key, final String defaultValue, final EvaluationContext ctx) {
      throw new IllegalStateException("provider is broken");
    }

    @Override
    public ProviderEvaluation<Boolean> getBooleanEvaluation(
        final String key, final Boolean defaultValue, final EvaluationContext ctx) {
      throw new IllegalStateException("provider is broken");
    }

    @Override
    public ProviderEvaluation<Integer> getIntegerEvaluation(
        final String key, final Integer defaultValue, final EvaluationContext ctx) {
      throw new IllegalStateException("provider is broken");
    }

    @Override
    public ProviderEvaluation<Double> getDoubleEvaluation(
        final String key, final Double defaultValue, final EvaluationContext ctx) {
      throw new IllegalStateException("provider is broken");
    }

    @Override
    public ProviderEvaluation<Value> getObjectEvaluation(
        final String key, final Value defaultValue, final EvaluationContext ctx) {
      throw new IllegalStateException("provider is broken");
    }
  }
}
