package com.linkedin.datahub.graphql.featureflags;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.ConfigEnrichment;
import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.resolver.ConfigKeyConstants;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

public class FeatureFlagsTest {

  /** Answers one key; every other key falls through to the caller's default. */
  private record StubConfigEnrichment(String answeredKey, Object answer)
      implements ConfigEnrichment {

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T> T resolve(
        @Nonnull OperationFingerprint operation, @Nonnull String key, @Nonnull T defaultValue) {
      return answeredKey.equals(key) ? (T) answer : defaultValue;
    }
  }

  @Test
  public void metricsEnabledFallsBackToTheBoundValue() {
    FeatureFlags flags = new FeatureFlags();

    flags.setMetricsEnabled(true);
    assertTrue(flags.isMetricsEnabled(OperationFingerprint.EMPTY));
    assertTrue(flags.isMetricsEnabled(TestOperationContexts.systemContextNoSearchAuthorization()));

    flags.setMetricsEnabled(false);
    assertFalse(flags.isMetricsEnabled(OperationFingerprint.EMPTY));
    assertFalse(flags.isMetricsEnabled(TestOperationContexts.systemContextNoSearchAuthorization()));
  }

  @Test
  public void metricsEnabledResolvesThroughTheOperationConfig() {
    FeatureFlags flags = new FeatureFlags();
    flags.setMetricsEnabled(true);

    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    OperationContext overridden =
        base.toBuilder()
            .enrichmentBundle(
                base.getEnrichmentBundle()
                    .plus(
                        new StubConfigEnrichment(
                            ConfigKeyConstants.FeatureFlags.METRICS_ENABLED, false)))
            .build(base.getSessionActorContext(), false);

    assertFalse(flags.isMetricsEnabled(overridden));
    assertTrue(flags.isMetricsEnabled(base));
  }
}
