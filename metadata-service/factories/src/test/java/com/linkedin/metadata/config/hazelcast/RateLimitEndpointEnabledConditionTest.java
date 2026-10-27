package com.linkedin.metadata.config.hazelcast;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.ratelimit.RateLimitConfigLoader;
import com.linkedin.metadata.config.ratelimit.RateLimitEffectiveConfig;
import org.mockito.Mockito;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.mock.env.MockEnvironment;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class RateLimitEndpointEnabledConditionTest {

  private final RateLimitEndpointEnabledCondition condition =
      new RateLimitEndpointEnabledCondition();

  @AfterMethod
  public void tearDown() {
    RateLimitEffectiveConfig.reset();
  }

  @Test
  public void testEndpointEnabledProvisionsRateLimitHazelcastResources() {
    assertTrue(evaluate("true", "false"));
  }

  @Test
  public void testScopedEnabledProvisionsRateLimitHazelcastResources() {
    // Scoped-only must also provision the Hazelcast map config (the scoped buckets share the map).
    assertTrue(evaluate("false", "true"));
  }

  @Test
  public void testBothDisabledSkipsRateLimitHazelcastResources() {
    assertFalse(evaluate("false", "false"));
  }

  @Test
  public void testOverlayJsonEnablesRateLimitHazelcastResources() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_JSON_ENV, "{\"endpoint\":{\"enabled\":true}}");
    assertTrue(matches(environment));
  }

  @Test
  public void testMissingOverlayFileDoesNotFailCondition() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_FILE_ENV,
        "file:/tmp/datahub-missing-rate-limits-does-not-exist.yaml");
    assertFalse(matches(environment));
  }

  @Test
  public void testMissingOverlayFileStillHonorsEnvironmentEndpointFlag() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_FILE_ENV,
        "file:/tmp/datahub-missing-rate-limits-does-not-exist.yaml");
    environment.setProperty(HazelcastBootstrapProperties.RATE_LIMIT_ENDPOINT_ENABLED, "true");
    assertTrue(matches(environment));
  }

  private boolean evaluate(String endpointEnabled, String scopedEnabled) {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        HazelcastBootstrapProperties.RATE_LIMIT_ENDPOINT_ENABLED, endpointEnabled);
    environment.setProperty(HazelcastBootstrapProperties.RATE_LIMIT_SCOPED_ENABLED, scopedEnabled);
    return matches(environment);
  }

  private boolean matches(MockEnvironment environment) {
    ConditionContext context = Mockito.mock(ConditionContext.class);
    when(context.getEnvironment()).thenReturn(environment);
    return condition.matches(context, Mockito.mock(AnnotatedTypeMetadata.class));
  }
}
