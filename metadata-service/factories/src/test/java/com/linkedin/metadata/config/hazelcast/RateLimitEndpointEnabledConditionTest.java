package com.linkedin.metadata.config.hazelcast;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.mockito.Mockito;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.testng.annotations.Test;

public class RateLimitEndpointEnabledConditionTest {

  private final RateLimitEndpointEnabledCondition condition =
      new RateLimitEndpointEnabledCondition();

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

  private boolean evaluate(String endpointEnabled, String scopedEnabled) {
    ConditionContext context = Mockito.mock(ConditionContext.class);
    Environment environment = Mockito.mock(Environment.class);
    when(context.getEnvironment()).thenReturn(environment);
    when(environment.getProperty(HazelcastBootstrapProperties.RATE_LIMIT_ENDPOINT_ENABLED, "false"))
        .thenReturn(endpointEnabled);
    when(environment.getProperty(HazelcastBootstrapProperties.RATE_LIMIT_SCOPED_ENABLED, "false"))
        .thenReturn(scopedEnabled);
    return condition.matches(context, Mockito.mock(AnnotatedTypeMetadata.class));
  }
}
