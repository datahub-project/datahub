package com.linkedin.metadata.config.hazelcast;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.mockito.Mockito;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.testng.annotations.Test;

public class HazelcastInstanceBootstrapConditionTest {

  private final HazelcastInstanceBootstrapCondition condition =
      new HazelcastInstanceBootstrapCondition();

  @Test
  public void testSearchCacheHazelcastEnablesInstance() {
    assertTrue(evaluate("hazelcast", "false", "false"));
  }

  @Test
  public void testEndpointEnabledEnablesInstanceWithoutSearchCache() {
    assertTrue(evaluate("caffeine", "true", "false"));
  }

  @Test
  public void testEntityGraphCacheEnabledEnablesInstanceWithoutSearchCache() {
    assertTrue(evaluate("caffeine", "false", "true"));
  }

  @Test
  public void testNeitherEnabledSkipsInstance() {
    assertFalse(evaluate("caffeine", "false", "false"));
  }

  private boolean evaluate(
      String cacheImplementation, String endpointEnabled, String entityGraphCacheEnabled) {
    ConditionContext context = Mockito.mock(ConditionContext.class);
    Environment environment = Mockito.mock(Environment.class);
    when(context.getEnvironment()).thenReturn(environment);
    when(environment.getProperty(HazelcastBootstrapProperties.SEARCH_CACHE_IMPLEMENTATION))
        .thenReturn(cacheImplementation);
    when(environment.getProperty(
            HazelcastBootstrapProperties.SEARCH_CACHE_IMPLEMENTATION, "caffeine"))
        .thenReturn(cacheImplementation);
    when(environment.getProperty(HazelcastBootstrapProperties.RATE_LIMIT_ENDPOINT_ENABLED))
        .thenReturn(endpointEnabled);
    when(environment.getProperty(HazelcastBootstrapProperties.RATE_LIMIT_ENDPOINT_ENABLED, "false"))
        .thenReturn(endpointEnabled);
    when(environment.getProperty(HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED))
        .thenReturn(entityGraphCacheEnabled);
    when(environment.getProperty(HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED, "false"))
        .thenReturn(entityGraphCacheEnabled);
    return condition.matches(context, Mockito.mock(AnnotatedTypeMetadata.class));
  }
}
