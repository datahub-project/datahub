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

public class HazelcastInstanceBootstrapConditionTest {

  private final HazelcastInstanceBootstrapCondition condition =
      new HazelcastInstanceBootstrapCondition();

  @AfterMethod
  public void tearDown() {
    RateLimitEffectiveConfig.reset();
  }

  @Test
  public void testSearchCacheHazelcastEnablesInstance() {
    assertTrue(evaluate("hazelcast", "false", "false", "false"));
  }

  @Test
  public void testEndpointEnabledEnablesInstanceWithoutSearchCache() {
    assertTrue(evaluate("caffeine", "true", "false", "false"));
  }

  @Test
  public void testScopedEnabledEnablesInstanceWithoutSearchCache() {
    // Scoped-only must provision a Hazelcast instance; otherwise the engine throws at startup.
    assertTrue(evaluate("caffeine", "false", "false", "true"));
  }

  @Test
  public void testEntityGraphCacheEnabledEnablesInstanceWithoutSearchCache() {
    assertTrue(evaluate("caffeine", "false", "true", "false"));
  }

  @Test
  public void testNeitherEnabledSkipsInstance() {
    assertFalse(evaluate("caffeine", "false", "false", "false"));
  }

  @Test
  public void testOverlayJsonEnablesInstanceWithoutSearchCache() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_JSON_ENV, "{\"scoped\":{\"enabled\":true}}");
    assertTrue(matches(environment));
  }

  @Test
  public void testMissingOverlayFileDoesNotFailBootstrapCondition() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(HazelcastBootstrapProperties.SEARCH_CACHE_IMPLEMENTATION, "caffeine");
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_FILE_ENV,
        "file:/tmp/datahub-missing-rate-limits-does-not-exist.yaml");
    assertFalse(matches(environment));
  }

  @Test
  public void testWriteLockHazelcastWithOptimisticLockingEnablesInstance() {
    assertTrue(evaluateWriteLock("hazelcast", "true", "ebean"));
  }

  @Test
  public void testWriteLockHazelcastWithoutOptimisticLockingSkipsInstance() {
    // The gate is bypassed when optimistic locking is off, so the embedded node must NOT boot for
    // it.
    assertFalse(evaluateWriteLock("hazelcast", "false", "ebean"));
  }

  @Test
  public void testWriteLockHazelcastTrimsOptimisticLockingValue() {
    // Spring's relaxed binding trims " true " to enable OL at runtime; the bootstrap condition must
    // trim too, or it would skip Hazelcast while the gate is live (degrading it to no-op).
    assertTrue(evaluateWriteLock("hazelcast", " true ", "ebean"));
  }

  @Test
  public void testWriteLockHazelcastOnCassandraSkipsInstance() {
    // Cassandra does not implement optimistic locking, so the gate can never engage — don't boot HZ
    // for it even if OPTIMISTIC_LOCKING_ENABLED is left true.
    assertFalse(evaluateWriteLock("hazelcast", "true", "cassandra"));
  }

  private boolean evaluateWriteLock(
      String backend, String optimisticLockingEnabled, String entityServiceImpl) {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(HazelcastBootstrapProperties.ENTITY_WRITE_LOCK_BACKEND, backend);
    environment.setProperty(
        HazelcastBootstrapProperties.OPTIMISTIC_LOCKING_ENABLED, optimisticLockingEnabled);
    environment.setProperty(HazelcastBootstrapProperties.ENTITY_SERVICE_IMPL, entityServiceImpl);
    return matches(environment);
  }

  private boolean evaluate(
      String cacheImplementation,
      String endpointEnabled,
      String entityGraphCacheEnabled,
      String scopedEnabled) {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        HazelcastBootstrapProperties.SEARCH_CACHE_IMPLEMENTATION, cacheImplementation);
    environment.setProperty(
        HazelcastBootstrapProperties.RATE_LIMIT_ENDPOINT_ENABLED, endpointEnabled);
    environment.setProperty(HazelcastBootstrapProperties.RATE_LIMIT_SCOPED_ENABLED, scopedEnabled);
    environment.setProperty(
        HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED, entityGraphCacheEnabled);
    return matches(environment);
  }

  private boolean matches(MockEnvironment environment) {
    ConditionContext context = Mockito.mock(ConditionContext.class);
    when(context.getEnvironment()).thenReturn(environment);
    return condition.matches(context, Mockito.mock(AnnotatedTypeMetadata.class));
  }
}
