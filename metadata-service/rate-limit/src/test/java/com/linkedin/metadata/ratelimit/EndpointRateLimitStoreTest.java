package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import io.github.bucket4j.ConsumptionProbe;
import java.util.List;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class EndpointRateLimitStoreTest {

  private HazelcastInstance hazelcastInstance;

  @AfterMethod
  public void tearDown() {
    HazelcastTestSupport.shutdown(hazelcastInstance);
    hazelcastInstance = null;
  }

  @Test
  public void testDistributedEndpointBucketEnforcesClusterCapacity() {
    hazelcastInstance = HazelcastTestSupport.createIsolatedInstance();
    EndpointRateLimitStore store =
        new EndpointRateLimitStore(new RateLimitProperties.Endpoint(), hazelcastInstance);
    store.registerEndpointRule(endpointRule("signup-rule"));

    assertTrue(store.tryConsumeAndReturnRemaining("signup-rule").isConsumed());
    assertTrue(store.tryConsumeAndReturnRemaining("signup-rule").isConsumed());
    ConsumptionProbe denied = store.tryConsumeAndReturnRemaining("signup-rule");
    assertFalse(denied.isConsumed());
    assertTrue(denied.getNanosToWaitForRefill() > 0);
    assertEquals(store.capacity("signup-rule"), 2);
  }

  @Test
  public void testTryConsumeForActor_distinctActorsAreIndependent() {
    hazelcastInstance = HazelcastTestSupport.createIsolatedInstance();
    EndpointRateLimitStore store =
        new EndpointRateLimitStore(new RateLimitProperties.Endpoint(), hazelcastInstance);
    store.registerEndpointRule(endpointRule("api-rule"));

    // Exhaust actor alice
    store.tryConsumeForActor("api-rule", "urn:li:corpuser:alice");
    store.tryConsumeForActor("api-rule", "urn:li:corpuser:alice");
    assertFalse(store.tryConsumeForActor("api-rule", "urn:li:corpuser:alice").isConsumed());

    // Actor bob is unaffected
    assertTrue(store.tryConsumeForActor("api-rule", "urn:li:corpuser:bob").isConsumed());
  }

  @Test
  public void testTryConsumeForActor_unregisteredRuleReturnsNull() {
    hazelcastInstance = HazelcastTestSupport.createIsolatedInstance();
    EndpointRateLimitStore store =
        new EndpointRateLimitStore(new RateLimitProperties.Endpoint(), hazelcastInstance);

    assertNull(store.tryConsumeForActor("no-such-rule", "urn:li:corpuser:alice"));
  }

  @Test
  public void testTryConsumeForActor_repeatedCallsResumeState() {
    hazelcastInstance = HazelcastTestSupport.createIsolatedInstance();
    EndpointRateLimitStore store =
        new EndpointRateLimitStore(new RateLimitProperties.Endpoint(), hazelcastInstance);
    store.registerEndpointRule(endpointRule("write-rule"));

    long remainingAfterFirst =
        store.tryConsumeForActor("write-rule", "urn:li:corpuser:carol").getRemainingTokens();

    // Second call rebuilds the proxy for the same actor key — state must resume, not reset
    long remainingAfterSecond =
        store.tryConsumeForActor("write-rule", "urn:li:corpuser:carol").getRemainingTokens();

    assertTrue(
        remainingAfterSecond < remainingAfterFirst,
        "Second proxy build must resume persisted state, not reset it");
  }

  private static RateLimitProperties.Rule endpointRule(String id) {
    return RateLimitProperties.Rule.builder()
        .id(id)
        .pathPattern("/auth/signUp")
        .methods(List.of("POST"))
        .capacity(2)
        .refillTokens(2)
        .refillPeriodSeconds(60)
        .build();
  }
}
