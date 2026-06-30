package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import io.datahubproject.metadata.context.ObjectMapperContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests the scoped front-gate chain (Part A: actor → class → global, with refund-on-deny) and the
 * heavy-resolver gate entry point (Part B: {@link RateLimitEngine#consumeHeavyResolver}).
 */
public class RateLimitEngineScopedChainTest {

  private static final String GRAPHQL_PATH = "/api/graphql";
  private static final String ALICE = "urn:li:corpuser:alice";
  private static final String BOB = "urn:li:corpuser:bob";

  private HazelcastInstance hazelcastInstance;

  @AfterMethod
  public void tearDown() {
    HazelcastTestSupport.shutdown(hazelcastInstance);
    hazelcastInstance = null;
  }

  @Test
  public void testActorBucketDeniesWhenExhaustedAndIsolatesActors() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getActor(), 1);
    RateLimitEngine engine = engine(config);

    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
    RateLimitDecision denied = graphql(engine, "getMe", ALICE, null);
    assertFalse(denied.isAllowed());
    assertEquals(denied.getDenyingRuleId(), "scoped:actor");

    // A different actor has its own bucket and is unaffected.
    assertTrue(graphql(engine, "getMe", BOB, null).isAllowed());
  }

  @Test
  public void testClassBucketsBrowserAndSdkAreSeparate() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getBrowser(), 5);
    enable(config.getScoped().getSdk(), 1);
    RateLimitEngine engine = engine(config);

    // SDK (non-browser) exhausts its capacity-1 bucket on the second call.
    assertTrue(graphql(engine, "getMe", ALICE, ClientClass.NON_BROWSER).isAllowed());
    RateLimitDecision sdkDenied = graphql(engine, "getMe", ALICE, ClientClass.NON_BROWSER);
    assertFalse(sdkDenied.isAllowed());
    assertEquals(sdkDenied.getDenyingRuleId(), "scoped:sdk");

    // Browser traffic uses a separate, more generous bucket and is unaffected.
    assertTrue(graphql(engine, "getMe", BOB, ClientClass.BROWSER).isAllowed());
  }

  @Test
  public void testGlobalBucketIsFleetWideAcrossActors() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getGlobal(), 1);
    RateLimitEngine engine = engine(config);

    // The global bucket is shared across actors, so a second actor is denied once it is exhausted.
    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
    RateLimitDecision denied = graphql(engine, "getMe", BOB, null);
    assertFalse(denied.isAllowed());
    assertEquals(denied.getDenyingRuleId(), "scoped:global");
  }

  @Test
  public void testScopedChainInactiveWhenDisabled() {
    RateLimitProperties config = scopedConfig();
    config.getScoped().setEnabled(false);
    // Even with a capacity-1 actor bucket configured, a disabled chain enforces nothing.
    enable(config.getScoped().getActor(), 1);
    RateLimitEngine engine = engine(config);

    assertFalse(engine.isEnabled());
    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
  }

  @Test
  public void testPerLimiterDisableSkipsThatBucket() {
    RateLimitProperties config = scopedConfig();
    // Global is sized to 1 but explicitly disabled, so it must not be consumed.
    config.getScoped().getGlobal().setCapacity(1);
    config.getScoped().getGlobal().setRefillTokens(1);
    config.getScoped().getGlobal().setDisabled(true);
    // Keep actor active so the chain has at least one enabled bucket.
    enable(config.getScoped().getActor(), 1000);
    RateLimitEngine engine = engine(config);

    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
    assertTrue(graphql(engine, "getMe", BOB, null).isAllowed());
    assertTrue(graphql(engine, "getMe", "urn:li:corpuser:carol", null).isAllowed());
  }

  @Test
  public void testRefundKeepsUpstreamBucketWhenLaterStageDenies() {
    RateLimitProperties config = scopedConfig();
    // refund is on by default. Actor is generous; global is the bottleneck.
    enable(config.getScoped().getActor(), 3);
    enable(config.getScoped().getGlobal(), 1);
    RateLimitEngine engine = engine(config);

    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
    // Every subsequent request consumes actor, is denied at global, and the actor token is
    // refunded,
    // so the actor bucket never exhausts and the denial stays at the global stage.
    for (int i = 0; i < 5; i++) {
      RateLimitDecision denied = graphql(engine, "getMe", ALICE, null);
      assertFalse(denied.isAllowed());
      assertEquals(denied.getDenyingRuleId(), "scoped:global", "iteration " + i);
    }
  }

  @Test
  public void testNoRefundExhaustsUpstreamBucket() {
    RateLimitProperties config = scopedConfig();
    config.getScoped().setRefundDisabled(true);
    enable(config.getScoped().getActor(), 3);
    enable(config.getScoped().getGlobal(), 1);
    RateLimitEngine engine = engine(config);

    // req1 allowed (actor 3->2, global 1->0).
    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
    // req2, req3 denied at global; actor is debited each time and NOT refunded (3->2->1->0).
    assertEquals(graphql(engine, "getMe", ALICE, null).getDenyingRuleId(), "scoped:global");
    assertEquals(graphql(engine, "getMe", ALICE, null).getDenyingRuleId(), "scoped:global");
    // req4: actor is now exhausted, so the chain denies at the actor stage instead.
    assertEquals(graphql(engine, "getMe", ALICE, null).getDenyingRuleId(), "scoped:actor");
  }

  @Test
  public void testTenantScopedBucketsIsolatedAcrossTenants() {
    hazelcastInstance = HazelcastTestSupport.createIsolatedInstance();

    RateLimitProperties tenant1 = scopedConfig();
    tenant1.setTenantId("tenant-1");
    enable(tenant1.getScoped().getActor(), 1);

    RateLimitProperties tenant2 = scopedConfig();
    tenant2.setTenantId("tenant-2");
    enable(tenant2.getScoped().getActor(), 1);

    RateLimitEngine engine1 =
        new RateLimitEngine(
            tenant1, "", null, hazelcastInstance, ObjectMapperContext.defaultMapper);
    RateLimitEngine engine2 =
        new RateLimitEngine(
            tenant2, "", null, hazelcastInstance, ObjectMapperContext.defaultMapper);

    // Same actor urn, same shared Hazelcast — but tenant-prefixed keys keep the buckets
    // independent.
    assertTrue(graphql(engine1, "getMe", ALICE, null).isAllowed());
    assertFalse(graphql(engine1, "getMe", ALICE, null).isAllowed());
    assertTrue(graphql(engine2, "getMe", ALICE, null).isAllowed());
  }

  @Test
  public void testMasterKillSwitchBypassesEverything() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getActor(), 1);
    config.setEnabled(false); // master off

    RateLimitEngine engine = engine(config);

    assertFalse(engine.isEnabled());
    // The capacity-1 actor bucket would deny the second call, but the master switch bypasses all.
    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
  }

  @Test
  public void testActorBucketAppliesToNonUserActors() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getActor(), 1);
    RateLimitEngine engine = engine(config);

    // A non-USER principal (service/role) must be bucketed like any other actor — no free pass.
    String service = "urn:li:service:ingestion-bot";
    assertTrue(graphql(engine, "getMe", service, null).isAllowed());
    RateLimitDecision denied = graphql(engine, "getMe", service, null);
    assertFalse(denied.isAllowed());
    assertEquals(denied.getDenyingRuleId(), "scoped:actor");
  }

  @Test
  public void testRestScrollPathCoveredByFrontGateGlobalLane() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getGlobal(), 1);
    RateLimitEngine engine = engine(config);

    // REST search/scroll endpoints also flow through the front gate (no per-resolver heavy bucket,
    // but the fleet-global lane applies), so they can't sidestep rate limiting.
    assertTrue(engine.evaluateAndAcquireRest("/openapi/v3/entity/scroll", "POST").isAllowed());
    RateLimitDecision denied = engine.evaluateAndAcquireRest("/openapi/v3/entity/scroll", "POST");
    assertFalse(denied.isAllowed());
    assertEquals(denied.getDenyingRuleId(), "scoped:global");
  }

  @Test
  public void testConsumeHeavyResolverEnforcesConfiguredResolverOnly() {
    RateLimitProperties config = scopedConfig();
    config.getScoped().getHeavyResolvers().put("searchAcrossEntities", bucketLimits(1));
    RateLimitEngine engine = engine(config);

    // The configured heavy resolver consumes its bucket and is denied on the second call.
    assertNull(engine.consumeHeavyResolver("searchAcrossEntities", false));
    RateLimitDecision denied = engine.consumeHeavyResolver("searchAcrossEntities", false);
    assertFalse(denied.isAllowed());
    assertEquals(denied.getDenyingRuleId(), "scoped:op:searchAcrossEntities");
    // Retry-After is populated from Bucket4j's refill estimate (floored at minRetryAfterSeconds),
    // so the 429 the controller returns carries a usable Retry-After header.
    assertNotNull(denied.getRetryAfterSeconds());
    assertTrue(denied.getRetryAfterSeconds() >= config.getMinRetryAfterSeconds());

    // A resolver that isn't configured heavy is never limited.
    assertNull(engine.consumeHeavyResolver("getMe", false));
  }

  @Test
  public void testConsumeHeavyResolverExemptsSystemActorWhenConfigured() {
    RateLimitProperties config = scopedConfig();
    RateLimitProperties.BucketLimits limits = bucketLimits(1);
    limits.setExemptSystemActor(true);
    config.getScoped().getHeavyResolvers().put("searchAcrossEntities", limits);
    RateLimitEngine engine = engine(config);

    // The system principal bypasses the bucket entirely — repeated calls are never charged/denied.
    assertNull(engine.consumeHeavyResolver("searchAcrossEntities", true));
    assertNull(engine.consumeHeavyResolver("searchAcrossEntities", true));

    // A normal actor is still subject to the capacity-1 bucket.
    assertNull(engine.consumeHeavyResolver("searchAcrossEntities", false));
    RateLimitDecision denied = engine.consumeHeavyResolver("searchAcrossEntities", false);
    assertFalse(denied.isAllowed());
    assertEquals(denied.getDenyingRuleId(), "scoped:op:searchAcrossEntities");
  }

  @Test
  public void testConsumeHeavyResolverNoOpWhenScopedDisabled() {
    RateLimitProperties config = scopedConfig();
    config.getScoped().setEnabled(false);
    config.getScoped().getHeavyResolvers().put("searchAcrossEntities", bucketLimits(1));
    RateLimitEngine engine = engine(config);

    assertNull(engine.consumeHeavyResolver("searchAcrossEntities", false));
    assertNull(engine.consumeHeavyResolver("searchAcrossEntities", false));
  }

  private RateLimitDecision graphql(
      RateLimitEngine engine, String operation, String actorUrn, ClientClass clientClass) {
    return engine.evaluateAndAcquireGraphQL(GRAPHQL_PATH, "POST", operation, actorUrn, clientClass);
  }

  private RateLimitEngine engine(RateLimitProperties config) {
    hazelcastInstance = HazelcastTestSupport.createIsolatedInstance();
    return new RateLimitEngine(
        config, "", null, hazelcastInstance, ObjectMapperContext.defaultMapper);
  }

  /** Scoped chain enabled, every other limiter off, all four scoped buckets disabled by default. */
  private RateLimitProperties scopedConfig() {
    RateLimitProperties config = new RateLimitProperties();
    config.setFailOpen(false);
    config.setRetryAfterJitterPercent(0);
    config.getCapacity().setEnabled(false);
    config.getCapacity().getGraphql().setPathPattern(GRAPHQL_PATH);
    config.getEndpoint().setEnabled(false);
    RateLimitProperties.ScopedLimits scoped = config.getScoped();
    scoped.setEnabled(true);
    scoped.getActor().setDisabled(true);
    scoped.getBrowser().setDisabled(true);
    scoped.getSdk().setDisabled(true);
    scoped.getGlobal().setDisabled(true);
    return config;
  }

  private static void enable(RateLimitProperties.BucketLimits bucket, int capacity) {
    bucket.setDisabled(false);
    bucket.setCapacity(capacity);
    bucket.setRefillTokens(capacity);
    bucket.setRefillPeriodSeconds(60);
  }

  private static RateLimitProperties.BucketLimits bucketLimits(int capacity) {
    return RateLimitProperties.BucketLimits.builder()
        .capacity(capacity)
        .refillTokens(capacity)
        .refillPeriodSeconds(60)
        .build();
  }
}
