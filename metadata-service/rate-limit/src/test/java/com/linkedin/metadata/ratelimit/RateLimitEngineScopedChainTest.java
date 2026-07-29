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
  public void testClassBucketSharedAcrossActorsByDefault() {
    RateLimitProperties config = scopedConfig();
    // Default (sdk.perActor=false): one shared sdk ceiling per tenant.
    enable(config.getScoped().getSdk(), 1);
    RateLimitEngine engine = engine(config);

    // Alice consumes the single shared sdk token; Bob is denied on the SAME bucket.
    assertTrue(graphql(engine, "getMe", ALICE, ClientClass.NON_BROWSER).isAllowed());
    RateLimitDecision denied = graphql(engine, "getMe", BOB, ClientClass.NON_BROWSER);
    assertFalse(denied.isAllowed());
    assertEquals(denied.getDenyingRuleId(), "scoped:sdk");
  }

  @Test
  public void testClassBucketPerActorWhenFlagEnabled() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getSdk(), 1);
    config.getScoped().getSdk().setPerActor(true); // key the sdk class bucket per actor
    RateLimitEngine engine = engine(config);

    // Each actor now has its own sdk bucket, so Alice and Bob both pass their first call...
    assertTrue(graphql(engine, "getMe", ALICE, ClientClass.NON_BROWSER).isAllowed());
    assertTrue(graphql(engine, "getMe", BOB, ClientClass.NON_BROWSER).isAllowed());
    // ...and Alice's own bucket (not a shared one) is what denies her second call.
    RateLimitDecision aliceDenied = graphql(engine, "getMe", ALICE, ClientClass.NON_BROWSER);
    assertFalse(aliceDenied.isAllowed());
    assertEquals(aliceDenied.getDenyingRuleId(), "scoped:sdk");
  }

  @Test
  public void testPerActorIsIndependentPerClass() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getSdk(), 1);
    enable(config.getScoped().getBrowser(), 1);
    config.getScoped().getSdk().setPerActor(true); // sdk: per-actor
    // browser: left shared (perActor=false) — the flag is set independently per class.
    RateLimitEngine engine = engine(config);

    // sdk is per-actor → Alice and Bob each get their own sdk bucket, both pass.
    assertTrue(graphql(engine, "getMe", ALICE, ClientClass.NON_BROWSER).isAllowed());
    assertTrue(graphql(engine, "getMe", BOB, ClientClass.NON_BROWSER).isAllowed());
    // browser is shared → Alice takes the single token, Bob is denied on the same bucket.
    assertTrue(graphql(engine, "getMe", ALICE, ClientClass.BROWSER).isAllowed());
    assertEquals(
        graphql(engine, "getMe", BOB, ClientClass.BROWSER).getDenyingRuleId(), "scoped:browser");
  }

  @Test
  public void testClassBucketSkippedWhenClientClassDisabled() {
    RateLimitProperties config = scopedConfig();
    // Class discrimination off: the scoped class step must be skipped entirely, so a (spoofable)
    // BROWSER header cannot route a caller into the browser bucket — nor can it be charged the sdk
    // one. Here the browser bucket is capacity-1; with the gate off, a second BROWSER request must
    // still pass because the class step never runs.
    config.setClientClassEnabled(false);
    enable(config.getScoped().getBrowser(), 1);
    RateLimitEngine engine = engine(config);

    assertTrue(graphql(engine, "getMe", ALICE, ClientClass.BROWSER).isAllowed());
    assertTrue(graphql(engine, "getMe", ALICE, ClientClass.BROWSER).isAllowed());
  }

  @Test
  public void testRefundScopedChainRestoresConsumedTokens() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getActor(), 1);
    RateLimitEngine engine = engine(config);

    // Spend the single actor token.
    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
    // Refund it (as the heavy-resolver gate does when it rejects a request that already passed the
    // scoped chain), so the next request is not falsely denied by a quota a rejected request
    // burned.
    engine.refundScopedChain(ALICE, null);
    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
  }

  @Test
  public void testRefundScopedChainIsNoOpWhenRefundDisabled() {
    RateLimitProperties config = scopedConfig();
    config.getScoped().setRefundDisabled(true);
    enable(config.getScoped().getActor(), 1);
    RateLimitEngine engine = engine(config);

    assertTrue(graphql(engine, "getMe", ALICE, null).isAllowed());
    engine.refundScopedChain(ALICE, null); // refunds disabled → no-op
    assertFalse(graphql(engine, "getMe", ALICE, null).isAllowed());
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
  public void testRestPerActorBucketDeniesAndIsolatesActors() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getActor(), 1);
    RateLimitEngine engine = engine(config);

    // REST/OpenAPI carries the authenticated actor now, so the per-actor bucket applies (not just
    // GraphQL). Alice's capacity-1 bucket denies her second REST call, while Bob is unaffected.
    assertTrue(
        engine
            .evaluateAndAcquireRest("/openapi/v3/entity/scroll", "POST", null, ALICE)
            .isAllowed());
    RateLimitDecision denied =
        engine.evaluateAndAcquireRest("/openapi/v3/entity/scroll", "POST", null, ALICE);
    assertFalse(denied.isAllowed());
    assertEquals(denied.getDenyingRuleId(), "scoped:actor");

    assertTrue(
        engine.evaluateAndAcquireRest("/openapi/v3/entity/scroll", "POST", null, BOB).isAllowed());
  }

  @Test
  public void testRestNullActorSkipsPerActorStep() {
    RateLimitProperties config = scopedConfig();
    // Actor bucket is capacity-1; with a null actor (system/unauthenticated) the actor step is
    // skipped, so repeated REST calls are not per-actor throttled.
    enable(config.getScoped().getActor(), 1);
    RateLimitEngine engine = engine(config);

    assertTrue(
        engine.evaluateAndAcquireRest("/openapi/v3/entity/scroll", "POST", null, null).isAllowed());
    assertTrue(
        engine.evaluateAndAcquireRest("/openapi/v3/entity/scroll", "POST", null, null).isAllowed());
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
  public void testHeavyResolverStaysPerTenantEvenIfPerActorSet() {
    RateLimitProperties config = scopedConfig();
    RateLimitProperties.BucketLimits limits = bucketLimits(1);
    limits.setPerActor(true); // must be IGNORED for heavy resolvers (per-tenant by design)
    config.getScoped().getHeavyResolvers().put("searchAcrossEntities", limits);
    RateLimitEngine engine = engine(config);

    // The bucket is {tenant}:op:{resolver} regardless of perActor — no actor in the key — so the
    // capacity-1 tenant bucket is exhausted on the second call. A tenant can't multiply this budget
    // by using more users.
    assertNull(engine.consumeHeavyResolver("searchAcrossEntities", false));
    RateLimitDecision denied = engine.consumeHeavyResolver("searchAcrossEntities", false);
    assertFalse(denied.isAllowed());
    assertEquals(denied.getDenyingRuleId(), "scoped:op:searchAcrossEntities");
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
    // Class (browser/sdk) buckets only apply when client-class discrimination is enabled.
    config.setClientClassEnabled(true);
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
