package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import io.datahubproject.metadata.context.ObjectMapperContext;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * End-to-end "abuse" smoke tests for the scoped chain: a misbehaving caller bombards the engine and
 * we assert it gets throttled to exactly its bucket capacity while well-behaved traffic is
 * unaffected. Includes a concurrent flood to confirm the distributed bucket enforces its limit
 * atomically under real parallel load.
 */
public class RateLimitAbuseScenarioTest {

  private static final String GRAPHQL_PATH = "/api/graphql";

  private HazelcastInstance hazelcastInstance;

  @AfterMethod
  public void tearDown() {
    HazelcastTestSupport.shutdown(hazelcastInstance);
    hazelcastInstance = null;
  }

  @Test
  public void testSingleActorFloodIsThrottledWhileOthersUnaffected() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getActor(), 10);
    RateLimitEngine engine = engine(config);

    String abuser = "urn:li:corpuser:abuser";
    int allowed = 0;
    int denied = 0;
    String lastDenyRule = null;
    for (int i = 0; i < 50; i++) {
      RateLimitDecision decision = graphql(engine, "getMe", abuser, null);
      if (decision.isAllowed()) {
        allowed++;
      } else {
        denied++;
        lastDenyRule = decision.getDenyingRuleId();
      }
    }

    // Exactly the bucket capacity gets through; the rest of the flood is rejected at the actor
    // stage.
    assertEquals(allowed, 10);
    assertEquals(denied, 40);
    assertEquals(lastDenyRule, "scoped:actor");

    // A well-behaved actor with its own bucket sails through despite the abuser next door.
    String victim = "urn:li:corpuser:victim";
    for (int i = 0; i < 10; i++) {
      assertTrue(graphql(engine, "getMe", victim, null).isAllowed(), "victim request " + i);
    }
  }

  @Test
  public void testSdkFloodDoesNotStarveBrowserTraffic() {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getSdk(), 5);
    enable(config.getScoped().getBrowser(), 100);
    RateLimitEngine engine = engine(config);

    int sdkAllowed = 0;
    for (int i = 0; i < 50; i++) {
      if (graphql(engine, "getMe", "urn:li:corpuser:bot", ClientClass.NON_BROWSER).isAllowed()) {
        sdkAllowed++;
      }
    }
    assertEquals(sdkAllowed, 5, "SDK flood must be capped at the sdk bucket capacity");

    // Interactive browser traffic uses a separate, generous bucket and is unaffected by the flood.
    for (int i = 0; i < 20; i++) {
      assertTrue(
          graphql(engine, "getMe", "urn:li:corpuser:human", ClientClass.BROWSER).isAllowed(),
          "browser request " + i);
    }
  }

  @Test
  public void testManyActorsCollectivelyShedAtFleetGlobalBucket() {
    RateLimitProperties config = scopedConfig();
    // Each actor is well under its own generous limit; the fleet ceiling is what sheds load.
    enable(config.getScoped().getActor(), 1000);
    enable(config.getScoped().getGlobal(), 20);
    RateLimitEngine engine = engine(config);

    int allowed = 0;
    int deniedAtGlobal = 0;
    for (int actor = 0; actor < 5; actor++) {
      String urn = "urn:li:corpuser:actor-" + actor;
      for (int i = 0; i < 10; i++) {
        RateLimitDecision decision = graphql(engine, "getMe", urn, null);
        if (decision.isAllowed()) {
          allowed++;
        } else if ("scoped:global".equals(decision.getDenyingRuleId())) {
          deniedAtGlobal++;
        }
      }
    }

    // 50 requests across 5 actors; only the fleet-wide 20 get through, the other 30 shed at global.
    assertEquals(allowed, 20);
    assertEquals(deniedAtGlobal, 30);
  }

  @Test
  public void testConcurrentFloodRespectsBucketCapacityAtomically() throws Exception {
    RateLimitProperties config = scopedConfig();
    enable(config.getScoped().getActor(), 50);
    RateLimitEngine engine = engine(config);

    String abuser = "urn:li:corpuser:abuser";
    int threads = 8;
    int perThread = 100;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    AtomicInteger allowed = new AtomicInteger();
    try {
      Runnable flood =
          () -> {
            for (int i = 0; i < perThread; i++) {
              if (graphql(engine, "getMe", abuser, null).isAllowed()) {
                allowed.incrementAndGet();
              }
            }
          };
      for (int t = 0; t < threads; t++) {
        pool.submit(flood);
      }
      pool.shutdown();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "flood did not finish in time");
    } finally {
      pool.shutdownNow();
    }

    // 800 concurrent attempts, capacity 50: the distributed bucket must admit exactly 50, no races.
    assertEquals(allowed.get(), 50);
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
}
