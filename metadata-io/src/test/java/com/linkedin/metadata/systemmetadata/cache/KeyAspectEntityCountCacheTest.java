package com.linkedin.metadata.systemmetadata.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.cache.KeyAspectEntityCountCacheConfiguration;
import com.linkedin.metadata.config.cache.KeyAspectEntityCountSingleFlightConfiguration;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KeyAspectEntityCountCacheTest {

  private HazelcastInstance hazelcast;
  private KeyAspectEntityCountCache cache;
  private KeyAspectEntityCountCacheKey cacheKey;

  @BeforeMethod
  public void setUp() {
    Config config = new Config();
    config.setInstanceName("entity-count-cache-test-" + java.util.UUID.randomUUID());
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    hazelcast = Hazelcast.newHazelcastInstance(config);

    KeyAspectEntityCountCacheConfiguration cacheConfig =
        new KeyAspectEntityCountCacheConfiguration();
    cacheConfig.setEnabled(true);
    cacheConfig.setTtlSeconds(3600);
    KeyAspectEntityCountSingleFlightConfiguration singleFlight =
        new KeyAspectEntityCountSingleFlightConfiguration();
    singleFlight.setStaleBuildingMillis(120_000L);
    singleFlight.setWaiterMaxMillis(5_000L);
    singleFlight.setPollIntervalMillis(50L);
    cacheConfig.setSingleFlight(singleFlight);

    cache = new KeyAspectEntityCountCache(cacheConfig, hazelcast);
    cacheKey =
        KeyAspectEntityCountCacheKey.of(
            TestOperationContexts.systemContextNoSearchAuthorization().getSearchContextId(),
            List.of("dataset"));
  }

  @AfterMethod
  public void tearDown() {
    if (hazelcast != null) {
      hazelcast.shutdown();
    }
  }

  @Test
  public void skipCacheBypassesResultCache() {
    AtomicInteger loads = new AtomicInteger();
    KeyAspectEntityCountCache.CountLoader loader = () -> sampleResult(loads.incrementAndGet());

    cache.get(TestOperationContexts.systemContextNoSearchAuthorization(), cacheKey, true, loader);
    cache.get(TestOperationContexts.systemContextNoSearchAuthorization(), cacheKey, true, loader);

    assertEquals(loads.get(), 2);
  }

  @Test
  public void cachedSecondCallSkipsLoader() {
    AtomicInteger loads = new AtomicInteger();
    KeyAspectEntityCountCache.CountLoader loader = () -> sampleResult(loads.incrementAndGet());

    cache.get(TestOperationContexts.systemContextNoSearchAuthorization(), cacheKey, false, loader);
    KeyAspectEntityCountResult second =
        cache.get(
            TestOperationContexts.systemContextNoSearchAuthorization(), cacheKey, false, loader);

    assertEquals(loads.get(), 1);
    assertTrue(second.isCacheHit());
  }

  @Test
  public void tryClaimQueryAllowsSingleWinner() {
    KeyAspectEntityCountCacheKey key = KeyAspectEntityCountCacheKey.of("ctx", List.of("chart"));
    assertTrue(cache.tryClaimQuery(key, "claimant-a", 60_000L));
    assertFalse(cache.tryClaimQuery(key, "claimant-b", 60_000L));
    cache.releaseInFlight(key);
    assertTrue(cache.tryClaimQuery(key, "claimant-c", 60_000L));
    cache.releaseInFlight(key);
  }

  @Test
  public void cacheKeyUsesAllTypesSentinelForEmptyList() {
    KeyAspectEntityCountCacheKey key = KeyAspectEntityCountCacheKey.of("ctx", List.of());
    assertEquals(key.getEntityTypesKey(), KeyAspectEntityCountCacheKey.ALL_TYPES);
  }

  @Test
  public void localSingleFlightDeduplicatesConcurrentLoads() {
    KeyAspectEntityCountCacheConfiguration cacheConfig =
        new KeyAspectEntityCountCacheConfiguration();
    cacheConfig.setEnabled(true);
    cacheConfig.setTtlSeconds(3600);
    KeyAspectEntityCountCache localCache = new KeyAspectEntityCountCache(cacheConfig, null);
    KeyAspectEntityCountCacheKey key = KeyAspectEntityCountCacheKey.of("local", List.of("dataset"));
    AtomicInteger loads = new AtomicInteger();
    KeyAspectEntityCountCache.CountLoader loader = () -> sampleResult(loads.incrementAndGet());

    localCache.get(TestOperationContexts.systemContextNoSearchAuthorization(), key, false, loader);
    localCache.get(TestOperationContexts.systemContextNoSearchAuthorization(), key, false, loader);

    assertEquals(loads.get(), 1);
  }

  @Test
  public void disabledCacheAlwaysInvokesLoader() {
    KeyAspectEntityCountCacheConfiguration cacheConfig =
        new KeyAspectEntityCountCacheConfiguration();
    cacheConfig.setEnabled(false);
    KeyAspectEntityCountCache disabledCache = new KeyAspectEntityCountCache(cacheConfig, hazelcast);
    AtomicInteger loads = new AtomicInteger();

    disabledCache.get(
        TestOperationContexts.systemContextNoSearchAuthorization(),
        cacheKey,
        false,
        () -> sampleResult(loads.incrementAndGet()));
    disabledCache.get(
        TestOperationContexts.systemContextNoSearchAuthorization(),
        cacheKey,
        false,
        () -> sampleResult(loads.incrementAndGet()));

    assertEquals(loads.get(), 2);
  }

  @Test
  public void staleBuildingClaimCanBeTakenOver() {
    KeyAspectEntityCountCacheKey key = KeyAspectEntityCountCacheKey.of("ctx", List.of("dataset"));
    assertTrue(cache.tryClaimQuery(key, "first", 1L));
    assertTrue(cache.tryClaimQuery(key, "second", 1L));
    cache.releaseInFlight(key);
  }

  @Test
  public void distributedSingleFlightWaiterGetsCachedResult() throws Exception {
    KeyAspectEntityCountCacheKey key =
        KeyAspectEntityCountCacheKey.of("waiter", List.of("dataset"));
    java.util.concurrent.CountDownLatch loaderStarted = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch releaseLoader = new java.util.concurrent.CountDownLatch(1);
    AtomicInteger loads = new AtomicInteger();

    Thread claimant =
        new Thread(
            () ->
                cache.get(
                    TestOperationContexts.systemContextNoSearchAuthorization(),
                    key,
                    false,
                    () -> {
                      loads.incrementAndGet();
                      loaderStarted.countDown();
                      try {
                        releaseLoader.await();
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      return sampleResult(5);
                    }));

    Thread waiter =
        new Thread(
            () ->
                cache.get(
                    TestOperationContexts.systemContextNoSearchAuthorization(),
                    key,
                    false,
                    () -> {
                      loads.incrementAndGet();
                      return sampleResult(99);
                    }));

    claimant.start();
    assertTrue(loaderStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));
    waiter.start();
    releaseLoader.countDown();
    claimant.join(5_000);
    waiter.join(5_000);

    assertEquals(loads.get(), 1);
  }

  private static KeyAspectEntityCountResult sampleResult(int activeCount) {
    return KeyAspectEntityCountResult.builder()
        .counts(
            List.of(
                KeyAspectEntityCountEntry.builder()
                    .entityType("dataset")
                    .keyAspect("datasetKey")
                    .activeCount(activeCount)
                    .softDeletedCount(0L)
                    .build()))
        .requestedTypes(List.of("dataset"))
        .computedAt(Instant.now())
        .cacheHit(false)
        .build();
  }
}
