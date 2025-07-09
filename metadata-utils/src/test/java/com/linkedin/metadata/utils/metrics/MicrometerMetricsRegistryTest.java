package com.linkedin.metadata.utils.metrics;

import static com.linkedin.metadata.utils.metrics.MicrometerMetricsRegistry.GLOBALLY_REGISTERED_CACHES;
import static com.linkedin.metadata.utils.metrics.MicrometerMetricsRegistry.GLOBALLY_REGISTERED_EXECUTOR_SERVICE;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hazelcast.map.IMap;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MicrometerMetricsRegistryTest {

  @Mock private IMap<Object, Object> hazelcastMap;
  @Mock private ExecutorService mockExecutorService;

  private MeterRegistry meterRegistry;
  private ExecutorService realExecutorService;

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    meterRegistry = new SimpleMeterRegistry();

    // Clear the global registry before each
    clearGlobalRegistry();

    // Create a real executor service for some tests
    realExecutorService = Executors.newFixedThreadPool(2);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    // Shutdown the real executor service
    if (realExecutorService != null && !realExecutorService.isShutdown()) {
      realExecutorService.shutdown();
      realExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private void clearGlobalRegistry() throws Exception {
    GLOBALLY_REGISTERED_CACHES.clear();
    GLOBALLY_REGISTERED_EXECUTOR_SERVICE.clear();
  }

  @Test
  public void testRegisterCaffeineCache() {
    // Given
    String cacheName = "testCaffeineCache";
    Cache<Object, Object> caffeineCache =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();

    // When
    boolean result =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, caffeineCache, meterRegistry);

    // Then
    assertTrue(result);

    // Verify that metrics are registered
    assertFalse(meterRegistry.getMeters().isEmpty());
    assertTrue(
        meterRegistry.getMeters().stream()
            .anyMatch(meter -> meter.getId().getName().startsWith("cache")));
  }

  @Test
  public void testRegisterHazelcastCache() {
    // Given
    String cacheName = "testHazelcastCache";
    when(hazelcastMap.getName()).thenReturn(cacheName);

    // When
    boolean result =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, hazelcastMap, meterRegistry);

    // Then
    assertTrue(result);

    // Verify that metrics are registered
    assertFalse(meterRegistry.getMeters().isEmpty());
    assertTrue(
        meterRegistry.getMeters().stream()
            .anyMatch(meter -> meter.getId().getName().startsWith("cache")));
  }

  @Test
  public void testRegisterCacheOnlyOnce() {
    // Given
    String cacheName = "testCache";
    Cache<Object, Object> caffeineCache =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();

    // When - register the same cache twice
    boolean firstResult =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, caffeineCache, meterRegistry);
    boolean secondResult =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, caffeineCache, meterRegistry);

    // Then
    assertTrue(firstResult);
    assertFalse(secondResult); // Second registration should return false
  }

  @Test
  public void testRegisterMultipleCaches() {
    // Given
    String cacheName1 = "cache1";
    String cacheName2 = "cache2";
    Cache<Object, Object> caffeineCache1 =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();
    Cache<Object, Object> caffeineCache2 =
        Caffeine.newBuilder().maximumSize(200).recordStats().build();

    // When
    boolean result1 =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName1, caffeineCache1, meterRegistry);
    boolean result2 =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName2, caffeineCache2, meterRegistry);

    // Then
    assertTrue(result1);
    assertTrue(result2);

    // Verify that metrics are registered for both caches
    assertFalse(meterRegistry.getMeters().isEmpty());
    assertTrue(meterRegistry.getMeters().size() > 1);
  }

  @Test
  public void testRegisterCacheWithNullMeterRegistry() {
    // Given
    String cacheName = "testCache";
    Cache<Object, Object> caffeineCache =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();

    // When
    boolean result = MicrometerMetricsRegistry.registerCacheMetrics(cacheName, caffeineCache, null);

    // Then
    assertFalse(result);
  }

  @Test
  public void testRegisterCacheWithNullNativeCache() {
    // Given
    String cacheName = "testCache";

    // When
    boolean result = MicrometerMetricsRegistry.registerCacheMetrics(cacheName, null, meterRegistry);

    // Then
    assertFalse(result);
  }

  @Test
  public void testRegisterCacheWithNullCacheName() {
    // Given
    Cache<Object, Object> caffeineCache =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();

    // When
    boolean result =
        MicrometerMetricsRegistry.registerCacheMetrics(null, caffeineCache, meterRegistry);

    // Then
    assertFalse(result); // Should return false for null cache name
  }

  @Test
  public void testRegisterCacheWithUnsupportedCacheType() {
    // Given
    String cacheName = "unsupportedCache";
    Object unsupportedCache = new Object(); // Not a Caffeine or Hazelcast cache

    // When
    boolean result =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, unsupportedCache, meterRegistry);

    // Then
    assertTrue(result); // Should return true but not register any metrics

    // No metrics should be registered for unsupported cache type
    assertTrue(meterRegistry.getMeters().isEmpty());
  }

  @Test
  public void testConcurrentRegistration() throws InterruptedException {
    // Given
    String cacheName = "concurrentCache";
    Cache<Object, Object> caffeineCache =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();

    // When - simulate concurrent registration attempts
    Thread[] threads = new Thread[3];
    boolean[] results = new boolean[3];

    for (int i = 0; i < 3; i++) {
      final int index = i;
      threads[i] =
          new Thread(
              () -> {
                results[index] =
                    MicrometerMetricsRegistry.registerCacheMetrics(
                        cacheName, caffeineCache, meterRegistry);
              });
      threads[i].start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    // Then - only one thread should have successfully registered
    int successCount = 0;
    for (boolean result : results) {
      if (result) successCount++;
    }
    assertEquals(successCount, 1);
  }

  @Test
  public void testMetricsTagsForCaffeineCache() {
    // Given
    String cacheName = "taggedCaffeineCache";
    Cache<Object, Object> caffeineCache =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();

    // When
    boolean result =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, caffeineCache, meterRegistry);

    // Then
    assertTrue(result);
    assertFalse(meterRegistry.getMeters().isEmpty());

    // Check for cache.size metric which is always present
    // CaffeineCacheMetrics uses "cache" as the tag name, not "name"
    assertTrue(
        meterRegistry.getMeters().stream()
            .anyMatch(
                meter ->
                    meter.getId().getName().equals("cache.size")
                        && meter.getId().getTag("cache") != null
                        && meter.getId().getTag("cache").equals(cacheName)));
  }

  @Test
  public void testMetricsTagsForHazelcastCache() {
    // Given
    String cacheName = "taggedHazelcastCache";
    when(hazelcastMap.getName()).thenReturn(cacheName);

    // When
    boolean result =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, hazelcastMap, meterRegistry);

    // Then
    assertTrue(result);
    assertFalse(meterRegistry.getMeters().isEmpty());

    // Verify at least one metric exists with the cache prefix
    assertTrue(
        meterRegistry.getMeters().stream()
            .anyMatch(meter -> meter.getId().getName().startsWith("cache")));

    // Verify the name tag is present on at least one metric
    assertTrue(
        meterRegistry.getMeters().stream()
            .anyMatch(meter -> cacheName.equals(meter.getId().getTag("name"))));
  }

  @Test
  public void testRegisterSameCacheWithDifferentRegistries() {
    // Given
    String cacheName = "multiRegistryCache";
    Cache<Object, Object> caffeineCache =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();
    MeterRegistry secondRegistry = new SimpleMeterRegistry();

    // When
    boolean result1 =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, caffeineCache, meterRegistry);
    boolean result2 =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, caffeineCache, secondRegistry);

    // Then - second should fail since we're using cache name only as the key
    assertTrue(result1);
    assertFalse(result2); // This will fail because the cache name is already registered

    // Only first registry should have metrics
    assertFalse(meterRegistry.getMeters().isEmpty());
    assertTrue(secondRegistry.getMeters().isEmpty());
  }

  @Test
  public void testExceptionHandlingDuringRegistration() {
    // Given
    String cacheName = "exceptionCache";
    // Create a mock cache that will cause an exception during metric registration
    Cache<Object, Object> problematicCache = mock(Cache.class);
    when(problematicCache.estimatedSize()).thenThrow(new RuntimeException("Test exception"));

    // When
    boolean result =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, problematicCache, meterRegistry);

    // Then - should return false and not crash
    assertFalse(result);

    // Attempting to register again should succeed since it was removed from the set
    Cache<Object, Object> goodCache = Caffeine.newBuilder().maximumSize(100).recordStats().build();
    boolean retryResult =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, goodCache, meterRegistry);
    assertTrue(retryResult);
  }

  @Test
  public void testRegistryKeyFormat() throws Exception {
    // Given
    String cacheName = "testCache";
    Cache<Object, Object> caffeineCache =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();

    // Register with first registry
    boolean result1 =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, caffeineCache, meterRegistry);
    assertTrue(result1);

    // Get the global cache set to verify the key format
    Field field = MicrometerMetricsRegistry.class.getDeclaredField("GLOBALLY_REGISTERED_CACHES");
    field.setAccessible(true);
    Set<String> registeredCaches = (Set<String>) field.get(null);

    // Verify the key is just the cache name (no registry hash)
    assertEquals(registeredCaches.size(), 1);
    String registeredKey = registeredCaches.iterator().next();
    assertEquals(registeredKey, cacheName);
  }

  @Test
  public void testAllNullParameters() {
    // When
    boolean result = MicrometerMetricsRegistry.registerCacheMetrics(null, null, null);

    // Then
    assertFalse(result);
  }

  @Test
  public void testNullCheckOrder() {
    // Test various combinations of null parameters

    // Null cache name
    Cache<Object, Object> caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    assertFalse(MicrometerMetricsRegistry.registerCacheMetrics(null, caffeineCache, meterRegistry));

    // Null native cache
    assertFalse(MicrometerMetricsRegistry.registerCacheMetrics("test", null, meterRegistry));

    // Null meter registry
    assertFalse(MicrometerMetricsRegistry.registerCacheMetrics("test", caffeineCache, null));
  }

  @Test
  public void testSuccessfulRegistrationDoesNotThrowOnSubsequentAttempts() {
    // Given
    String cacheName = "testCache";
    Cache<Object, Object> caffeineCache =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();

    // When - register successfully
    boolean firstResult =
        MicrometerMetricsRegistry.registerCacheMetrics(cacheName, caffeineCache, meterRegistry);
    assertTrue(firstResult);

    // Then - subsequent attempts should return false but not throw
    for (int i = 0; i < 5; i++) {
      boolean result =
          MicrometerMetricsRegistry.registerCacheMetrics(cacheName, caffeineCache, meterRegistry);
      assertFalse(result);
    }
  }

  @Test
  public void testConsistentTagsBetweenCaffeineAndHazelcast() {
    // Given
    String caffeineCacheName = "caffeineCache";
    String hazelcastCacheName = "hazelcastCache";

    Cache<Object, Object> caffeineCache =
        Caffeine.newBuilder().maximumSize(100).recordStats().build();
    when(hazelcastMap.getName()).thenReturn(hazelcastCacheName);

    // When
    boolean caffeineResult =
        MicrometerMetricsRegistry.registerCacheMetrics(
            caffeineCacheName, caffeineCache, meterRegistry);
    boolean hazelcastResult =
        MicrometerMetricsRegistry.registerCacheMetrics(
            hazelcastCacheName, hazelcastMap, meterRegistry);

    // Then
    assertTrue(caffeineResult);
    assertTrue(hazelcastResult);

    // Collect all unique tag keys from Caffeine metrics
    Set<String> caffeineTagKeys =
        meterRegistry.getMeters().stream()
            .filter(meter -> meter.getId().getName().startsWith("cache"))
            .filter(meter -> caffeineCacheName.equals(meter.getId().getTag("cache")))
            .flatMap(meter -> meter.getId().getTags().stream())
            .map(Tag::getKey)
            .collect(java.util.stream.Collectors.toSet());

    // Collect all unique tag keys from Hazelcast metrics
    Set<String> hazelcastTagKeys =
        meterRegistry.getMeters().stream()
            .filter(meter -> meter.getId().getName().startsWith("cache"))
            .filter(meter -> hazelcastCacheName.equals(meter.getId().getTag("cache")))
            .flatMap(meter -> meter.getId().getTags().stream())
            .map(Tag::getKey)
            .collect(java.util.stream.Collectors.toSet());

    // Hazelcast has an additional "ownership" tag that Caffeine doesn't have
    // Remove it for comparison of common tags
    Set<String> hazelcastCommonTags = new java.util.HashSet<>(hazelcastTagKeys);
    hazelcastCommonTags.remove("ownership");

    // Verify that the common tags are the same
    assertEquals(
        caffeineTagKeys,
        hazelcastCommonTags,
        "Common tag keys should be identical between Caffeine and Hazelcast metrics");

    // Verify Hazelcast has the ownership tag
    assertTrue(
        hazelcastTagKeys.contains("ownership"), "Hazelcast metrics should have 'ownership' tag");
  }

  @Test
  public void testRegisterExecutorService() {
    // Given
    String executorName = "testExecutor";

    // When
    boolean result =
        MicrometerMetricsRegistry.registerExecutorMetrics(
            executorName, realExecutorService, meterRegistry);

    // Then
    assertTrue(result);

    // Verify that metrics are registered
    assertFalse(meterRegistry.getMeters().isEmpty());
    assertTrue(
        meterRegistry.getMeters().stream()
            .anyMatch(meter -> meter.getId().getName().startsWith("executor")));
  }

  @Test
  public void testRegisterExecutorServiceOnlyOnce() {
    // Given
    String executorName = "testExecutor";

    // When - register the same executor twice
    boolean firstResult =
        MicrometerMetricsRegistry.registerExecutorMetrics(
            executorName, realExecutorService, meterRegistry);
    boolean secondResult =
        MicrometerMetricsRegistry.registerExecutorMetrics(
            executorName, realExecutorService, meterRegistry);

    // Then
    assertTrue(firstResult);
    assertFalse(secondResult); // Second registration should return false
  }

  @Test
  public void testRegisterMultipleExecutors() {
    // Given
    String executorName1 = "executor1";
    String executorName2 = "executor2";
    ExecutorService executorService2 = Executors.newCachedThreadPool();

    try {
      // When
      boolean result1 =
          MicrometerMetricsRegistry.registerExecutorMetrics(
              executorName1, realExecutorService, meterRegistry);
      boolean result2 =
          MicrometerMetricsRegistry.registerExecutorMetrics(
              executorName2, executorService2, meterRegistry);

      // Then
      assertTrue(result1);
      assertTrue(result2);

      // Verify that metrics are registered for both executors
      assertFalse(meterRegistry.getMeters().isEmpty());
      assertTrue(meterRegistry.getMeters().size() > 1);
    } finally {
      executorService2.shutdown();
    }
  }

  @Test
  public void testRegisterExecutorWithNullMeterRegistry() {
    // Given
    String executorName = "testExecutor";

    // When
    boolean result =
        MicrometerMetricsRegistry.registerExecutorMetrics(executorName, realExecutorService, null);

    // Then
    assertFalse(result);
  }

  @Test
  public void testRegisterExecutorWithNullExecutorService() {
    // Given
    String executorName = "testExecutor";

    // When
    boolean result =
        MicrometerMetricsRegistry.registerExecutorMetrics(executorName, null, meterRegistry);

    // Then
    assertFalse(result);
  }

  @Test
  public void testRegisterExecutorWithNullExecutorName() {
    // When
    boolean result =
        MicrometerMetricsRegistry.registerExecutorMetrics(null, realExecutorService, meterRegistry);

    // Then
    assertFalse(result); // Should return false for null executor name
  }

  @Test
  public void testConcurrentExecutorRegistration() throws InterruptedException {
    // Given
    String executorName = "concurrentExecutor";

    // When - simulate concurrent registration attempts
    Thread[] threads = new Thread[3];
    boolean[] results = new boolean[3];

    for (int i = 0; i < 3; i++) {
      final int index = i;
      threads[i] =
          new Thread(
              () -> {
                results[index] =
                    MicrometerMetricsRegistry.registerExecutorMetrics(
                        executorName, realExecutorService, meterRegistry);
              });
      threads[i].start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    // Then - only one thread should have successfully registered
    int successCount = 0;
    for (boolean result : results) {
      if (result) successCount++;
    }
    assertEquals(successCount, 1);
  }

  @Test
  public void testExecutorMetricsTags() {
    // Given
    String executorName = "taggedExecutor";

    // When
    boolean result =
        MicrometerMetricsRegistry.registerExecutorMetrics(
            executorName, realExecutorService, meterRegistry);

    // Then
    assertTrue(result);
    assertFalse(meterRegistry.getMeters().isEmpty());

    // Check for executor metrics with proper tags
    assertTrue(
        meterRegistry.getMeters().stream()
            .anyMatch(
                meter ->
                    meter.getId().getName().startsWith("executor")
                        && meter.getId().getTag("name") != null
                        && meter.getId().getTag("name").equals(executorName)));
  }

  @Test
  public void testRegisterSameExecutorWithDifferentRegistries() {
    // Given
    String executorName = "multiRegistryExecutor";
    MeterRegistry secondRegistry = new SimpleMeterRegistry();

    // When
    boolean result1 =
        MicrometerMetricsRegistry.registerExecutorMetrics(
            executorName, realExecutorService, meterRegistry);
    boolean result2 =
        MicrometerMetricsRegistry.registerExecutorMetrics(
            executorName, realExecutorService, secondRegistry);

    // Then - second should fail since we're using executor name only as the key
    assertTrue(result1);
    assertFalse(result2); // This will fail because the executor name is already registered

    // Only first registry should have metrics
    assertFalse(meterRegistry.getMeters().isEmpty());
    assertTrue(secondRegistry.getMeters().isEmpty());
  }

  @Test
  public void testExecutorRegistryKeyFormat() throws Exception {
    // Given
    String executorName = "testExecutor";

    // Register with first registry
    boolean result1 =
        MicrometerMetricsRegistry.registerExecutorMetrics(
            executorName, realExecutorService, meterRegistry);
    assertTrue(result1);

    // Get the global executor set to verify the key format
    Field field =
        MicrometerMetricsRegistry.class.getDeclaredField("GLOBALLY_REGISTERED_EXECUTOR_SERVICE");
    field.setAccessible(true);
    Set<String> registeredExecutors = (Set<String>) field.get(null);

    // Verify the key is just the executor name (no registry hash)
    assertEquals(registeredExecutors.size(), 1);
    String registeredKey = registeredExecutors.iterator().next();
    assertEquals(registeredKey, executorName);
  }

  @Test
  public void testAllNullParametersForExecutor() {
    // When
    boolean result = MicrometerMetricsRegistry.registerExecutorMetrics(null, null, null);

    // Then
    assertFalse(result);
  }

  @Test
  public void testExecutorNullCheckOrder() {
    // Test various combinations of null parameters

    // Null executor name
    assertFalse(
        MicrometerMetricsRegistry.registerExecutorMetrics(
            null, realExecutorService, meterRegistry));

    // Null executor service
    assertFalse(MicrometerMetricsRegistry.registerExecutorMetrics("test", null, meterRegistry));

    // Null meter registry
    assertFalse(
        MicrometerMetricsRegistry.registerExecutorMetrics("test", realExecutorService, null));
  }

  @Test
  public void testSuccessfulExecutorRegistrationDoesNotThrowOnSubsequentAttempts() {
    // Given
    String executorName = "testExecutor";

    // When - register successfully
    boolean firstResult =
        MicrometerMetricsRegistry.registerExecutorMetrics(
            executorName, realExecutorService, meterRegistry);
    assertTrue(firstResult);

    // Then - subsequent attempts should return false but not throw
    for (int i = 0; i < 5; i++) {
      boolean result =
          MicrometerMetricsRegistry.registerExecutorMetrics(
              executorName, realExecutorService, meterRegistry);
      assertFalse(result);
    }
  }

  @Test
  public void testDifferentExecutorTypes() {
    // Test with different executor implementations
    String fixedPoolName = "fixedPool";
    String cachedPoolName = "cachedPool";
    String singleThreadName = "singleThread";

    ExecutorService cachedPool = Executors.newCachedThreadPool();
    ExecutorService singleThread = Executors.newSingleThreadExecutor();

    try {
      // When
      boolean fixedResult =
          MicrometerMetricsRegistry.registerExecutorMetrics(
              fixedPoolName, realExecutorService, meterRegistry);
      boolean cachedResult =
          MicrometerMetricsRegistry.registerExecutorMetrics(
              cachedPoolName, cachedPool, meterRegistry);
      boolean singleResult =
          MicrometerMetricsRegistry.registerExecutorMetrics(
              singleThreadName, singleThread, meterRegistry);

      // Then
      assertTrue(fixedResult);
      assertTrue(cachedResult);
      assertTrue(singleResult);

      // Verify metrics are present for all executor types
      assertTrue(
          meterRegistry.getMeters().stream()
              .anyMatch(meter -> fixedPoolName.equals(meter.getId().getTag("name"))));
      assertTrue(
          meterRegistry.getMeters().stream()
              .anyMatch(meter -> cachedPoolName.equals(meter.getId().getTag("name"))));
      assertTrue(
          meterRegistry.getMeters().stream()
              .anyMatch(meter -> singleThreadName.equals(meter.getId().getTag("name"))));
    } finally {
      cachedPool.shutdown();
      singleThread.shutdown();
    }
  }

  @Test
  public void testThreadPoolExecutorSpecificMetrics() {
    // Given
    String executorName = "threadPoolExecutor";
    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

    try {
      // When
      boolean result =
          MicrometerMetricsRegistry.registerExecutorMetrics(
              executorName, threadPoolExecutor, meterRegistry);

      // Then
      assertTrue(result);

      // Verify ThreadPoolExecutor specific metrics are present
      // ExecutorServiceMetrics should register metrics like pool.size, active, queued, etc.
      assertTrue(
          meterRegistry.getMeters().stream()
              .anyMatch(
                  meter ->
                      meter.getId().getName().contains("executor.pool.size")
                          && executorName.equals(meter.getId().getTag("name"))));
    } finally {
      threadPoolExecutor.shutdown();
    }
  }

  @Test
  public void testExecutorMetricsAfterShutdown() throws InterruptedException {
    // Given
    String executorName = "shutdownExecutor";
    ExecutorService executorToShutdown = Executors.newSingleThreadExecutor();

    // Register metrics before shutdown
    boolean result =
        MicrometerMetricsRegistry.registerExecutorMetrics(
            executorName, executorToShutdown, meterRegistry);
    assertTrue(result);

    // When - shutdown the executor
    executorToShutdown.shutdown();
    executorToShutdown.awaitTermination(5, TimeUnit.SECONDS);

    // Then - metrics should still be present (they might show terminated state)
    assertFalse(meterRegistry.getMeters().isEmpty());
    assertTrue(
        meterRegistry.getMeters().stream()
            .anyMatch(
                meter ->
                    meter.getId().getName().startsWith("executor")
                        && executorName.equals(meter.getId().getTag("name"))));
  }
}
