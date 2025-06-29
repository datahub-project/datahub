package com.linkedin.metadata.utils.metrics;

import static com.linkedin.metadata.utils.metrics.MicrometerMetricsRegistry.GLOBALLY_REGISTERED_CACHES;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MicrometerMetricsRegistryTest {

  @Mock private IMap<Object, Object> hazelcastMap;

  private MeterRegistry meterRegistry;

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    meterRegistry = new SimpleMeterRegistry();

    // Clear the global registry before each test using reflection
    clearGlobalRegistry();
  }

  private void clearGlobalRegistry() throws Exception {
    GLOBALLY_REGISTERED_CACHES.clear();
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
}
