package com.linkedin.metadata.utils.metrics;

import com.github.benmanes.caffeine.cache.Cache;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.micrometer.core.instrument.binder.cache.HazelcastCacheMetrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MicrometerMetricsRegistry {

  static final Set<String> GLOBALLY_REGISTERED_CACHES = ConcurrentHashMap.newKeySet();
  static final Set<String> GLOBALLY_REGISTERED_EXECUTOR_SERVICE = ConcurrentHashMap.newKeySet();

  private MicrometerMetricsRegistry() {}

  /**
   * Register cache metrics if not already registered globally. This method is thread-safe and
   * ensures each cache is only registered once.
   */
  public static synchronized boolean registerCacheMetrics(
      @Nonnull String cacheName,
      @Nonnull Object nativeCache,
      @Nullable MeterRegistry meterRegistry) {

    if (cacheName == null || nativeCache == null || meterRegistry == null) {
      return false;
    }

    if (!GLOBALLY_REGISTERED_CACHES.add(cacheName)) {
      return false;
    }

    try {
      if (nativeCache instanceof Cache) {
        Cache<?, ?> caffeineCache = (Cache<?, ?>) nativeCache;
        CaffeineCacheMetrics.monitor(meterRegistry, caffeineCache, cacheName, "name", cacheName);
        log.debug("Registered Caffeine cache metrics for: {}", cacheName);
      } else if (nativeCache instanceof com.hazelcast.map.IMap) {
        com.hazelcast.map.IMap<?, ?> hazelcastMap = (com.hazelcast.map.IMap<?, ?>) nativeCache;
        HazelcastCacheMetrics.monitor(meterRegistry, hazelcastMap, "name", cacheName);
        log.debug("Registered Hazelcast cache metrics for: {}", cacheName);
      }
      return true;
    } catch (Exception e) {
      // Remove from set if registration failed
      GLOBALLY_REGISTERED_CACHES.remove(cacheName);
      log.error(
          "Failed to register metrics for cache: {}. Error: {}", cacheName, e.getMessage(), e);
      return false;
    }
  }

  /**
   * Register executor service metrics if not already registered globally. This method is
   * thread-safe and ensures each cache is only registered once.
   */
  public static synchronized boolean registerExecutorMetrics(
      @Nonnull String executorName,
      @Nonnull ExecutorService executorService,
      @Nullable MeterRegistry meterRegistry) {

    if (executorName == null || executorService == null || meterRegistry == null) {
      return false;
    }

    if (!GLOBALLY_REGISTERED_EXECUTOR_SERVICE.add(executorName)) {
      return false;
    }

    try {
      ExecutorServiceMetrics.monitor(meterRegistry, executorService, executorName);
      return true;
    } catch (Exception e) {
      // Remove from set if registration failed
      GLOBALLY_REGISTERED_EXECUTOR_SERVICE.remove(executorName);
      log.error(
          "Failed to register metrics for executor: {}. Error: {}",
          executorName,
          e.getMessage(),
          e);
      return false;
    }
  }
}
