package com.linkedin.gms.factory.system_telemetry;

import com.linkedin.metadata.utils.metrics.MicrometerMetricsRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collection;
import javax.annotation.Nonnull;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class CacheInstrumentationFactory {
  /**
   * Creates an instrumented CacheManager that automatically registers Micrometer metrics for all
   * caches accessed through it.
   *
   * <p>This bean is marked as @Primary to ensure it's used by default throughout the application,
   * replacing direct access to the underlying cache manager. When any cache is accessed via {@code
   * getCache(String)}, metrics are automatically registered if not already present, enabling
   * visibility into cache performance including hits, misses, evictions, and size.
   *
   * <p>The metrics registration is lazy - caches are only instrumented when first accessed,
   * avoiding unnecessary overhead for unused caches while ensuring complete coverage for active
   * caches.
   *
   * @param meterRegistry component responsible for registering cache metrics with Micrometer
   * @return an instrumented CacheManager that delegates to the configured implementation while
   *     automatically registering metrics for accessed caches
   */
  @Bean
  @Primary
  public CacheManager instrumentedCacheManager(CacheManager delegate, MeterRegistry meterRegistry) {

    return new CacheManager() {

      @Override
      public Cache getCache(@Nonnull String name) {
        Cache cache = delegate.getCache(name);
        MicrometerMetricsRegistry.registerCacheMetrics(name, cache, meterRegistry);
        return cache;
      }

      @Override
      @Nonnull
      public Collection<String> getCacheNames() {
        return delegate.getCacheNames();
      }
    };
  }
}
