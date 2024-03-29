package com.linkedin.common.client;

import com.codahale.metrics.Gauge;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.linkedin.metadata.config.cache.client.ClientCacheConfig;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Generic cache with common configuration for limited weight, per item expiry, and batch loading
 *
 * @param <K> key
 * @param <V> value
 */
@Slf4j
@Builder
public class ClientCache<K, V, C extends ClientCacheConfig> {
  @Nonnull protected final C config;
  @Nonnull protected final LoadingCache<K, V> cache;
  @Nonnull private final Function<Iterable<? extends K>, Map<K, V>> loadFunction;
  @Nonnull private final Weigher<K, V> weigher;
  @Nonnull private final BiFunction<C, K, Integer> ttlSecondsFunction;

  public @Nullable V get(@Nonnull K key) {
    return cache.get(key);
  }

  public @Nonnull Map<K, V> getAll(@Nonnull Iterable<? extends K> keys) {
    return cache.getAll(keys);
  }

  public void refresh(@Nonnull K key) {
    cache.refresh(key);
  }

  public static class ClientCacheBuilder<K, V, C extends ClientCacheConfig> {

    private ClientCacheBuilder<K, V, C> cache(LoadingCache<K, V> cache) {
      return null;
    }

    private ClientCache<K, V, C> build() {
      return null;
    }

    public ClientCache<K, V, C> build(Class<?> metricClazz) {
      // loads data from entity client
      CacheLoader<K, V> loader =
          new CacheLoader<K, V>() {
            @Override
            public V load(@Nonnull K key) {
              return loadAll(Set.of(key)).get(key);
            }

            @Override
            @Nonnull
            public Map<K, V> loadAll(@Nonnull Set<? extends K> keys) {
              return loadFunction.apply(keys);
            }
          };

      // build cache
      Caffeine<K, V> caffeine =
          Caffeine.newBuilder()
              .maximumWeight(config.getMaxBytes())
              // limit total size
              .weigher(weigher)
              .softValues()
              // define per entity/aspect ttls
              .expireAfter(
                  new Expiry<K, V>() {
                    public long expireAfterCreate(
                        @Nonnull K key, @Nonnull V aspect, long currentTime) {
                      int ttlSeconds = ttlSecondsFunction.apply(config, key);
                      if (ttlSeconds < 0) {
                        ttlSeconds = Integer.MAX_VALUE;
                      }
                      return TimeUnit.SECONDS.toNanos(ttlSeconds);
                    }

                    public long expireAfterUpdate(
                        @Nonnull K key, @Nonnull V aspect, long currentTime, long currentDuration) {
                      return currentDuration;
                    }

                    public long expireAfterRead(
                        @Nonnull K key, @Nonnull V aspect, long currentTime, long currentDuration) {
                      return currentDuration;
                    }
                  });

      if (config.isStatsEnabled()) {
        caffeine.recordStats();
      }

      LoadingCache<K, V> cache = caffeine.build(loader);

      if (config.isStatsEnabled()) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleAtFixedRate(
            () -> {
              CacheStats cacheStats = cache.stats();

              MetricUtils.gauge(metricClazz, "hitRate", () -> (Gauge<Double>) cacheStats::hitRate);
              MetricUtils.gauge(
                  metricClazz,
                  "loadFailureRate",
                  () -> (Gauge<Double>) cacheStats::loadFailureRate);
              MetricUtils.gauge(
                  metricClazz, "evictionCount", () -> (Gauge<Long>) cacheStats::evictionCount);
              MetricUtils.gauge(
                  metricClazz,
                  "loadFailureCount",
                  () -> (Gauge<Long>) cacheStats::loadFailureCount);
              MetricUtils.gauge(
                  metricClazz,
                  "averageLoadPenalty",
                  () -> (Gauge<Double>) cacheStats::averageLoadPenalty);
              MetricUtils.gauge(
                  metricClazz, "evictionWeight", () -> (Gauge<Long>) cacheStats::evictionWeight);

              log.debug(metricClazz.getSimpleName() + ": " + cacheStats);
            },
            0,
            config.getStatsIntervalSeconds(),
            TimeUnit.SECONDS);
      }

      return new ClientCache<>(config, cache, loadFunction, weigher, ttlSecondsFunction);
    }
  }
}
