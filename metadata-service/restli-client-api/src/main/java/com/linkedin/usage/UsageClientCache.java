package com.linkedin.usage;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.linkedin.common.client.ClientCache;
import com.linkedin.metadata.config.cache.client.UsageClientCacheConfig;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;

@Builder
public class UsageClientCache {
  @Nonnull private UsageClientCacheConfig config;
  @Nonnull private final ClientCache<Key, UsageQueryResult, UsageClientCacheConfig> cache;
  @Nonnull private Function<Key, UsageQueryResult> loadFunction;

  public UsageQueryResult getUsageStats(
      @Nonnull OperationContext opContext,
      @Nonnull String resource,
      @Nonnull UsageTimeRange range) {
    Key cacheKey =
        Key.builder()
            .contextId(opContext.getEntityContextId())
            .resource(resource)
            .range(range)
            .build();
    if (config.isEnabled()) {
      return cache.get(cacheKey);
    } else {
      return loadFunction.apply(cacheKey);
    }
  }

  public static class UsageClientCacheBuilder {

    private UsageClientCacheBuilder cache(LoadingCache<Key, UsageQueryResult> cache) {
      return this;
    }

    public UsageClientCache build() {
      // estimate size
      Weigher<Key, UsageQueryResult> weighByEstimatedSize =
          (key, value) -> value.data().toString().getBytes().length;

      // batch loads data from usage client
      Function<Iterable<? extends Key>, Map<Key, UsageQueryResult>> loader =
          (Iterable<? extends Key> keys) ->
              StreamSupport.stream(keys.spliterator(), false)
                  .map(k -> Map.entry(k, loadFunction.apply(k)))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // default ttl only
      BiFunction<UsageClientCacheConfig, Key, Integer> ttlSeconds =
          (config, key) -> config.getDefaultTTLSeconds();

      cache =
          ClientCache.<Key, UsageQueryResult, UsageClientCacheConfig>builder()
              .weigher(weighByEstimatedSize)
              .config(config)
              .loadFunction(loader)
              .ttlSecondsFunction(ttlSeconds)
              .build(UsageClientCache.class);

      return new UsageClientCache(config, cache, loadFunction);
    }
  }

  @Data
  @Builder
  public static class Key {
    private final String contextId;
    private final String resource;
    private final UsageTimeRange range;
  }
}
