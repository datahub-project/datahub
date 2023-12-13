package com.linkedin.usage;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import com.linkedin.common.client.ClientCache;
import com.linkedin.metadata.config.cache.client.UsageClientCacheConfig;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Builder
public class UsageClientCache {
  @NonNull private UsageClientCacheConfig config;
  @NonNull private final ClientCache<Key, UsageQueryResult, UsageClientCacheConfig> cache;
  @NonNull private BiFunction<String, UsageTimeRange, UsageQueryResult> loadFunction;

  public UsageQueryResult getUsageStats(@Nonnull String resource, @Nonnull UsageTimeRange range) {
    if (config.isEnabled()) {
      return cache.get(Key.builder().resource(resource).range(range).build());
    } else {
      return loadFunction.apply(resource, range);
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
              StreamSupport.stream(keys.spliterator(), true)
                  .map(k -> Map.entry(k, loadFunction.apply(k.getResource(), k.getRange())))
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
  protected static class Key {
    private final String resource;
    private final UsageTimeRange range;
  }
}
