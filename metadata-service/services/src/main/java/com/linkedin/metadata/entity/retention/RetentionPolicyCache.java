package com.linkedin.metadata.entity.retention;

import com.linkedin.retention.Retention;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Cache of <em>resolved</em> retention policies keyed by {@code (entityName, aspectName)} after
 * wildcard fallback. Backed by Spring {@code CacheManager} so the same named cache is Caffeine
 * locally and Hazelcast when {@code searchService.cacheImplementation=hazelcast}.
 */
public interface RetentionPolicyCache {

  String CACHE_NAME = "retentionPolicy";

  RetentionPolicyCache NO_OP =
      new RetentionPolicyCache() {
        @Override
        public Retention get(@Nonnull String entityName, @Nonnull String aspectName) {
          return null;
        }

        @Override
        public void put(
            @Nonnull String entityName, @Nonnull String aspectName, @Nonnull Retention retention) {}

        @Override
        public void invalidateAll() {}
      };

  /**
   * @return cached resolved policy, or {@code null} on miss / stale / backend error
   */
  @Nullable
  Retention get(@Nonnull String entityName, @Nonnull String aspectName);

  void put(@Nonnull String entityName, @Nonnull String aspectName, @Nonnull Retention retention);

  /** Drop every entry. Policy writes are rare and may change wildcard fallback for many keys. */
  void invalidateAll();
}
