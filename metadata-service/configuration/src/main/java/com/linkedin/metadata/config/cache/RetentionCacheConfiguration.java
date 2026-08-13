package com.linkedin.metadata.config.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Process-local Caffeine (or Hazelcast when {@code searchService.cacheImplementation=hazelcast})
 * cache of resolved aspect retention policies. Policies rarely change; the write path otherwise
 * re-reads them from primary storage on every aspect upsert.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class RetentionCacheConfiguration {
  /** Aligned with {@code cache.primary.ttlSeconds}; Caffeine uses that shared expireAfterWrite. */
  public static final int DEFAULT_TTL_SECONDS = 600;

  @Builder.Default private boolean enabled = true;
  @Builder.Default private long ttlSeconds = DEFAULT_TTL_SECONDS;

  public long getTtlMillis() {
    return ttlSeconds * 1000;
  }
}
