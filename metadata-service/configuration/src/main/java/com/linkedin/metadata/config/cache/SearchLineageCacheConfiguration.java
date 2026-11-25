package com.linkedin.metadata.config.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class SearchLineageCacheConfiguration {
  long ttlSeconds;
  long lightningThreshold;

  /** TTL for partial lineage results, such as walks that timed out; 0 never serves them. */
  long partialTtlSeconds;

  public long getTTLMillis() {
    return ttlSeconds * 1000;
  }

  public long getPartialTTLMillis() {
    return partialTtlSeconds * 1000;
  }
}
