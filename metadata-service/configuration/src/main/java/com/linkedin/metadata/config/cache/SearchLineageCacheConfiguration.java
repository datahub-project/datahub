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
  long maxSize;
  String evictionPolicy;

  public long getTTLMillis() {
    return ttlSeconds * 1000;
  }
}
