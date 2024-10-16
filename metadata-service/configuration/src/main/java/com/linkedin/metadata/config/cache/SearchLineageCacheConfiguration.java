package com.linkedin.metadata.config.cache;

import lombok.Data;

@Data
public class SearchLineageCacheConfiguration {
  long ttlSeconds;
  long lightningThreshold;

  public long getTTLMillis() {
    return ttlSeconds * 1000;
  }
}
