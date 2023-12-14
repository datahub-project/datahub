package com.linkedin.metadata.config.cache.client;

public interface ClientCacheConfig {
  boolean isEnabled();

  boolean isStatsEnabled();

  int getStatsIntervalSeconds();

  int getDefaultTTLSeconds();

  int getMaxBytes();
}
