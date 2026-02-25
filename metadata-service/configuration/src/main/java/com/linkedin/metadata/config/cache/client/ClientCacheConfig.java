package com.linkedin.metadata.config.cache.client;

public interface ClientCacheConfig {
  String getName();

  boolean isEnabled();

  boolean isStatsEnabled();

  int getStatsIntervalSeconds();

  int getDefaultTTLSeconds();

  int getMaxBytes();
}
