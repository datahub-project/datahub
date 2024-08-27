package com.linkedin.metadata.config.cache.client;

import lombok.Data;

@Data
public class UsageClientCacheConfig implements ClientCacheConfig {
  private boolean enabled;
  private boolean statsEnabled;
  private int statsIntervalSeconds;
  private int defaultTTLSeconds;
  private int maxBytes;
}
