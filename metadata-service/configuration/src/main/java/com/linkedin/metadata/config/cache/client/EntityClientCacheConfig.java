package com.linkedin.metadata.config.cache.client;

import java.util.Map;
import lombok.Data;

@Data
public class EntityClientCacheConfig implements ClientCacheConfig {
  private boolean enabled;
  private boolean statsEnabled;
  private int statsIntervalSeconds;
  private int defaultTTLSeconds;
  private int maxBytes;

  // entityName -> aspectName -> cache ttl override
  private Map<String, Map<String, Integer>> entityAspectTTLSeconds;
}
