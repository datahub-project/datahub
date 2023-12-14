package com.linkedin.metadata.config.cache;

import lombok.Data;

@Data
public class EntityDocCountCacheConfiguration {
  long ttlSeconds;
  long lightningThreshold;
}
