package com.linkedin.metadata.config.cache;

import lombok.Data;

@Data
public class PrimaryCacheConfiguration {
  long ttlSeconds;
  long maxSize;
}
