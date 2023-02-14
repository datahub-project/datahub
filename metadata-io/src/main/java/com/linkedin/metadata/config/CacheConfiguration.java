package com.linkedin.metadata.config;

import lombok.Data;


@Data
public class CacheConfiguration {
  PrimaryCacheConfiguration primary;
  HomepageCacheConfiguration homepage;
}
