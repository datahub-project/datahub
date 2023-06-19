package com.linkedin.metadata.config.cache;

import lombok.Data;


@Data
public class CacheConfiguration {
  PrimaryCacheConfiguration primary;
  HomepageCacheConfiguration homepage;
  SearchCacheConfiguration search;
}
