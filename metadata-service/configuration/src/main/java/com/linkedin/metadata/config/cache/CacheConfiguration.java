package com.linkedin.metadata.config.cache;

import com.linkedin.metadata.config.cache.client.ClientCacheConfiguration;
import lombok.Data;

@Data
public class CacheConfiguration {
  PrimaryCacheConfiguration primary;
  HomepageCacheConfiguration homepage;
  SearchCacheConfiguration search;
  ClientCacheConfiguration client;
}
