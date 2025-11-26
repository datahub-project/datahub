package com.linkedin.metadata.config.cache;

import com.linkedin.metadata.config.cache.client.ClientCacheConfiguration;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class CacheConfiguration {
  PrimaryCacheConfiguration primary;
  HomepageCacheConfiguration homepage;
  SearchCacheConfiguration search;
  ClientCacheConfiguration client;
}
