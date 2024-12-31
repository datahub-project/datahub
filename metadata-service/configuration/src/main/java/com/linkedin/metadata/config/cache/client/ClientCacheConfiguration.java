package com.linkedin.metadata.config.cache.client;

import lombok.Data;

@Data
public class ClientCacheConfiguration {
  EntityClientCacheConfig entityClient;
  UsageClientCacheConfig usageClient;
}
