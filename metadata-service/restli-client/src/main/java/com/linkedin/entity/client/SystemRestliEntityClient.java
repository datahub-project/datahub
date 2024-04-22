package com.linkedin.entity.client;

import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.restli.client.Client;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import lombok.Getter;

/** Restli backed SystemEntityClient */
@Getter
public class SystemRestliEntityClient extends RestliEntityClient implements SystemEntityClient {
  private final EntityClientCache entityClientCache;
  private final OperationContext systemOperationContext;
  private final ConcurrentHashMap<String, OperationContext> operationContextMap;

  public SystemRestliEntityClient(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull final Client restliClient,
      @Nonnull final BackoffPolicy backoffPolicy,
      int retryCount,
      EntityClientCacheConfig cacheConfig) {
    super(restliClient, backoffPolicy, retryCount);
    this.operationContextMap = new ConcurrentHashMap<>();
    this.systemOperationContext = systemOperationContext;
    this.entityClientCache = buildEntityClientCache(SystemRestliEntityClient.class, cacheConfig);
  }
}
