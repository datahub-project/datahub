package com.linkedin.entity.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/** Restli backed SystemEntityClient */
@Getter
public class SystemRestliEntityClient extends RestliEntityClient implements SystemEntityClient {
  private final EntityClientCache entityClientCache;
  private final Cache<String, OperationContext> operationContextMap;

  public SystemRestliEntityClient(
      @Nonnull final Client restliClient,
      @Nonnull EntityClientConfig clientConfig,
      EntityClientCacheConfig cacheConfig) {
    super(restliClient, clientConfig);
    this.operationContextMap = CacheBuilder.newBuilder().maximumSize(500).build();
    this.entityClientCache = buildEntityClientCache(SystemRestliEntityClient.class, cacheConfig);
  }

  @Nullable
  @Override
  public EntityResponse getV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull Urn urn,
      @Nullable Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return getV2(opContext, urn, aspectNames);
  }

  @Nonnull
  @Override
  public Map<Urn, EntityResponse> batchGetV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return batchGetV2(opContext, urns, aspectNames);
  }

  @Override
  public Map<Urn, EntityResponse> batchGetV2NoCache(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return super.batchGetV2(opContext, entityName, urns, aspectNames, false);
  }
}
