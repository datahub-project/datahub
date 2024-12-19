package com.linkedin.metadata.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClientCache;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/** Java backed SystemEntityClient */
@Getter
public class SystemJavaEntityClient extends JavaEntityClient implements SystemEntityClient {

  private final EntityClientCache entityClientCache;
  private final Cache<String, OperationContext> operationContextMap;

  public SystemJavaEntityClient(
      EntityService<?> entityService,
      DeleteEntityService deleteEntityService,
      EntitySearchService entitySearchService,
      CachingEntitySearchService cachingEntitySearchService,
      SearchService searchService,
      LineageSearchService lineageSearchService,
      TimeseriesAspectService timeseriesAspectService,
      RollbackService rollbackService,
      EventProducer eventProducer,
      EntityClientCacheConfig cacheConfig,
      EntityClientConfig entityClientConfig) {
    super(
        entityService,
        deleteEntityService,
        entitySearchService,
        cachingEntitySearchService,
        searchService,
        lineageSearchService,
        timeseriesAspectService,
        rollbackService,
        eventProducer,
        entityClientConfig);
    this.operationContextMap = CacheBuilder.newBuilder().maximumSize(500).build();
    this.entityClientCache = buildEntityClientCache(SystemJavaEntityClient.class, cacheConfig);
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
