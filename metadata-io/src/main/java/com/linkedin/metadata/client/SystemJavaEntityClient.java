package com.linkedin.metadata.client;

import com.linkedin.entity.client.EntityClientCache;
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
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Qualifier;

/** Java backed SystemEntityClient */
@Getter
public class SystemJavaEntityClient extends JavaEntityClient implements SystemEntityClient {

  private final EntityClientCache entityClientCache;
  private final OperationContext systemOperationContext;
  private final ConcurrentHashMap<String, OperationContext> operationContextMap;

  public SystemJavaEntityClient(
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      EntityService<?> entityService,
      DeleteEntityService deleteEntityService,
      EntitySearchService entitySearchService,
      CachingEntitySearchService cachingEntitySearchService,
      SearchService searchService,
      LineageSearchService lineageSearchService,
      TimeseriesAspectService timeseriesAspectService,
      RollbackService rollbackService,
      EventProducer eventProducer,
      EntityClientCacheConfig cacheConfig) {
    super(
        systemOperationContext,
        entityService,
        deleteEntityService,
        entitySearchService,
        cachingEntitySearchService,
        searchService,
        lineageSearchService,
        timeseriesAspectService,
        rollbackService,
        eventProducer);
    this.operationContextMap = new ConcurrentHashMap<>();
    this.systemOperationContext = systemOperationContext;
    this.entityClientCache = buildEntityClientCache(SystemJavaEntityClient.class, cacheConfig);
  }
}
