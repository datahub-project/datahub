package com.linkedin.metadata.client;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.EntityClientCache;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import lombok.Getter;

/** Java backed SystemEntityClient */
@Getter
public class SystemJavaEntityClient extends JavaEntityClient implements SystemEntityClient {

  private final EntityClientCache entityClientCache;
  private final Authentication systemAuthentication;

  public SystemJavaEntityClient(
      EntityService entityService,
      DeleteEntityService deleteEntityService,
      EntitySearchService entitySearchService,
      CachingEntitySearchService cachingEntitySearchService,
      SearchService searchService,
      LineageSearchService lineageSearchService,
      TimeseriesAspectService timeseriesAspectService,
      EventProducer eventProducer,
      RestliEntityClient restliEntityClient,
      Authentication systemAuthentication,
      EntityClientCacheConfig cacheConfig) {
    super(
        entityService,
        deleteEntityService,
        entitySearchService,
        cachingEntitySearchService,
        searchService,
        lineageSearchService,
        timeseriesAspectService,
        eventProducer,
        restliEntityClient);
    this.systemAuthentication = systemAuthentication;
    this.entityClientCache =
        buildEntityClientCache(SystemJavaEntityClient.class, systemAuthentication, cacheConfig);
  }
}
