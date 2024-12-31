package com.linkedin.gms.factory.entityclient;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.client.SystemJavaEntityClient;
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
import javax.inject.Singleton;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** The *Java* Entity Client should be preferred if executing within the GMS service. */
@Configuration
@ConditionalOnProperty(name = "entityClient.impl", havingValue = "java", matchIfMissing = true)
public class JavaEntityClientFactory {

  @Bean("entityClient")
  @Singleton
  public EntityClient entityClient(
      final @Qualifier("entityService") EntityService<?> _entityService,
      final @Qualifier("deleteEntityService") DeleteEntityService _deleteEntityService,
      final @Qualifier("searchService") SearchService _searchService,
      final @Qualifier("entitySearchService") EntitySearchService _entitySearchService,
      final @Qualifier("cachingEntitySearchService") CachingEntitySearchService
              _cachingEntitySearchService,
      final @Qualifier("timeseriesAspectService") TimeseriesAspectService _timeseriesAspectService,
      final @Qualifier("relationshipSearchService") LineageSearchService _lineageSearchService,
      final @Qualifier("kafkaEventProducer") EventProducer _eventProducer,
      final RollbackService rollbackService,
      final EntityClientConfig entityClientConfig) {
    return new JavaEntityClient(
        _entityService,
        _deleteEntityService,
        _entitySearchService,
        _cachingEntitySearchService,
        _searchService,
        _lineageSearchService,
        _timeseriesAspectService,
        rollbackService,
        _eventProducer,
        entityClientConfig);
  }

  @Bean("systemEntityClient")
  @Singleton
  public SystemEntityClient systemEntityClient(
      final @Qualifier("entityService") EntityService<?> _entityService,
      final @Qualifier("deleteEntityService") DeleteEntityService _deleteEntityService,
      final @Qualifier("searchService") SearchService _searchService,
      final @Qualifier("entitySearchService") EntitySearchService _entitySearchService,
      final @Qualifier("cachingEntitySearchService") CachingEntitySearchService
              _cachingEntitySearchService,
      final @Qualifier("timeseriesAspectService") TimeseriesAspectService _timeseriesAspectService,
      final @Qualifier("relationshipSearchService") LineageSearchService _lineageSearchService,
      final @Qualifier("kafkaEventProducer") EventProducer _eventProducer,
      final RollbackService rollbackService,
      final EntityClientCacheConfig entityClientCacheConfig,
      final EntityClientConfig entityClientConfig) {
    return new SystemJavaEntityClient(
        _entityService,
        _deleteEntityService,
        _entitySearchService,
        _cachingEntitySearchService,
        _searchService,
        _lineageSearchService,
        _timeseriesAspectService,
        rollbackService,
        _eventProducer,
        entityClientCacheConfig,
        entityClientConfig);
  }
}
