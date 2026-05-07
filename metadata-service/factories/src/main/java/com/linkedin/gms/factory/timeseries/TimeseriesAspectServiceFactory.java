package com.linkedin.gms.factory.timeseries;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.timeseries.LatestTimeseriesAspectVersionCachingService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Slf4j
@Configuration
@Import({BaseElasticSearchComponentsFactory.class, EntityRegistryFactory.class})
public class TimeseriesAspectServiceFactory {

  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Autowired(required = false)
  private CacheManager cacheManager;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Bean(name = "timeseriesAspectService")
  @Primary
  @Nonnull
  protected TimeseriesAspectService getInstance(
      final QueryFilterRewriteChain queryFilterRewriteChain,
      final ConfigurationProvider configurationProvider,
      final MetricUtils metricUtils) {
    var cacheConfig = configurationProvider.getTimeseriesAspectService().getCache();

    ElasticSearchTimeseriesAspectService service =
        new ElasticSearchTimeseriesAspectService(
            components.getSearchClient(),
            components.getBulkProcessor(),
            components.getConfig().getBulkProcessor().getNumRetries(),
            queryFilterRewriteChain,
            configurationProvider.getTimeseriesAspectService(),
            entityRegistry,
            components.getIndexConvention(),
            components.getIndexBuilder(),
            metricUtils);
    if (cacheManager != null
        && cacheConfig.isEnabled()
        && !cacheConfig.getCachedAspects().isEmpty()) {
      log.info(
          "Timeseries latest-aspect cache enabled for: {} (TTL={}h, jitter={}min, maxSize={})",
          cacheConfig.getCachedAspects(),
          cacheConfig.getTtlHours(),
          cacheConfig.getTtlJitterMinutes(),
          cacheConfig.getMaxSize());
      return new LatestTimeseriesAspectVersionCachingService(
          service, cacheManager, cacheConfig, metricUtils);
    }

    if (cacheConfig.isEnabled() && cacheConfig.getCachedAspects().isEmpty()) {
      log.info(
          "Timeseries cache enabled but no aspects configured via TIMESERIES_LATEST_CACHED_ASPECTS");
    }
    return service;
  }
}
