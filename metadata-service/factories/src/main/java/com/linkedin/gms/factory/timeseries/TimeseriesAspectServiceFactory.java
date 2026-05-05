package com.linkedin.gms.factory.timeseries;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.timeseries.LatestTimeseriesAspectVersionCachingService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
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
@Import({ElasticSearchTimeseriesAspectServiceFactory.class})
public class TimeseriesAspectServiceFactory {
  @Autowired
  @Qualifier("elasticSearchTimeseriesAspectService")
  private ElasticSearchTimeseriesAspectService _elasticSearchTimeseriesAspectService;

  @Autowired(required = false)
  private CacheManager cacheManager;

  @Autowired private ConfigurationProvider configurationProvider;

  @Bean(name = "timeseriesAspectService")
  @Primary
  @Nonnull
  protected TimeseriesAspectService getInstance() {
    var cacheConfig = configurationProvider.getTimeseriesAspectService().getCache();

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
          _elasticSearchTimeseriesAspectService, cacheManager, cacheConfig);
    }

    if (cacheConfig.isEnabled() && cacheConfig.getCachedAspects().isEmpty()) {
      log.info(
          "Timeseries cache enabled but no aspects configured via TIMESERIES_LATEST_CACHED_ASPECTS");
    }

    return _elasticSearchTimeseriesAspectService;
  }
}
