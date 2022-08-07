package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.search.aggregator.AllEntitiesSearchAggregator;
import com.linkedin.metadata.search.cache.CachingAllEntitiesSearchAggregator;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class CachingAllEntitiesSearchAggregatorFactory {

  @Autowired
  @Qualifier("allEntitiesSearchAggregator")
  private AllEntitiesSearchAggregator allEntitiesSearchAggregator;

  @Autowired
  private CacheManager cacheManager;

  @Value("${searchService.resultBatchSize}")
  private Integer batchSize;

  @Value("${searchService.enableCache}")
  private Boolean enableCache;

  @Bean(name = "cachingAllEntitiesSearchAggregator")
  @Primary
  @Nonnull
  protected CachingAllEntitiesSearchAggregator getInstance() {
    return new CachingAllEntitiesSearchAggregator(
        cacheManager,
        allEntitiesSearchAggregator,
        batchSize,
        enableCache);
  }
}
