package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
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
public class CachingEntitySearchServiceFactory {

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @Autowired private CacheManager cacheManager;

  @Value("${searchService.resultBatchSize}")
  private Integer batchSize;

  @Value("${searchService.enableCache}")
  private Boolean enableCache;

  @Bean(name = "cachingEntitySearchService")
  @Primary
  @Nonnull
  protected CachingEntitySearchService getInstance() {
    return new CachingEntitySearchService(
        cacheManager, entitySearchService, batchSize, enableCache);
  }
}
