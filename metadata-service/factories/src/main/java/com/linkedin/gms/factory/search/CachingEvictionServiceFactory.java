package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.client.CacheEvictionService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class CachingEvictionServiceFactory {

  @Autowired private CacheManager cacheManager;

  @Value("${searchService.enableCache}")
  private Boolean cachingEnabled;

  @Value("${searchService.enableEviction}")
  private Boolean enableEviction;

  @Bean(name = "cachingEvictionService")
  @Primary
  @Nonnull
  protected CacheEvictionService getInstance() {
    return new CacheEvictionService(cacheManager, cachingEnabled, enableEviction);
  }
}
