package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.ranker.SearchRanker;
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
public class SearchServiceFactory {

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @Autowired
  @Qualifier("searchRanker")
  private SearchRanker searchRanker;

  @Autowired
  private CacheManager cacheManager;

  @Value("${searchService.resultBatchSize}")
  private Integer batchSize;

  @Value("${searchService.enableCache}")
  private Boolean enableCache;

  @Bean(name = "searchService")
  @Primary
  @Nonnull
  protected SearchService getInstance() {
    return new SearchService(entityRegistry, entitySearchService, searchRanker, cacheManager, batchSize, enableCache);
  }
}
