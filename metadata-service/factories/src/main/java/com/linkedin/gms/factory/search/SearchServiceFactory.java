package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.cache.CachingAllEntitiesSearchAggregator;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.ranker.SearchRanker;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
  @Qualifier("cachingEntitySearchService")
  private CachingEntitySearchService cachingEntitySearchService;

  @Autowired
  @Qualifier("cachingAllEntitiesSearchAggregator")
  private CachingAllEntitiesSearchAggregator cachingAllEntitiesSearchAggregator;

  @Autowired
  @Qualifier("searchRanker")
  private SearchRanker searchRanker;

  @Bean(name = "searchService")
  @Primary
  @Nonnull
  protected SearchService getInstance() {
    return new SearchService(
        new EntityDocCountCache(entityRegistry, entitySearchService),
        cachingEntitySearchService,
        cachingAllEntitiesSearchAggregator,
        searchRanker);
  }
}
