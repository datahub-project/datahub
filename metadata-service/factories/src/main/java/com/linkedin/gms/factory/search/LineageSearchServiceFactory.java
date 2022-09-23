package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.GraphServiceFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import javax.annotation.Nonnull;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;


@Configuration
@Import({GraphServiceFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class LineageSearchServiceFactory {

  @Bean(name = "relationshipSearchService")
  @Primary
  @Nonnull
  protected LineageSearchService getInstance(CacheManager cacheManager, GraphService graphService,
       SearchService searchService, ConfigurationProvider configurationProvider) {
    boolean cacheEnabled = configurationProvider.getFeatureFlags().isLineageSearchCacheEnabled();
    return new LineageSearchService(searchService, graphService,
        cacheEnabled ? cacheManager.getCache("relationshipSearchService") : null, cacheEnabled);
  }
}
