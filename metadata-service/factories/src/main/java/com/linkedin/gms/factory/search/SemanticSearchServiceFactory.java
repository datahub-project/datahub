/**
 * SAAS-SPECIFIC: This factory is part of the semantic search feature exclusive to DataHub SaaS. It
 * should NOT be merged back to the open-source DataHub repository. Dependencies: Creates
 * SemanticSearchService with embedding provider and OpenSearch k-NN.
 */
package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.semantic.SemanticEntitySearch;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SemanticSearchServiceFactory {

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
  @Qualifier("semanticEntitySearchService")
  private SemanticEntitySearch semanticEntitySearchService;

  @Bean(name = "semanticSearchService")
  @Nonnull
  protected SemanticSearchService getInstance(ConfigurationProvider configurationProvider) {
    return new SemanticSearchService(
        new EntityDocCountCache(
            entityRegistry,
            entitySearchService,
            configurationProvider.getCache().getHomepage().getEntityCounts()),
        cachingEntitySearchService,
        semanticEntitySearchService,
        configurationProvider.getSearchService());
  }
}
