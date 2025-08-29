/**
 * SAAS-SPECIFIC: This factory is part of the semantic search feature exclusive to DataHub SaaS. It
 * should NOT be merged back to the open-source DataHub repository. Dependencies: Creates
 * SemanticEntitySearchService with OpenSearch k-NN support.
 */
package com.linkedin.gms.factory.search.semantic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.semantic.SemanticEntitySearch;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SemanticEntitySearchServiceFactory {

  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Autowired
  @Qualifier("embeddingProvider")
  private EmbeddingProvider embeddingProvider;

  @Bean(name = "semanticEntitySearchService")
  @Nonnull
  protected SemanticEntitySearch getInstance(ObjectMapper objectMapper) {
    return new SemanticEntitySearchService(
        searchClient, indexConvention, embeddingProvider, objectMapper);
  }
}
