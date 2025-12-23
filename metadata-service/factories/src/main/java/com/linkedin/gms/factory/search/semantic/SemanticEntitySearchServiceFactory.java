package com.linkedin.gms.factory.search.semantic;

import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.semantic.SemanticEntitySearch;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SemanticEntitySearchServiceFactory {

  @Autowired
  @Qualifier("searchClientShim")
  private SearchClientShim<?> searchClient;

  @Autowired
  @Qualifier("embeddingProvider")
  private EmbeddingProvider embeddingProvider;

  @Bean(name = "semanticEntitySearchService")
  @Nonnull
  protected SemanticEntitySearch getInstance(
      @Qualifier("mappingsBuilder") final MappingsBuilder mappingsBuilder) {
    return new SemanticEntitySearchService(searchClient, embeddingProvider, mappingsBuilder);
  }
}
