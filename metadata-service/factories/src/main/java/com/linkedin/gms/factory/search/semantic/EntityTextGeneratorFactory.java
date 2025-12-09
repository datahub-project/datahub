package com.linkedin.gms.factory.search.semantic;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.semantic.EntityTextGenerator;
import com.linkedin.metadata.search.semantic.SearchableFieldExtractor;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntityTextGeneratorFactory {

  @Bean(name = "entityTextGenerator")
  @Nonnull
  protected EntityTextGenerator getInstance(
      @Qualifier("searchableFieldExtractor") @Nonnull
          final SearchableFieldExtractor searchableFieldExtractor,
      @Nonnull final ConfigurationProvider configurationProvider) {
    int maxTextLength = 8000; // Default
    SemanticSearchConfiguration semanticSearchConfig =
        configurationProvider.getElasticSearch().getEntityIndex().getSemanticSearch();
    if (semanticSearchConfig != null && semanticSearchConfig.getEmbeddingsUpdate() != null) {
      maxTextLength = semanticSearchConfig.getEmbeddingsUpdate().getMaxTextLength();
    }

    return new EntityTextGenerator(searchableFieldExtractor, maxTextLength);
  }
}
