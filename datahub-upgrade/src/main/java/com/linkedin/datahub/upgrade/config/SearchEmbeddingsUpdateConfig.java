package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.semanticsearch.SearchEmbeddingsUpdate;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.semantic.EntityTextGenerator;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SearchEmbeddingsUpdateConfig {

  @Bean(name = "searchEmbeddingsUpdate")
  @Nonnull
  public SearchEmbeddingsUpdate createInstance(
      @Qualifier("systemOperationContext") @Nonnull OperationContext systemOperationContext,
      @Nonnull SearchClientShim<?> searchClientShim,
      @Nonnull ConfigurationProvider configurationProvider,
      @Nonnull EntityTextGenerator entityTextGenerator,
      @Nonnull EmbeddingProvider embeddingProvider,
      @Nonnull ESBulkProcessor esBulkProcessor) {
    ElasticSearchConfiguration elasticSearchConfiguration =
        configurationProvider.getElasticSearch();

    return new SearchEmbeddingsUpdate(
        systemOperationContext,
        searchClientShim,
        elasticSearchConfiguration,
        entityTextGenerator,
        embeddingProvider,
        esBulkProcessor);
  }
}
