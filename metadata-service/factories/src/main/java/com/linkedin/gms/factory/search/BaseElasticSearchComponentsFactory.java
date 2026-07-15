package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Aggregates Elasticsearch/OpenSearch client wiring alongside shared search configuration ({@link
 * ElasticSearchConfiguration}, {@link IndexConvention}).
 *
 * <p>OpenSearch-backed beans ({@link SearchClientShim}, {@link ESBulkProcessor}, {@link
 * ESIndexBuilder}) register only when {@code elasticsearch.enabled=true} (including {@code
 * matchIfMissing}). When integration is off, PostgreSQL-backed services use this bean for naming
 * and config only; ES client fields are null.
 */
@Configuration
@Import({
  IndexConventionFactory.class,
  ElasticSearchBulkProcessorFactory.class,
  ElasticSearchIndexBuilderFactory.class
})
public class BaseElasticSearchComponentsFactory {
  @lombok.Getter
  @lombok.AllArgsConstructor
  public static class BaseElasticSearchComponents {
    @Nonnull ElasticSearchConfiguration config;
    @Nullable SearchClientShim<?> searchClient;
    @Nonnull IndexConvention indexConvention;
    @Nullable ESBulkProcessor bulkProcessor;
    @Nullable ESIndexBuilder indexBuilder;
  }

  @Bean(name = "baseElasticSearchComponents")
  @Nonnull
  protected BaseElasticSearchComponents getInstance(
      ConfigurationProvider configurationProvider,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention,
      ObjectProvider<SearchClientShim<?>> searchClientProvider,
      ObjectProvider<ESBulkProcessor> bulkProcessorProvider,
      ObjectProvider<ESIndexBuilder> indexBuilderProvider) {
    return new BaseElasticSearchComponents(
        configurationProvider.getElasticSearch(),
        searchClientProvider.getIfAvailable(),
        indexConvention,
        bulkProcessorProvider.getIfAvailable(),
        indexBuilderProvider.getIfAvailable());
  }
}
