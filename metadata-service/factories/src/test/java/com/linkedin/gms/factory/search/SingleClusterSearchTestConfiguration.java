package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.mock;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The production single-cluster {@link SearchClusterRegistry} built from the bound configuration,
 * with the {@code searchClientShim} bean the test provides. Bulk processor and index builder are
 * mocks because they talk to the search cluster.
 */
@Configuration
public class SingleClusterSearchTestConfiguration {
  @Bean("searchClusterRegistry")
  public SearchClusterRegistry searchClusterRegistry(
      ConfigurationProvider configurationProvider,
      @Qualifier("searchClientShim") SearchClientShim<?> client) {
    return SearchClusterRegistry.singleCluster(
        configurationProvider.getElasticSearch(),
        client,
        mock(ESBulkProcessor.class),
        mock(ESIndexBuilder.class));
  }
}
