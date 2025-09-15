package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/** Factory for components required for any services using elasticsearch */
@Configuration
@Import({
  RestHighLevelClientFactory.class,
  IndexConventionFactory.class,
  ElasticSearchBulkProcessorFactory.class,
  ElasticSearchIndexBuilderFactory.class
})
public class BaseElasticSearchComponentsFactory {
  @lombok.Value
  public static class BaseElasticSearchComponents {
    ElasticSearchConfiguration config;
    RestHighLevelClient searchClient;
    IndexConvention indexConvention;
    ESBulkProcessor bulkProcessor;
    ESIndexBuilder indexBuilder;
  }

  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Autowired
  @Qualifier("elasticSearchBulkProcessor")
  private ESBulkProcessor bulkProcessor;

  @Autowired
  @Qualifier("elasticSearchIndexBuilder")
  private ESIndexBuilder indexBuilder;

  @Bean(name = "baseElasticSearchComponents")
  @Nonnull
  protected BaseElasticSearchComponents getInstance(ConfigurationProvider configurationProvider) {
    return new BaseElasticSearchComponents(
        configurationProvider.getElasticSearch(),
        searchClient,
        indexConvention,
        bulkProcessor,
        indexBuilder);
  }
}
