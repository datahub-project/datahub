package com.linkedin.metadata.kafka.elasticsearch;

import com.linkedin.metadata.kafka.config.ElasticsearchEnabledCondition;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

// TODO: Move this factory.
// Skip when running on Postgres-only profiles (elasticsearch.enabled=false): ESBulkProcessor is
// only registered by ElasticSearchBulkProcessorFactory under the same condition, and consumers of
// elasticsearchConnector should treat it as optional via ObjectProvider when ES is disabled.
@Slf4j
@Configuration
@Conditional(ElasticsearchEnabledCondition.class)
public class ElasticsearchConnectorFactory {
  @Autowired
  @Qualifier("elasticSearchBulkProcessor")
  private ESBulkProcessor elasticSearchBulkProcessor;

  @Value("${elasticsearch.bulkProcessor.numRetries}")
  private Integer numRetries;

  @Bean(name = "elasticsearchConnector")
  @Nonnull
  public ElasticsearchConnector createInstance() {
    return new ElasticsearchConnector(elasticSearchBulkProcessor, numRetries);
  }
}
