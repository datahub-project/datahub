package com.linkedin.metadata.kafka.elasticsearch;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// TODO: Move this factory.
@Slf4j
@Configuration
public class ElasticsearchConnectorFactory {
  @Autowired
  @Qualifier("elasticSearchBulkProcessor")
  private ESBulkProcessor bulkProcessor;

  @Value("${elasticsearch.bulkProcessor.numRetries}")
  private Integer numRetries;

  @Bean(name = "elasticsearchConnector")
  @Nonnull
  public ElasticsearchConnector createInstance() {
    return new ElasticsearchConnector(bulkProcessor, numRetries);
  }
}
