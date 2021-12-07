package com.linkedin.metadata.kafka.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import javax.annotation.Nonnull;

// TODO: Move this factory.
@Slf4j
@Configuration
public class ElasticsearchConnectorFactory {

  @Value("${ES_BULK_REQUESTS_LIMIT:1}")
  private Integer bulkRequestsLimit;

  @Value("${ES_BULK_FLUSH_PERIOD:1}")
  private Integer bulkFlushPeriod;

  @Bean(name = "elasticsearchConnector")
  @Nonnull
  public ElasticsearchConnector createInstance(@Nonnull RestHighLevelClient elasticSearchRestHighLevelClient) {
    return new ElasticsearchConnector(elasticSearchRestHighLevelClient, bulkRequestsLimit, bulkFlushPeriod);
  }

}