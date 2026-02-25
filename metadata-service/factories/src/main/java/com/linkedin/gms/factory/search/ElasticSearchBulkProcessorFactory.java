package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.support.WriteRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class ElasticSearchBulkProcessorFactory {
  @Autowired
  @Qualifier("searchClientShim")
  private SearchClientShim<?> searchClient;

  @Value("${elasticsearch.bulkProcessor.requestsLimit}")
  private Integer bulkRequestsLimit;

  @Value("${elasticsearch.bulkProcessor.flushPeriod}")
  private Integer bulkFlushPeriod;

  @Value("${elasticsearch.bulkProcessor.numRetries}")
  private Integer numRetries;

  @Value("${elasticsearch.bulkProcessor.retryInterval}")
  private Long retryInterval;

  @Value("#{new Boolean('${elasticsearch.bulkProcessor.async}')}")
  private boolean async;

  @Value("#{new Boolean('${elasticsearch.bulkProcessor.enableBatchDelete}')}")
  private boolean enableBatchDelete;

  @Value("${elasticsearch.bulkProcessor.refreshPolicy}")
  private String refreshPolicy;

  @Value("${elasticsearch.threadCount}")
  private Integer threadCount;

  @Bean(name = "elasticSearchBulkProcessor")
  @Nonnull
  protected ESBulkProcessor getInstance(MetricUtils metricUtils) {
    return ESBulkProcessor.builder(searchClient, metricUtils)
        .async(async)
        .bulkFlushPeriod(bulkFlushPeriod)
        .bulkRequestsLimit(bulkRequestsLimit)
        .retryInterval(retryInterval)
        .numRetries(numRetries)
        .threadCount(threadCount)
        .batchDelete(enableBatchDelete)
        .writeRequestRefreshPolicy(WriteRequest.RefreshPolicy.valueOf(refreshPolicy))
        .build();
  }
}
