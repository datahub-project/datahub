package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import org.apache.http.client.config.RequestConfig;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

  @Value("${elasticsearch.bulkProcessor.slowByQueryOperationTimeoutSeconds}")
  private int slowByQueryOperationTimeoutSeconds;

  @Bean(name = "elasticSearchBulkProcessor")
  @Nonnull
  protected ESBulkProcessor getInstance(MetricUtils metricUtils) {
    RequestOptions byQueryOpts = buildByQueryRequestOptions(slowByQueryOperationTimeoutSeconds);
    return ESBulkProcessor.builder(searchClient, metricUtils)
        .async(async)
        .bulkFlushPeriod(bulkFlushPeriod)
        .bulkRequestsLimit(bulkRequestsLimit)
        .retryInterval(retryInterval)
        .numRetries(numRetries)
        .threadCount(threadCount)
        .batchDelete(enableBatchDelete)
        .byQueryRequestOptions(byQueryOpts)
        .writeRequestRefreshPolicy(WriteRequest.RefreshPolicy.valueOf(refreshPolicy))
        .build();
  }

  @Nonnull
  static RequestOptions buildByQueryRequestOptions(int slowOperationTimeoutSeconds) {
    int socketTimeoutMs = Math.max(1, slowOperationTimeoutSeconds) * 1000;
    RequestConfig baseConfig = RequestOptions.DEFAULT.getRequestConfig();
    RequestConfig requestConfig =
        baseConfig != null
            ? RequestConfig.copy(baseConfig).setSocketTimeout(socketTimeoutMs).build()
            : RequestConfig.custom().setSocketTimeout(socketTimeoutMs).build();
    return RequestOptions.DEFAULT.toBuilder().setRequestConfig(requestConfig).build();
  }
}
