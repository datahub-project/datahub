package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import org.apache.http.client.config.RequestConfig;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchBulkProcessorFactory {
  @Autowired
  @Qualifier("searchClientShim")
  private SearchClientShim<?> searchClient;

  @Bean(name = "elasticSearchBulkProcessor")
  @Nonnull
  protected ESBulkProcessor getInstance(
      final ConfigurationProvider configurationProvider, MetricUtils metricUtils) {
    return build(
        searchClient,
        configurationProvider.getElasticSearch().getBulkProcessor(),
        configurationProvider.getElasticSearch().getThreadCount(),
        metricUtils);
  }

  /**
   * Builds a bulk processor for one client from that cluster's effective settings. Two clusters
   * that share an endpoint still get separate processors when their merged settings differ, since
   * flush period and retry behavior are per-writer state.
   */
  @Nonnull
  static ESBulkProcessor build(
      @Nonnull SearchClientShim<?> client,
      @Nonnull BulkProcessorConfiguration config,
      int threadCount,
      MetricUtils metricUtils) {
    RequestOptions byQueryOpts =
        buildByQueryRequestOptions(config.getSlowByQueryOperationTimeoutSeconds());
    return ESBulkProcessor.builder(client, metricUtils)
        .async(config.isAsync())
        .bulkFlushPeriod(config.getFlushPeriod())
        .bulkRequestsLimit(config.getRequestsLimit())
        .retryInterval(config.getRetryInterval())
        .numRetries(config.getNumRetries())
        .threadCount(threadCount)
        .batchDelete(config.isEnableBatchDelete())
        .itemRequeueEnabled(config.isItemRequeueEnabled())
        .itemRequeueMaxAttempts(config.getItemRequeueMaxAttempts())
        .ackAfterTransfer(config.isAckAfterTransfer())
        .ackAfterTransferTimeoutSeconds(config.getAckAfterTransferTimeoutSeconds())
        .byQueryRequestOptions(byQueryOpts)
        .writeRequestRefreshPolicy(WriteRequest.RefreshPolicy.valueOf(config.getRefreshPolicy()))
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
