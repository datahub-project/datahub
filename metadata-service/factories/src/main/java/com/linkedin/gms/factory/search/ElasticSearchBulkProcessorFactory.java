package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.telemetry.RequestAttributionConfiguration;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.BulkTelemetryConfig;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.opentelemetry.api.trace.Tracer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
      final ConfigurationProvider configurationProvider,
      MetricUtils metricUtils,
      @Nullable final SystemTelemetryContext systemTelemetryContext) {
    return build(
        searchClient,
        configurationProvider.getElasticSearch().getBulkProcessor(),
        configurationProvider.getElasticSearch().getThreadCount(),
        metricUtils,
        attribution(configurationProvider),
        systemTelemetryContext != null ? systemTelemetryContext.getTracer() : null);
  }

  /** The attribution block, or null when the telemetry block is absent (minimal test contexts). */
  @Nullable
  static RequestAttributionConfiguration attribution(ConfigurationProvider configurationProvider) {
    return configurationProvider.getTelemetry() != null
        ? configurationProvider.getTelemetry().getRequestAttribution()
        : null;
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
    return build(client, config, threadCount, metricUtils, null, null);
  }

  /**
   * Bulk-write attribution settings from {@code telemetry.requestAttribution}: batch spans when
   * {@code enabled} (and a tracer is available), the batch id header when {@code
   * opensearchOpaqueId} too. {@link BulkTelemetryConfig#DISABLED} when {@code attribution} is null.
   */
  @Nonnull
  static BulkTelemetryConfig bulkTelemetry(
      @Nullable RequestAttributionConfiguration attribution, @Nullable Tracer tracer) {
    if (attribution == null) {
      return BulkTelemetryConfig.DISABLED;
    }
    return BulkTelemetryConfig.builder()
        .tracer(tracer)
        .batchSpans(attribution.isEnabled())
        .opaqueId(attribution.isEnabled() && attribution.isOpensearchOpaqueId())
        .serviceName(attribution.getServiceName())
        .build();
  }

  /**
   * As {@link #build(SearchClientShim, BulkProcessorConfiguration, int, MetricUtils)}, with
   * bulk-write attribution per {@link #bulkTelemetry(RequestAttributionConfiguration, Tracer)}.
   */
  @Nonnull
  static ESBulkProcessor build(
      @Nonnull SearchClientShim<?> client,
      @Nonnull BulkProcessorConfiguration config,
      int threadCount,
      MetricUtils metricUtils,
      @Nullable RequestAttributionConfiguration attribution,
      @Nullable Tracer tracer) {
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
        .bulkTelemetry(bulkTelemetry(attribution, tracer))
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
