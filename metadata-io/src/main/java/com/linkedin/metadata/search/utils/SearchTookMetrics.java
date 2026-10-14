package com.linkedin.metadata.search.utils;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;

/**
 * Runs a search and records ES {@code took} and the wall time spent outside it, tagged by {@code
 * operation}.
 */
public final class SearchTookMetrics {

  public static final String SEARCH_TOOK_METRIC = "datahub.elasticsearch.search.took";
  public static final String SEARCH_OUTSIDE_TOOK_METRIC =
      "datahub.elasticsearch.search.outside_took";
  public static final String OPERATION_TAG = "operation";

  private SearchTookMetrics() {}

  public static SearchResponse search(
      @Nonnull SearchClientShim<?> client,
      @Nonnull OperationContext opContext,
      @Nonnull SearchRequest searchRequest,
      @Nonnull String operation)
      throws IOException {
    return search(
        client, opContext, searchRequest, operation, opContext.getMetricUtils().orElse(null));
  }

  public static SearchResponse search(
      @Nonnull SearchClientShim<?> client,
      @Nonnull OperationContext opContext,
      @Nonnull SearchRequest searchRequest,
      @Nonnull String operation,
      @Nullable MetricUtils metricUtils)
      throws IOException {
    long start = System.nanoTime();
    SearchResponse response = client.search(opContext, searchRequest, RequestOptions.DEFAULT);
    long wallNanos = System.nanoTime() - start;
    if (metricUtils != null && response != null && response.getTook() != null) {
      long tookNanos = response.getTook().nanos();
      metricUtils.recordTimer(SEARCH_TOOK_METRIC, tookNanos, OPERATION_TAG, operation);
      metricUtils.recordTimer(
          SEARCH_OUTSIDE_TOOK_METRIC,
          Math.max(0L, wallNanos - tookNanos),
          OPERATION_TAG,
          operation);
    }
    return response;
  }
}
