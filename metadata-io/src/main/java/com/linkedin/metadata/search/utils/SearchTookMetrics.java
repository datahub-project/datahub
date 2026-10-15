package com.linkedin.metadata.search.utils;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchResponse;

/**
 * Times a search call and records ES {@code took} and the wall time spent outside it, tagged by
 * {@link SearchOperation}. Pass only the search call, or its work is counted as outside took.
 */
public final class SearchTookMetrics {

  public static final String SEARCH_TOOK_METRIC = "datahub.elasticsearch.search.took";
  public static final String SEARCH_OUTSIDE_TOOK_METRIC =
      "datahub.elasticsearch.search.outside_took";
  public static final String OPERATION_TAG = "operation";

  @FunctionalInterface
  public interface SearchCall {
    SearchResponse execute() throws IOException;
  }

  private SearchTookMetrics() {}

  public static SearchResponse time(
      @Nonnull OperationContext opContext,
      @Nonnull SearchOperation operation,
      @Nonnull SearchCall search)
      throws IOException {
    return time(opContext.getMetricUtils().orElse(null), operation, search);
  }

  public static SearchResponse time(
      @Nullable MetricUtils metricUtils,
      @Nonnull SearchOperation operation,
      @Nonnull SearchCall search)
      throws IOException {
    long start = System.nanoTime();
    SearchResponse response = search.execute();
    long wallNanos = System.nanoTime() - start;
    if (metricUtils != null && response != null && response.getTook() != null) {
      long tookNanos = response.getTook().nanos();
      metricUtils.recordTimer(SEARCH_TOOK_METRIC, tookNanos, OPERATION_TAG, operation.getTag());
      metricUtils.recordTimer(
          SEARCH_OUTSIDE_TOOK_METRIC,
          Math.max(0L, wallNanos - tookNanos),
          OPERATION_TAG,
          operation.getTag());
    }
    return response;
  }
}
