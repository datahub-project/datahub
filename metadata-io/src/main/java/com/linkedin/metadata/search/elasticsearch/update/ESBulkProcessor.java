package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.tasks.TaskSubmissionResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;

@Slf4j
@Builder(builderMethodName = "hiddenBuilder")
public class ESBulkProcessor implements Closeable {
  private static final String ES_WRITES_METRIC = "num_elasticSearch_writes";
  private static final String ES_BATCHES_METRIC = "num_elasticSearch_batches_submitted";
  private static final String ES_DELETE_EXCEPTION_METRIC = "delete_by_query";
  private static final String ES_SUBMIT_DELETE_EXCEPTION_METRIC = "submit_delete_by_query_task";
  private static final String ES_SUBMIT_REINDEX_METRIC = "reindex_submit";
  private static final String ES_REINDEX_SUCCESS_METRIC = "reindex_success";
  private static final String ES_REINDEX_FAILED_METRIC = "reindex_failed";

  public static ESBulkProcessor.ESBulkProcessorBuilder builder(RestHighLevelClient searchClient) {
    return hiddenBuilder().searchClient(searchClient);
  }

  @NonNull private final RestHighLevelClient searchClient;
  @Builder.Default @NonNull private Boolean async = false;
  @Builder.Default @NonNull private Boolean batchDelete = false;
  @Builder.Default private Integer bulkRequestsLimit = 500;
  @Builder.Default private Integer bulkFlushPeriod = 1;
  @Builder.Default private Integer numRetries = 3;
  @Builder.Default private Long retryInterval = 1L;
  @Builder.Default private TimeValue defaultTimeout = TimeValue.timeValueMinutes(1);
  @Getter private final WriteRequest.RefreshPolicy writeRequestRefreshPolicy;

  @Setter(AccessLevel.NONE)
  @Getter(AccessLevel.NONE)
  private final BulkProcessor bulkProcessor;

  private ESBulkProcessor(
      @NonNull RestHighLevelClient searchClient,
      @NonNull Boolean async,
      @NonNull Boolean batchDelete,
      Integer bulkRequestsLimit,
      Integer bulkFlushPeriod,
      Integer numRetries,
      Long retryInterval,
      TimeValue defaultTimeout,
      WriteRequest.RefreshPolicy writeRequestRefreshPolicy,
      BulkProcessor ignored) {
    this.searchClient = searchClient;
    this.async = async;
    this.batchDelete = batchDelete;
    this.bulkRequestsLimit = bulkRequestsLimit;
    this.bulkFlushPeriod = bulkFlushPeriod;
    this.numRetries = numRetries;
    this.retryInterval = retryInterval;
    this.defaultTimeout = defaultTimeout;
    this.writeRequestRefreshPolicy = writeRequestRefreshPolicy;
    this.bulkProcessor = async ? toAsyncBulkProcessor() : toBulkProcessor();
  }

  public ESBulkProcessor add(DocWriteRequest<?> request) {
    MetricUtils.counter(this.getClass(), ES_WRITES_METRIC).inc();
    bulkProcessor.add(request);
    return this;
  }

  public Optional<BulkByScrollResponse> deleteByQuery(
      QueryBuilder queryBuilder, String... indices) {
    return deleteByQuery(queryBuilder, true, bulkRequestsLimit, defaultTimeout, indices);
  }

  public Optional<BulkByScrollResponse> deleteByQuery(
      QueryBuilder queryBuilder, boolean refresh, String... indices) {
    return deleteByQuery(queryBuilder, refresh, bulkRequestsLimit, defaultTimeout, indices);
  }

  public Optional<BulkByScrollResponse> deleteByQuery(
      QueryBuilder queryBuilder, boolean refresh, int limit, TimeValue timeout, String... indices) {
    DeleteByQueryRequest deleteByQueryRequest =
        new DeleteByQueryRequest()
            .setQuery(queryBuilder)
            .setBatchSize(limit)
            .setMaxRetries(numRetries)
            .setRetryBackoffInitialTime(TimeValue.timeValueSeconds(retryInterval))
            .setTimeout(timeout)
            .setRefresh(refresh);
    deleteByQueryRequest.indices(indices);

    try {
      if (!batchDelete) {
        // flush pending writes
        bulkProcessor.flush();
      }
      // perform delete after local flush
      final BulkByScrollResponse deleteResponse =
          searchClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
      MetricUtils.counter(this.getClass(), ES_WRITES_METRIC).inc(deleteResponse.getTotal());
      return Optional.of(deleteResponse);
    } catch (Exception e) {
      log.error("ERROR: Failed to delete by query. See stacktrace for a more detailed error:", e);
      MetricUtils.exceptionCounter(ESBulkProcessor.class, ES_DELETE_EXCEPTION_METRIC, e);
    }

    return Optional.empty();
  }

  public Optional<TaskSubmissionResponse> deleteByQueryAsync(
      QueryBuilder queryBuilder,
      boolean refresh,
      int limit,
      @Nullable TimeValue timeout,
      String... indices) {
    DeleteByQueryRequest deleteByQueryRequest =
        new DeleteByQueryRequest()
            .setQuery(queryBuilder)
            .setBatchSize(limit)
            .setMaxRetries(numRetries)
            .setRetryBackoffInitialTime(TimeValue.timeValueSeconds(retryInterval))
            .setRefresh(refresh);
    if (timeout != null) {
      deleteByQueryRequest.setTimeout(timeout);
    }
    // count the number of conflicts, but do not abort the operation
    deleteByQueryRequest.setConflicts("proceed");
    deleteByQueryRequest.indices(indices);
    try {
      // flush pending writes
      bulkProcessor.flush();
      TaskSubmissionResponse resp =
          searchClient.submitDeleteByQueryTask(deleteByQueryRequest, RequestOptions.DEFAULT);
      MetricUtils.counter(this.getClass(), ES_BATCHES_METRIC).inc();
      return Optional.of(resp);
    } catch (Exception e) {
      log.error(
          "ERROR: Failed to submit a delete by query task. See stacktrace for a more detailed error:",
          e);
      MetricUtils.exceptionCounter(ESBulkProcessor.class, ES_SUBMIT_DELETE_EXCEPTION_METRIC, e);
    }
    return Optional.empty();
  }

  private BulkProcessor toBulkProcessor() {
    return BulkProcessor.builder(
            (request, bulkListener) -> {
              try {
                BulkResponse response = searchClient.bulk(request, RequestOptions.DEFAULT);
                bulkListener.onResponse(response);
              } catch (IOException e) {
                bulkListener.onFailure(e);
                throw new RuntimeException(e);
              }
            },
            BulkListener.getInstance(writeRequestRefreshPolicy))
        .setBulkActions(bulkRequestsLimit)
        .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
        // This retry is ONLY for "resource constraints", i.e. 429 errors (each request has other
        // retry methods)
        .setBackoffPolicy(
            BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(retryInterval), numRetries))
        .build();
  }

  private BulkProcessor toAsyncBulkProcessor() {
    return BulkProcessor.builder(
            (request, bulkListener) -> {
              searchClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
            },
            BulkListener.getInstance(writeRequestRefreshPolicy))
        .setBulkActions(bulkRequestsLimit)
        .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
        // This retry is ONLY for "resource constraints", i.e. 429 errors (each request has other
        // retry methods)
        .setBackoffPolicy(
            BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(retryInterval), numRetries))
        .build();
  }

  @Override
  public void close() throws IOException {
    bulkProcessor.close();
  }

  public void flush() {
    bulkProcessor.flush();
  }
}
