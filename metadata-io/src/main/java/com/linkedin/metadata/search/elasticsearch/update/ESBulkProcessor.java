package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
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
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.script.Script;

@Slf4j
@Builder(builderMethodName = "hiddenBuilder")
public class ESBulkProcessor implements Closeable {
  private static final String ES_WRITES_METRIC = "num_elasticSearch_writes";
  private static final String ES_BATCHES_METRIC = "num_elasticSearch_batches_submitted";
  private static final String ES_DELETE_EXCEPTION_METRIC = "delete_by_query";
  private static final String ES_UPDATE_EXCEPTION_METRIC = "update_by_query";
  private static final String ES_SUBMIT_DELETE_EXCEPTION_METRIC = "submit_delete_by_query_task";
  private static final String ES_SUBMIT_REINDEX_METRIC = "reindex_submit";
  private static final String ES_REINDEX_SUCCESS_METRIC = "reindex_success";
  private static final String ES_REINDEX_FAILED_METRIC = "reindex_failed";

  public static ESBulkProcessor.ESBulkProcessorBuilder builder(
      RestHighLevelClient searchClient, MetricUtils metricUtils) {
    return hiddenBuilder().metricUtils(metricUtils).searchClient(searchClient);
  }

  @NonNull private final RestHighLevelClient searchClient;
  @Builder.Default @NonNull private Boolean async = false;
  @Builder.Default @NonNull private Boolean batchDelete = false;
  @Builder.Default private Integer bulkRequestsLimit = 500;
  @Builder.Default private Integer bulkFlushPeriod = 1;
  @Builder.Default private Integer numRetries = 3;
  @Builder.Default private Long retryInterval = 1L;
  @Builder.Default private TimeValue defaultTimeout = TimeValue.timeValueMinutes(1);
  @Builder.Default private Integer threadCount = 1; // Default to single processor
  @Getter private final WriteRequest.RefreshPolicy writeRequestRefreshPolicy;

  // Multiple processors for parallel execution - initialized in constructor, not included in
  // builder
  private BulkProcessor[] bulkProcessors;

  private AtomicInteger roundRobinCounter;

  private final MetricUtils metricUtils;

  private ESBulkProcessor(
      @NonNull RestHighLevelClient searchClient,
      @NonNull Boolean async,
      @NonNull Boolean batchDelete,
      Integer bulkRequestsLimit,
      Integer bulkFlushPeriod,
      Integer numRetries,
      Long retryInterval,
      TimeValue defaultTimeout,
      Integer threadCount,
      WriteRequest.RefreshPolicy writeRequestRefreshPolicy,
      BulkProcessor[] bulkProcessors,
      AtomicInteger roundRobinCounter,
      MetricUtils metricUtils) {
    this.searchClient = searchClient;
    this.async = async;
    this.batchDelete = batchDelete;
    this.bulkRequestsLimit = bulkRequestsLimit;
    this.bulkFlushPeriod = bulkFlushPeriod;
    this.numRetries = numRetries;
    this.retryInterval = retryInterval;
    this.defaultTimeout = defaultTimeout;
    this.threadCount = threadCount;
    this.writeRequestRefreshPolicy = writeRequestRefreshPolicy;
    this.metricUtils = metricUtils;
    // Set fields passed from builder
    this.bulkProcessors = bulkProcessors != null ? bulkProcessors : new BulkProcessor[threadCount];
    this.roundRobinCounter = roundRobinCounter != null ? roundRobinCounter : new AtomicInteger(0);

    // Initialize BulkProcessors if they weren't passed from builder
    if (this.bulkProcessors.length == 0 || this.bulkProcessors[0] == null) {
      this.bulkProcessors = new BulkProcessor[threadCount];
      for (int i = 0; i < threadCount; i++) {
        this.bulkProcessors[i] = async ? toAsyncBulkProcessor(i) : toBulkProcessor(i);
      }
      log.info(
          "Initialized ESBulkProcessor with {} BulkProcessor instances for parallel execution",
          threadCount);
    }
  }

  public ESBulkProcessor add(DocWriteRequest<?> request) {
    if (metricUtils != null) metricUtils.increment(this.getClass(), ES_WRITES_METRIC, 1);

    // Round-robin distribution across processors
    int index = roundRobinCounter.getAndIncrement() % threadCount;
    bulkProcessors[index].add(request);

    log.debug(
        "Added request id: {}, operation type: {}, index: {}",
        request.id(),
        request.opType(),
        request.index());
    return this;
  }

  /**
   * Add a request with URN-based routing for entity document consistency. This method routes all
   * operations for the same URN to the same BulkProcessor to ensure consistent ordering and avoid
   * conflicts when updating the same entity.
   *
   * @param urn the URN of the entity being updated
   * @param request the document write request
   * @return this ESBulkProcessor instance
   */
  public ESBulkProcessor add(@NonNull String urn, @NonNull DocWriteRequest<?> request) {
    if (metricUtils != null) metricUtils.increment(this.getClass(), ES_WRITES_METRIC, 1);

    // URN-based consistent hashing for entity document consistency
    int index = Math.abs(urn.hashCode()) % threadCount;
    bulkProcessors[index].add(request);

    log.debug(
        "Added URN-aware request urn: {}, id: {}, operation type: {}, index: {}",
        urn,
        request.id(),
        request.opType(),
        request.index());
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

  public Optional<BulkByScrollResponse> updateByQuery(
      Script script, QueryBuilder queryBuilder, String... indices) {
    // Create an UpdateByQueryRequest
    UpdateByQueryRequest updateByQuery = new UpdateByQueryRequest(indices);
    updateByQuery.setQuery(queryBuilder);
    updateByQuery.setScript(script);

    try {
      final BulkByScrollResponse updateResponse =
          searchClient.updateByQuery(updateByQuery, RequestOptions.DEFAULT);
      if (metricUtils != null)
        metricUtils.increment(this.getClass(), ES_WRITES_METRIC, updateResponse.getTotal());
      return Optional.of(updateResponse);
    } catch (Exception e) {
      log.error("ERROR: Failed to update by query. See stacktrace for a more detailed error:", e);
      if (metricUtils != null)
        metricUtils.exceptionIncrement(ESBulkProcessor.class, ES_UPDATE_EXCEPTION_METRIC, e);
    }

    return Optional.empty();
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
        flush();
      }
      // perform delete after local flush
      final BulkByScrollResponse deleteResponse =
          searchClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
      if (metricUtils != null)
        metricUtils.increment(this.getClass(), ES_WRITES_METRIC, deleteResponse.getTotal());
      return Optional.of(deleteResponse);
    } catch (Exception e) {
      log.error("ERROR: Failed to delete by query. See stacktrace for a more detailed error:", e);
      if (metricUtils != null)
        metricUtils.exceptionIncrement(ESBulkProcessor.class, ES_DELETE_EXCEPTION_METRIC, e);
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
      flush();
      TaskSubmissionResponse resp =
          searchClient.submitDeleteByQueryTask(deleteByQueryRequest, RequestOptions.DEFAULT);
      if (metricUtils != null) metricUtils.increment(this.getClass(), ES_BATCHES_METRIC, 1);
      return Optional.of(resp);
    } catch (Exception e) {
      log.error(
          "ERROR: Failed to submit a delete by query task. See stacktrace for a more detailed error:",
          e);
      if (metricUtils != null)
        metricUtils.exceptionIncrement(ESBulkProcessor.class, ES_SUBMIT_DELETE_EXCEPTION_METRIC, e);
    }
    return Optional.empty();
  }

  private BulkProcessor toBulkProcessor(int processorIndex) {
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
            BulkListener.getInstance(processorIndex, writeRequestRefreshPolicy, metricUtils))
        .setBulkActions(bulkRequestsLimit)
        .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
        // This retry is ONLY for "resource constraints", i.e. 429 errors (each request has other
        // retry methods)
        .setBackoffPolicy(
            BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(retryInterval), numRetries))
        .build();
  }

  private BulkProcessor toAsyncBulkProcessor(int processorIndex) {
    return BulkProcessor.builder(
            (request, bulkListener) -> {
              searchClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
            },
            BulkListener.getInstance(processorIndex, writeRequestRefreshPolicy, metricUtils))
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
    flush(); // Make sure pending operations are flushed

    // Close all processors
    for (BulkProcessor processor : bulkProcessors) {
      processor.close();
    }
  }

  public void flush() {
    // Flush all processors
    for (BulkProcessor processor : bulkProcessors) {
      processor.flush();
    }
  }
}
