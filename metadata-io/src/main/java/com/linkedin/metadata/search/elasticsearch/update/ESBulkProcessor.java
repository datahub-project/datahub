package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
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
      SearchClientShim<?> searchClient, MetricUtils metricUtils) {
    return hiddenBuilder().metricUtils(metricUtils).searchClient(searchClient);
  }

  @Nonnull private final SearchClientShim<?> searchClient;
  @Builder.Default @Nonnull private Boolean async = false;
  @Builder.Default @Nonnull private Boolean batchDelete = false;
  @Builder.Default private Integer bulkRequestsLimit = 500;
  @Builder.Default private Integer bulkFlushPeriod = 1;
  @Builder.Default private Integer numRetries = 3;
  @Builder.Default private Long retryInterval = 1L;
  @Builder.Default private TimeValue defaultTimeout = TimeValue.timeValueMinutes(1);
  @Getter private final WriteRequest.RefreshPolicy writeRequestRefreshPolicy;

  private final MetricUtils metricUtils;

  private ESBulkProcessor(
      @Nonnull SearchClientShim<?> searchClient,
      @Nonnull Boolean async,
      @Nonnull Boolean batchDelete,
      Integer bulkRequestsLimit,
      Integer bulkFlushPeriod,
      Integer numRetries,
      Long retryInterval,
      TimeValue defaultTimeout,
      WriteRequest.RefreshPolicy writeRequestRefreshPolicy,
      MetricUtils metricUtils) {
    this.searchClient = searchClient;
    this.async = async;
    this.batchDelete = batchDelete;
    this.bulkRequestsLimit = bulkRequestsLimit;
    this.bulkFlushPeriod = bulkFlushPeriod;
    this.numRetries = numRetries;
    this.retryInterval = retryInterval;
    this.defaultTimeout = defaultTimeout;
    this.writeRequestRefreshPolicy = writeRequestRefreshPolicy;
    if (async) {
      searchClient.generateAsyncBulkProcessor(
          writeRequestRefreshPolicy,
          metricUtils,
          bulkRequestsLimit,
          bulkFlushPeriod,
          retryInterval,
          numRetries);
    } else {
      searchClient.generateBulkProcessor(
          writeRequestRefreshPolicy,
          metricUtils,
          bulkRequestsLimit,
          bulkFlushPeriod,
          retryInterval,
          numRetries);
    }
    this.metricUtils = metricUtils;
  }

  public ESBulkProcessor add(DocWriteRequest<?> request) {
    if (metricUtils != null) metricUtils.increment(this.getClass(), ES_WRITES_METRIC, 1);
    searchClient.addBulk(request);
    log.debug(
        "Added request id: {}, operation type: {}, index: {}",
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
        searchClient.flushBulkProcessor();
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

  public Optional<String> deleteByQueryAsync(
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
      searchClient.flushBulkProcessor();
      String resp =
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

  @Override
  public void close() throws IOException {
    flush(); // Make sure pending operations are flushed
    searchClient.closeBulkProcessor();
  }

  public void flush() {
    searchClient.flushBulkProcessor();
  }
}
