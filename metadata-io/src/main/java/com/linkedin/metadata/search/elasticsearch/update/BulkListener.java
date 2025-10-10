package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.index.engine.DocumentMissingException;

@Slf4j
public class BulkListener implements BulkProcessor.Listener {
  private static final Map<String, BulkListener> INSTANCES = new HashMap<>();

  public static BulkListener getInstance(MetricUtils metricUtils) {
    return INSTANCES.computeIfAbsent("null", p -> new BulkListener(null, metricUtils));
  }

  public static BulkListener getInstance(
      int processorIndex, WriteRequest.RefreshPolicy refreshPolicy, MetricUtils metricUtils) {
    String key = processorIndex + ":" + refreshPolicy;
    return INSTANCES.computeIfAbsent(key, p -> new BulkListener(refreshPolicy, metricUtils));
  }

  private final String processorId;
  private final WriteRequest.RefreshPolicy refreshPolicy;
  private final MetricUtils metricUtils;

  public BulkListener(
      String processorId, WriteRequest.RefreshPolicy policy, MetricUtils metricUtils) {
    this.processorId = processorId;
    this.refreshPolicy = policy;
    this.metricUtils = metricUtils;
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request) {
    if (refreshPolicy != null) {
      request.setRefreshPolicy(refreshPolicy);
    }
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
    String ingestTook = "";
    long ingestTookInMillis = response.getIngestTookInMillis();
    if (ingestTookInMillis != BulkResponse.NO_INGEST_TOOK) {
      ingestTook = " Bulk ingest preprocessing took time ms: " + ingestTookInMillis;
    }

    if (response.hasFailures()) {
      log.error(
          "Failed to feed bulk request {} via {}."
              + " Number of events: {} Took time ms: {}{} Message: {}",
          executionId,
          processorId,
          response.getItems().length,
          response.getTook().getMillis(),
          ingestTook,
          response.buildFailureMessage());
    } else {
      log.info(
          "Successfully fed bulk request {} via {}." + " Number of events: {} Took time ms: {}{}",
          executionId,
          processorId,
          response.getItems().length,
          response.getTook().getMillis(),
          ingestTook);
    }
    incrementMetrics(response);
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

    Throwable unwrappedFailure = ExceptionsHelper.unwrapCause(failure);

    if (unwrappedFailure instanceof DocumentMissingException) {
      log.warn(
          "Attempting to bulk load a missing document. executionId: {}.  No retries left. Request: {}",
          executionId,
          buildBulkRequestSummary(request),
          failure);
    } else {
      // Exception raised outside this method
      log.error(
          "Error feeding bulk request {}. No retries left. Request: {}",
          executionId,
          buildBulkRequestSummary(request),
          failure);
      incrementMetrics(request, failure);
    }
  }

  private void incrementMetrics(BulkResponse response) {
    if (metricUtils != null)
      Arrays.stream(response.getItems())
          .map(req -> buildMetricName(req.getOpType(), req.status().name()))
          .forEach(metricName -> metricUtils.increment(BulkListener.class, metricName, 1));
  }

  private void incrementMetrics(BulkRequest request, Throwable failure) {
    if (metricUtils != null)
      request.requests().stream()
          .map(req -> buildMetricName(req.opType(), "exception"))
          .forEach(
              metricName ->
                  metricUtils.exceptionIncrement(BulkListener.class, metricName, failure));
  }

  private static String buildMetricName(DocWriteRequest.OpType opType, String status) {
    return opType.getLowercase() + MetricUtils.DELIMITER + status.toLowerCase();
  }

  public static String buildBulkRequestSummary(BulkRequest request) {
    return request.requests().stream()
        .map(
            req ->
                String.format(
                    "Failed to perform bulk request: index [%s], optype: [%s], type [%s], id [%s]",
                    req.index(), req.opType(), req.opType(), req.id()))
        .collect(Collectors.joining(";"));
  }
}
