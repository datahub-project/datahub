package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.WriteRequest;

@Slf4j
public class BulkListener implements BulkProcessor.Listener {
  private static final Map<WriteRequest.RefreshPolicy, BulkListener> INSTANCES = new HashMap<>();

  public static BulkListener getInstance() {
    return INSTANCES.computeIfAbsent(null, BulkListener::new);
  }

  public static BulkListener getInstance(WriteRequest.RefreshPolicy refreshPolicy) {
    return INSTANCES.computeIfAbsent(refreshPolicy, BulkListener::new);
  }

  private final WriteRequest.RefreshPolicy refreshPolicy;

  public BulkListener(WriteRequest.RefreshPolicy policy) {
    refreshPolicy = policy;
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request) {
    if (refreshPolicy != null) {
      request.setRefreshPolicy(refreshPolicy);
    }
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
    if (response.hasFailures()) {
      log.error(
          "Failed to feed bulk request. Number of events: "
              + response.getItems().length
              + " Took time ms: "
              + response.getIngestTookInMillis()
              + " Message: "
              + response.buildFailureMessage());
    } else {
      log.info(
          "Successfully fed bulk request. Number of events: "
              + response.getItems().length
              + " Took time ms: "
              + response.getIngestTookInMillis());
    }
    incrementMetrics(response);
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
    // Exception raised outside this method
    log.error(
        "Error feeding bulk request. No retries left. Request: {}",
        buildBulkRequestSummary(request),
        failure);
    incrementMetrics(request, failure);
  }

  private static void incrementMetrics(BulkResponse response) {
    Arrays.stream(response.getItems())
        .map(req -> buildMetricName(req.getOpType(), req.status().name()))
        .forEach(metricName -> MetricUtils.counter(BulkListener.class, metricName).inc());
  }

  private static void incrementMetrics(BulkRequest request, Throwable failure) {
    request.requests().stream()
        .map(req -> buildMetricName(req.opType(), "exception"))
        .forEach(
            metricName -> MetricUtils.exceptionCounter(BulkListener.class, metricName, failure));
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
