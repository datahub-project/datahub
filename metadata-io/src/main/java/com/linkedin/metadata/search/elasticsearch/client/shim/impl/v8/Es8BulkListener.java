package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@AllArgsConstructor
@Slf4j
public class Es8BulkListener
    implements co.elastic.clients.elasticsearch._helpers.bulk.BulkListener<Object> {

  private final MetricUtils metricUtils;

  @Override
  public void beforeBulk(
      long executionId,
      co.elastic.clients.elasticsearch.core.BulkRequest request,
      List<Object> objects) {}

  @Override
  public void afterBulk(
      long executionId,
      co.elastic.clients.elasticsearch.core.BulkRequest request,
      List<Object> objects,
      co.elastic.clients.elasticsearch.core.BulkResponse response) {
    String ingestTook = "";
    Long ingestTookInMillis = response.ingestTook();
    if (ingestTookInMillis != null) {
      ingestTook = " Bulk ingest preprocessing took time ms: " + ingestTookInMillis;
    }

    if (response.errors()) {
      log.error(
          "Failed to feed bulk request "
              + executionId
              + "."
              + " Number of events: "
              + response.items().size()
              + " Took time ms: "
              + response.took()
              + ingestTook
              + " Message: "
              + response);
    } else {
      log.info(
          "Successfully fed bulk request "
              + executionId
              + "."
              + " Number of events: "
              + response.items().size()
              + " Took time ms: "
              + response.took()
              + ingestTook);
    }
    incrementMetrics(metricUtils, response);
  }

  @Override
  public void afterBulk(
      long executionId,
      co.elastic.clients.elasticsearch.core.BulkRequest request,
      List<Object> objects,
      Throwable failure) {

    if (failure instanceof ElasticsearchException
        && isDocumentMissing((ElasticsearchException) failure)) {
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
      incrementMetrics(metricUtils, request, failure);
    }
  }

  private boolean isDocumentMissing(ElasticsearchException failure) {
    return "document_missing_exception".equals(StringUtils.toRootLowerCase(failure.error().type()));
  }

  public static String buildBulkRequestSummary(
      co.elastic.clients.elasticsearch.core.BulkRequest request) {
    return request.operations().stream()
        .map(
            req ->
                String.format(
                    "Failed to perform bulk request: index [%s], optype: [%s]",
                    req.index(), req._kind().name()))
        .collect(Collectors.joining(";"));
  }

  private void incrementMetrics(
      MetricUtils metricUtils,
      co.elastic.clients.elasticsearch.core.BulkRequest request,
      Throwable failure) {
    if (metricUtils != null)
      request.operations().stream()
          .map(req -> buildMetricName(req._kind().name(), "exception"))
          .forEach(
              metricName ->
                  metricUtils.exceptionIncrement(BulkListener.class, metricName, failure));
  }

  private String buildMetricName(String opType, String status) {
    return StringUtils.toRootLowerCase(opType) + MetricUtils.DELIMITER + status;
  }

  private void incrementMetrics(
      MetricUtils metricUtils, co.elastic.clients.elasticsearch.core.BulkResponse response) {
    if (metricUtils != null) {
      response.items().stream()
          .map(req -> buildMetricName(req.operationType().name(), String.valueOf(req.status())))
          .forEach(metricName -> metricUtils.increment(BulkListener.class, metricName, 1));
    }
  }
}
