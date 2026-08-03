package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemFailureClassifier;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.core.rest.RestStatus;

@Slf4j
public class Es8BulkListener
    implements co.elastic.clients.elasticsearch._helpers.bulk.BulkListener<Object> {

  private static final String METRIC_ITEM_REQUEUE = "bulk_item_requeue";
  private static final String METRIC_LWW_EXHAUSTED = "bulk_item_lww_exhausted";
  private static final String METRIC_TRANSFER_FAILURE = "bulk_item_transfer_failure";

  private final MetricUtils metricUtils;
  @Nullable private final BulkWriteResultTracker tracker;
  @Nullable private final BulkItemRequeueSupport requeueSupport;

  public Es8BulkListener(MetricUtils metricUtils) {
    this(metricUtils, null, null);
  }

  public Es8BulkListener(
      MetricUtils metricUtils,
      @Nullable BulkWriteResultTracker tracker,
      @Nullable BulkItemRequeueSupport requeueSupport) {
    this.metricUtils = metricUtils;
    this.tracker = tracker;
    this.requeueSupport = requeueSupport;
  }

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
      handleItemFailures(objects, response);
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
      recordSuccesses(objects, response.items().size());
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
      if (tracker != null) {
        tracker.recordCompleted(objects != null ? objects.size() : 0);
      }
      clearAttempts(objects);
      return;
    }

    log.error(
        "Error feeding bulk request {}. No retries left. Request: {}",
        executionId,
        buildBulkRequestSummary(request),
        failure);
    incrementMetrics(metricUtils, request, failure);

    int unrecovered = 0;
    if (objects != null) {
      for (Object context : objects) {
        DocWriteRequest<?> writeRequest =
            context instanceof DocWriteRequest ? (DocWriteRequest<?>) context : null;
        if (writeRequest != null
            && requeueSupport != null
            && requeueSupport.tryRequeue(writeRequest)) {
          incrementMetric(METRIC_ITEM_REQUEUE);
        } else {
          unrecovered++;
          if (requeueSupport != null && writeRequest != null) {
            requeueSupport.clearAttempts(writeRequest);
          }
        }
      }
    }
    if (tracker != null && unrecovered > 0) {
      tracker.recordUnrecoveredTransferFailure(unrecovered);
      incrementMetric(METRIC_TRANSFER_FAILURE, unrecovered);
    }
  }

  private void handleItemFailures(
      List<Object> objects, co.elastic.clients.elasticsearch.core.BulkResponse response) {
    List<BulkResponseItem> items = response.items();
    for (int i = 0; i < items.size(); i++) {
      BulkResponseItem item = items.get(i);
      DocWriteRequest<?> writeRequest = contextAt(objects, i);
      ErrorCause error = item.error();
      if (error == null) {
        if (requeueSupport != null && writeRequest != null) {
          requeueSupport.clearAttempts(writeRequest);
        }
        if (tracker != null) {
          tracker.recordCompleted(1);
        }
        continue;
      }

      String failureType = error.type();
      String failureMessage = failureType + (error.reason() != null ? ": " + error.reason() : "");
      RestStatus status = RestStatus.fromCode(item.status());

      if (BulkItemFailureClassifier.isDocumentMissing(failureType)
          || BulkItemFailureClassifier.isDocumentMissing(failureMessage)) {
        log.warn(
            "Skipping document_missing_exception for index [{}] id [{}]", item.index(), item.id());
        if (requeueSupport != null && writeRequest != null) {
          requeueSupport.clearAttempts(writeRequest);
        }
        if (tracker != null) {
          tracker.recordCompleted(1);
        }
        continue;
      }

      boolean versionConflict = BulkItemFailureClassifier.isVersionConflict(failureMessage);
      boolean retriable = BulkItemFailureClassifier.isRetriableFailure(status, failureMessage);

      if (retriable && requeueSupport != null && requeueSupport.tryRequeue(writeRequest)) {
        incrementMetric(METRIC_ITEM_REQUEUE);
        continue;
      }

      if (versionConflict) {
        if (requeueSupport != null && writeRequest != null) {
          requeueSupport.clearAttempts(writeRequest);
        }
        if (tracker != null) {
          tracker.recordLwwExhausted(1);
        }
        incrementMetric(METRIC_LWW_EXHAUSTED);
      } else {
        if (requeueSupport != null && writeRequest != null) {
          requeueSupport.clearAttempts(writeRequest);
        }
        if (tracker != null) {
          tracker.recordUnrecoveredTransferFailure(1);
        }
        incrementMetric(METRIC_TRANSFER_FAILURE);
      }
    }
  }

  private void recordSuccesses(List<Object> objects, int count) {
    clearAttempts(objects);
    if (tracker != null) {
      tracker.recordCompleted(count);
    }
  }

  private void clearAttempts(List<Object> objects) {
    if (requeueSupport == null || objects == null) {
      return;
    }
    for (Object context : objects) {
      if (context instanceof DocWriteRequest) {
        requeueSupport.clearAttempts((DocWriteRequest<?>) context);
      }
    }
  }

  @Nullable
  private static DocWriteRequest<?> contextAt(List<Object> objects, int index) {
    if (objects == null || index >= objects.size()) {
      return null;
    }
    Object context = objects.get(index);
    return context instanceof DocWriteRequest ? (DocWriteRequest<?>) context : null;
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

  private static void incrementMetrics(
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

  private static String buildMetricName(String opType, String status) {
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

  private void incrementMetric(String name) {
    incrementMetric(name, 1);
  }

  private void incrementMetric(String name, int count) {
    if (metricUtils != null) {
      metricUtils.increment(BulkListener.class, name, count);
    }
  }
}
