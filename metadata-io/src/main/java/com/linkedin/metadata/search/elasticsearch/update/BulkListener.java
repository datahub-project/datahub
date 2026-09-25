package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.core.rest.RestStatus;

@Slf4j
public class BulkListener implements BulkProcessor.Listener {
  private static final Map<String, BulkListener> INSTANCES = new HashMap<>();
  private static final String METRIC_ITEM_REQUEUE = "bulk_item_requeue";
  private static final String METRIC_LWW_EXHAUSTED = "bulk_item_lww_exhausted";
  private static final String METRIC_TRANSFER_FAILURE = "bulk_item_transfer_failure";

  public static BulkListener getInstance(MetricUtils metricUtils) {
    return INSTANCES.computeIfAbsent("null", p -> new BulkListener(null, metricUtils, null, null));
  }

  public static BulkListener getInstance(
      int processorIndex, WriteRequest.RefreshPolicy refreshPolicy, MetricUtils metricUtils) {
    String key = processorIndex + ":" + refreshPolicy;
    return INSTANCES.computeIfAbsent(
        key, p -> new BulkListener(refreshPolicy, metricUtils, null, null));
  }

  /**
   * Creates a non-cached listener with write-result tracking and optional item requeue. Prefer this
   * for production bulk processors.
   */
  public static BulkListener create(
      @Nullable WriteRequest.RefreshPolicy refreshPolicy,
      @Nullable MetricUtils metricUtils,
      @Nullable BulkWriteResultTracker tracker,
      @Nullable BulkItemRequeueSupport requeueSupport) {
    return new BulkListener(refreshPolicy, metricUtils, tracker, requeueSupport);
  }

  private final WriteRequest.RefreshPolicy refreshPolicy;
  private final MetricUtils metricUtils;
  @Nullable private final BulkWriteResultTracker tracker;
  @Nullable private final BulkItemRequeueSupport requeueSupport;

  public BulkListener(WriteRequest.RefreshPolicy policy, MetricUtils metricUtils) {
    this(policy, metricUtils, null, null);
  }

  BulkListener(
      WriteRequest.RefreshPolicy policy,
      MetricUtils metricUtils,
      @Nullable BulkWriteResultTracker tracker,
      @Nullable BulkItemRequeueSupport requeueSupport) {
    refreshPolicy = policy;
    this.metricUtils = metricUtils;
    this.tracker = tracker;
    this.requeueSupport = requeueSupport;
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
          "Failed to feed bulk request "
              + executionId
              + "."
              + " Number of events: "
              + response.getItems().length
              + " Took time ms: "
              + response.getTook().getMillis()
              + ingestTook
              + " Message: "
              + response.buildFailureMessage());
      handleItemFailures(request, response);
    } else {
      log.info(
          "Successfully fed bulk request "
              + executionId
              + "."
              + " Number of events: "
              + response.getItems().length
              + " Took time ms: "
              + response.getTook().getMillis()
              + ingestTook);
      recordSuccesses(request, response.getItems().length);
    }
    incrementMetrics(response);
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
    if (BulkItemFailureClassifier.isDocumentMissing(failure.getMessage())) {
      log.warn(
          "Attempting to bulk load a missing document. executionId: {}. Request: {}",
          executionId,
          buildBulkRequestSummary(request),
          failure);
      if (tracker != null) {
        tracker.recordCompleted(request.numberOfActions());
      }
      clearAttempts(request);
      return;
    }

    log.error(
        "Error feeding bulk request {}. No retries left. Request: {}",
        executionId,
        buildBulkRequestSummary(request),
        failure);
    incrementMetrics(request, failure);

    // Transport-level failure: try requeue each request; otherwise unrecovered transfer failure.
    int unrecovered = 0;
    int requeued = 0;
    for (DocWriteRequest<?> writeRequest : request.requests()) {
      if (requeueSupport != null && requeueSupport.tryRequeue(writeRequest)) {
        requeued++;
        incrementMetric(METRIC_ITEM_REQUEUE);
      } else {
        unrecovered++;
        if (requeueSupport != null) {
          requeueSupport.clearAttempts(writeRequest);
        }
      }
    }
    if (tracker != null) {
      if (requeued > 0) {
        // requeued items remain pending
      }
      if (unrecovered > 0) {
        tracker.recordUnrecoveredTransferFailure(unrecovered);
        incrementMetric(METRIC_TRANSFER_FAILURE, unrecovered);
      }
    }
  }

  private void handleItemFailures(@Nonnull BulkRequest request, @Nonnull BulkResponse response) {
    BulkItemResponse[] items = response.getItems();
    for (int i = 0; i < items.length; i++) {
      BulkItemResponse item = items[i];
      DocWriteRequest<?> writeRequest =
          i < request.requests().size() ? request.requests().get(i) : null;
      if (!item.isFailed()) {
        if (requeueSupport != null && writeRequest != null) {
          requeueSupport.clearAttempts(writeRequest);
        }
        if (tracker != null) {
          tracker.recordCompleted(1);
        }
        continue;
      }

      BulkItemResponse.Failure failure = item.getFailure();
      RestStatus status = failure != null ? failure.getStatus() : null;
      String failureMessage = failure != null ? failure.getMessage() : item.getFailureMessage();

      if (BulkItemFailureClassifier.isDocumentMissing(failureMessage)) {
        log.warn(
            "Skipping document_missing_exception for index [{}] id [{}]",
            item.getIndex(),
            item.getId());
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
        // still pending — do not recordCompleted
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

  private void recordSuccesses(@Nonnull BulkRequest request, int count) {
    clearAttempts(request);
    if (tracker != null) {
      tracker.recordCompleted(count);
    }
  }

  private void clearAttempts(@Nonnull BulkRequest request) {
    if (requeueSupport == null) {
      return;
    }
    for (DocWriteRequest<?> writeRequest : request.requests()) {
      requeueSupport.clearAttempts(writeRequest);
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

  private void incrementMetric(String name) {
    incrementMetric(name, 1);
  }

  private void incrementMetric(String name, int count) {
    if (metricUtils != null) {
      metricUtils.increment(BulkListener.class, name, count);
    }
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
