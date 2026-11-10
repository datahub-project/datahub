package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemFailureClassifier;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkProcessorCompletionTracker;
import com.linkedin.metadata.search.elasticsearch.update.BulkResponseClassifier;
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
  private static final String METRIC_CREATE_EXISTS = "bulk_item_create_exists";

  private final MetricUtils metricUtils;
  @Nullable private final BulkProcessorCompletionTracker completionTracker;
  @Nullable private final BulkWriteResultTracker tracker;
  @Nullable private final BulkItemRequeueSupport requeueSupport;

  @Nullable
  private final BulkResponseClassifier<co.elastic.clients.elasticsearch.core.BulkResponse>
      responseClassifier;

  public Es8BulkListener(MetricUtils metricUtils) {
    this(metricUtils, null, null, null, null);
  }

  public Es8BulkListener(
      MetricUtils metricUtils, @Nullable BulkProcessorCompletionTracker completionTracker) {
    this(metricUtils, completionTracker, null, null, null);
  }

  public Es8BulkListener(
      MetricUtils metricUtils,
      @Nullable BulkWriteResultTracker tracker,
      @Nullable BulkItemRequeueSupport requeueSupport) {
    this(metricUtils, null, tracker, requeueSupport, null);
  }

  public Es8BulkListener(
      MetricUtils metricUtils,
      @Nullable BulkProcessorCompletionTracker completionTracker,
      @Nullable BulkWriteResultTracker tracker,
      @Nullable BulkItemRequeueSupport requeueSupport) {
    this(metricUtils, completionTracker, tracker, requeueSupport, null);
  }

  public Es8BulkListener(
      MetricUtils metricUtils,
      @Nullable BulkProcessorCompletionTracker completionTracker,
      @Nullable BulkWriteResultTracker tracker,
      @Nullable BulkItemRequeueSupport requeueSupport,
      @Nullable
          BulkResponseClassifier<co.elastic.clients.elasticsearch.core.BulkResponse>
              responseClassifier) {
    this.metricUtils = metricUtils;
    this.completionTracker = completionTracker;
    this.tracker = tracker;
    this.requeueSupport = requeueSupport;
    this.responseClassifier = responseClassifier;
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

    final BulkResponseClassifier.ExpectedItemRejects expected =
        responseClassifier == null ? null : responseClassifier.classify(response, metricUtils);
    if (completionTracker != null) {
      completionTracker.recordEs8Response(objects, response);
    }
    if (response.errors()) {
      if (expected != null && expected.onlyExpected()) {
        log.info(
            "Bulk request {} completed with {} expected item rejects and {} updates of absent"
                + " documents skipped. Number of events: {} Took time ms: {}{}",
            executionId,
            expected.rejected(),
            expected.skipped(),
            response.items().size(),
            response.took(),
            ingestTook);
      } else {
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
      }
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
    if (completionTracker != null && objects != null) {
      completionTracker.recordEs8Failure(objects, failure);
    }

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
        DocWriteRequest<?> writeRequest = writeRequestFrom(context);
        if (writeRequest != null
            && !countedByDrain(writeRequest)
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
      boolean drainWillRedeliver = error != null && countedByDrain(writeRequest);
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

      // A create-only write that finds the document already present lost to a newer write.
      if (BulkItemFailureClassifier.isCreateConflict(writeRequest, status, failureMessage)) {
        log.debug(
            "Skipping create for existing document in index [{}] id [{}]", item.index(), item.id());
        if (requeueSupport != null && writeRequest != null) {
          requeueSupport.clearAttempts(writeRequest);
        }
        if (tracker != null) {
          tracker.recordCompleted(1);
        }
        incrementMetric(METRIC_CREATE_EXISTS);
        continue;
      }

      boolean versionConflict = BulkItemFailureClassifier.isVersionConflict(failureMessage);
      boolean retriable =
          !BulkItemFailureClassifier.isCreateConflict(writeRequest, status, failureMessage)
              && BulkItemFailureClassifier.isRetriableFailure(status, failureMessage);

      if (retriable
          && !drainWillRedeliver
          && requeueSupport != null
          && requeueSupport.tryRequeue(writeRequest)) {
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

  /**
   * A drain already counted this write and will fail closed so the caller redelivers it. Requeue
   * would submit again on the bulk callback thread, which that drain does not wait for.
   */
  private boolean countedByDrain(@Nullable DocWriteRequest<?> writeRequest) {
    return completionTracker != null && completionTracker.countedByDrain(writeRequest);
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
      DocWriteRequest<?> writeRequest = writeRequestFrom(context);
      if (writeRequest != null) {
        requeueSupport.clearAttempts(writeRequest);
      }
    }
  }

  @Nullable
  private static DocWriteRequest<?> contextAt(List<Object> objects, int index) {
    if (objects == null || index >= objects.size()) {
      return null;
    }
    return writeRequestFrom(objects.get(index));
  }

  /**
   * The ES8 ingester context is the completion {@link BulkProcessorCompletionTracker.Submission}.
   * Older callers still pass the write request itself.
   */
  @Nullable
  private static DocWriteRequest<?> writeRequestFrom(@Nullable final Object context) {
    if (context instanceof DocWriteRequest) {
      return (DocWriteRequest<?>) context;
    }
    if (context instanceof BulkProcessorCompletionTracker.Submission) {
      return ((BulkProcessorCompletionTracker.Submission) context).getRequest();
    }
    return null;
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
