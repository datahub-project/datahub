package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemFailureClassifier;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkTelemetry;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.core.rest.RestStatus;

/**
 * Bulk listener for the Elasticsearch 8 {@code BulkIngester}; item bookkeeping mirrors {@link
 * BulkListener}.
 *
 * <p>Bulk-write attribution is partial here, by construction of the ingester: {@link BulkTelemetry}
 * produces the {@code index bulk} span (batch id, action count, indices, took, failures, links to
 * the actions' origins) from {@code beforeBulk}/{@code afterBulk}, but the ingester owns the HTTP
 * call. Its builder exposes no per-request {@code TransportOptions} or header hook, only a client
 * and fixed global settings, so no {@code X-Opaque-Id} is sent for the batch, and the span is never
 * made current around the call, so an agent's client span for the bulk request stays a separate
 * span rather than nesting under the batch span. The OpenSearch client path does both.
 */
@Slf4j
public class Es8BulkListener
    implements co.elastic.clients.elasticsearch._helpers.bulk.BulkListener<Object> {

  private static final String METRIC_ITEM_REQUEUE = "bulk_item_requeue";
  private static final String METRIC_LWW_EXHAUSTED = "bulk_item_lww_exhausted";
  private static final String METRIC_TRANSFER_FAILURE = "bulk_item_transfer_failure";

  private final MetricUtils metricUtils;
  @Nullable private final BulkWriteResultTracker tracker;
  @Nullable private final BulkItemRequeueSupport requeueSupport;
  private final BulkTelemetry telemetry;

  public Es8BulkListener(MetricUtils metricUtils) {
    this(metricUtils, null, null);
  }

  public Es8BulkListener(
      MetricUtils metricUtils,
      @Nullable BulkWriteResultTracker tracker,
      @Nullable BulkItemRequeueSupport requeueSupport) {
    this(metricUtils, tracker, requeueSupport, null);
  }

  /** With bulk-write attribution (see {@link BulkTelemetry}): a span per flushed batch. */
  public Es8BulkListener(
      MetricUtils metricUtils,
      @Nullable BulkWriteResultTracker tracker,
      @Nullable BulkItemRequeueSupport requeueSupport,
      @Nullable BulkTelemetry telemetry) {
    this.metricUtils = metricUtils;
    this.tracker = tracker;
    this.requeueSupport = requeueSupport;
    this.telemetry = telemetry != null ? telemetry : BulkTelemetry.disabled();
  }

  @Override
  public void beforeBulk(
      long executionId,
      co.elastic.clients.elasticsearch.core.BulkRequest request,
      List<Object> objects) {
    // The ingester owns the client, so no per-batch X-Opaque-Id here; the span still carries the
    // batch id and links to the actions' origins. Guarded so the disabled path allocates nothing.
    if (telemetry.isEnabled()) {
      telemetry.beforeBulk(request, writeRequests(objects));
    }
  }

  @Override
  public void afterBulk(
      long executionId,
      co.elastic.clients.elasticsearch.core.BulkRequest request,
      List<Object> objects,
      co.elastic.clients.elasticsearch.core.BulkResponse response) {
    if (telemetry.isEnabled()) {
      // The failed actions' origins are carried until handleItemFailures requeues or forgets them.
      telemetry.afterBulk(request, response.took(), failedActions(objects, response));
    }
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
    telemetry.afterBulk(request, failure);

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
      forgetAll(objects);
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
          telemetry.forget(writeRequest);
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
        telemetry.forget(writeRequest);
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

      // Giving up on the item: drop its carried origin along with its requeue attempts.
      if (requeueSupport != null && writeRequest != null) {
        requeueSupport.clearAttempts(writeRequest);
      }
      telemetry.forget(writeRequest);
      if (versionConflict) {
        if (tracker != null) {
          tracker.recordLwwExhausted(1);
        }
        incrementMetric(METRIC_LWW_EXHAUSTED);
      } else {
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

  private void forgetAll(@Nullable List<Object> objects) {
    if (!telemetry.isEnabled() || objects == null) {
      return;
    }
    for (Object context : objects) {
      telemetry.forget(context);
    }
  }

  private static List<DocWriteRequest<?>> writeRequests(@Nullable List<Object> objects) {
    List<DocWriteRequest<?>> out = new ArrayList<>(objects == null ? 0 : objects.size());
    if (objects != null) {
      for (Object context : objects) {
        if (context instanceof DocWriteRequest) {
          out.add((DocWriteRequest<?>) context);
        }
      }
    }
    return out;
  }

  /** The contexts whose items failed, by position; only computed when telemetry is on. */
  private static List<Object> failedActions(
      @Nullable List<Object> objects, co.elastic.clients.elasticsearch.core.BulkResponse response) {
    List<Object> failed = new ArrayList<>();
    List<BulkResponseItem> items = response.items();
    for (int i = 0; i < items.size(); i++) {
      if (items.get(i).error() != null && objects != null && i < objects.size()) {
        failed.add(objects.get(i));
      }
    }
    return failed;
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
