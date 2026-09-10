package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8.Es8BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.testng.annotations.Test;

public class Es8BulkListenerFailureHandlingTest {

  @Test
  public void testSuccessCompletesPending() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    MetricUtils metricUtils = mock(MetricUtils.class);
    Es8BulkListener listener = new Es8BulkListener(metricUtils, tracker, null);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkResponseItem item = successItem("idx", "1");
    BulkResponse response =
        new BulkResponse.Builder().errors(false).took(1L).ingestTook(2L).items(item).build();

    listener.afterBulk(1L, emptyBulkRequest(), List.of(indexRequest), response);

    assertEquals(tracker.getPendingItems(), 0);
  }

  @Test
  public void testMixedResponseSuccessItemClearsAttemptsAndCompletes() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(2);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    Es8BulkListener listener = new Es8BulkListener(null, tracker, support);

    IndexRequest ok = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    IndexRequest bad = new IndexRequest("idx").id("2").source(Collections.emptyMap());
    BulkResponse response =
        new BulkResponse.Builder()
            .errors(true)
            .took(1L)
            .items(
                successItem("idx", "1"),
                failureItem("idx", "2", 400, "mapper_parsing_exception", "bad mapping"))
            .build();

    listener.afterBulk(1L, emptyBulkRequest(), List.of(ok, bad), response);

    verify(requeue, never()).accept(any());
    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 1);
  }

  @Test
  public void testDocumentMissingItemIsCompletedNotRequeued() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    Es8BulkListener listener = new Es8BulkListener(null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkResponse response =
        new BulkResponse.Builder()
            .errors(true)
            .took(1L)
            .items(failureItem("idx", "1", 404, "document_missing_exception", "document missing"))
            .build();

    listener.afterBulk(1L, emptyBulkRequest(), List.of(indexRequest), response);

    verify(requeue, never()).accept(any());
    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testVersionConflictRequeue() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    MetricUtils metricUtils = mock(MetricUtils.class);
    Es8BulkListener listener = new Es8BulkListener(metricUtils, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkResponse response =
        new BulkResponse.Builder()
            .errors(true)
            .took(1L)
            .items(
                failureItem(
                    "idx", "1", 409, "version_conflict_engine_exception", "version conflict"))
            .build();

    listener.afterBulk(1L, emptyBulkRequest(), List.of(indexRequest), response);

    verify(requeue, times(1)).accept(any(DocWriteRequest.class));
    assertEquals(tracker.getPendingItems(), 1);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testExhaustedConflictIsLwwNotTransferFailure() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 0, req -> {});
    Es8BulkListener listener = new Es8BulkListener(null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkResponse response =
        new BulkResponse.Builder()
            .errors(true)
            .took(1L)
            .items(
                failureItem(
                    "idx", "1", 409, "version_conflict_engine_exception", "version conflict"))
            .build();

    listener.afterBulk(1L, emptyBulkRequest(), List.of(indexRequest), response);

    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testNonRetriableIsTransferFailure() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    Es8BulkListener listener = new Es8BulkListener(null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkResponse response =
        new BulkResponse.Builder()
            .errors(true)
            .took(1L)
            .items(failureItem("idx", "1", 400, "mapper_parsing_exception", "bad mapping"))
            .build();

    listener.afterBulk(1L, emptyBulkRequest(), List.of(indexRequest), response);

    verify(requeue, never()).accept(any());
    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 1);
  }

  @Test
  public void testAfterBulkThrowableDocumentMissingCompletes() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    Es8BulkListener listener = new Es8BulkListener(null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    ElasticsearchException failure =
        new ElasticsearchException(
            "bulk",
            new ErrorResponse.Builder()
                .status(404)
                .error(
                    new ErrorCause.Builder()
                        .type("document_missing_exception")
                        .reason("missing")
                        .build())
                .build());

    listener.afterBulk(1L, bulkRequestWithIndexOp(), List.of(indexRequest), failure);

    verify(requeue, never()).accept(any());
    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testAfterBulkThrowableRequeues() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    MetricUtils metricUtils = mock(MetricUtils.class);
    Es8BulkListener listener = new Es8BulkListener(metricUtils, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());

    listener.afterBulk(
        1L, bulkRequestWithIndexOp(), List.of(indexRequest), new RuntimeException("transport"));

    verify(requeue, times(1)).accept(any(DocWriteRequest.class));
    assertEquals(tracker.getPendingItems(), 1);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testAfterBulkThrowableUnrecoveredTransferFailure() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 0, req -> {});
    Es8BulkListener listener = new Es8BulkListener(null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());

    listener.afterBulk(
        1L, bulkRequestWithIndexOp(), List.of(indexRequest), new RuntimeException("transport"));

    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 1);
  }

  @Test
  public void testAfterBulkThrowableIgnoresNonDocWriteContext() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    Es8BulkListener listener = new Es8BulkListener(null, tracker, support);

    listener.afterBulk(
        1L, bulkRequestWithIndexOp(), List.of("not-a-request"), new RuntimeException("boom"));

    verify(requeue, never()).accept(any());
    assertEquals(tracker.getUnrecoveredTransferFailures(), 1);
  }

  @Test
  public void testBeforeBulkIsNoop() {
    Es8BulkListener listener = new Es8BulkListener(null);
    listener.beforeBulk(1L, emptyBulkRequest(), Collections.emptyList());
  }

  @Test
  public void testBuildBulkRequestSummary() {
    String summary = Es8BulkListener.buildBulkRequestSummary(bulkRequestWithIndexOp());
    assertTrue(summary.contains("_index\":\"idx\"") || summary.contains("idx"));
    assertTrue(summary.contains("optype: [Index]"));
  }

  @Test
  public void testNullObjectsOnThrowableDocumentMissing() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    Es8BulkListener listener = new Es8BulkListener(null, tracker, null);

    ElasticsearchException failure =
        new ElasticsearchException(
            "bulk",
            new ErrorResponse.Builder()
                .status(404)
                .error(
                    new ErrorCause.Builder()
                        .type("document_missing_exception")
                        .reason("missing")
                        .build())
                .build());

    // null contexts must not NPE; completed count is 0 when objects is null
    listener.afterBulk(1L, bulkRequestWithIndexOp(), null, failure);
    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
  }

  private static BulkRequest emptyBulkRequest() {
    return new BulkRequest.Builder().operations(Collections.emptyList()).build();
  }

  private static BulkRequest bulkRequestWithIndexOp() {
    BulkOperation op =
        BulkOperation.of(
            b ->
                b.index(
                    i ->
                        i.index("idx")
                            .id("1")
                            .document(Collections.singletonMap("field", "value"))));
    return new BulkRequest.Builder().operations(Arrays.asList(op)).build();
  }

  private static BulkResponseItem successItem(String index, String id) {
    return new BulkResponseItem.Builder()
        .operationType(OperationType.Index)
        .index(index)
        .id(id)
        .status(200)
        .build();
  }

  private static BulkResponseItem failureItem(
      String index, String id, int status, String type, String reason) {
    return new BulkResponseItem.Builder()
        .operationType(OperationType.Index)
        .index(index)
        .id(id)
        .status(status)
        .error(new ErrorCause.Builder().type(type).reason(reason).build())
        .build();
  }
}
