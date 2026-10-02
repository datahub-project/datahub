package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkTelemetry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.testng.annotations.Test;

public class BulkListenerTest {

  @Test
  public void testConstructor() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkListener test =
        BulkListener.getInstance(0, WriteRequest.RefreshPolicy.IMMEDIATE, metricUtils);
    assertNotNull(test);
    assertEquals(
        test, BulkListener.getInstance(0, WriteRequest.RefreshPolicy.IMMEDIATE, metricUtils));
    assertNotEquals(
        test, BulkListener.getInstance(1, WriteRequest.RefreshPolicy.IMMEDIATE, metricUtils));
  }

  @Test
  public void testDefaultPolicy() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkListener test =
        BulkListener.getInstance(0, WriteRequest.RefreshPolicy.IMMEDIATE, metricUtils);

    BulkRequest mockRequest1 = mock(BulkRequest.class);
    test.beforeBulk(0L, mockRequest1);
    verify(mockRequest1, times(1)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

    BulkRequest mockRequest2 = mock(BulkRequest.class);
    test = BulkListener.getInstance(0, WriteRequest.RefreshPolicy.IMMEDIATE, metricUtils);
    test.beforeBulk(0L, mockRequest2);
    verify(mockRequest2, times(1)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
  }

  private static BulkRequest twoActions() {
    return new BulkRequest()
        .add(new IndexRequest("idx").id("1").source(Map.of("a", 1)))
        .add(new IndexRequest("idx").id("2").source(Map.of("a", 2)));
  }

  @Test
  public void telemetryEndsBatchSpanWithItemFailures() {
    BulkTelemetryTest.Collector collector = new BulkTelemetryTest.Collector();
    BulkTelemetry telemetry =
        BulkTelemetryTest.create(BulkTelemetryTest.tracer(collector), true, true, "gms");
    BulkListener listener =
        BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, null, null, telemetry);

    BulkRequest request = twoActions();
    try (Scope ignored =
        BulkTelemetryTest.remoteSpan("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
            .makeCurrent()) {
      telemetry.onAdd(request.requests().get(0));
    }
    listener.beforeBulk(7L, request);
    assertEquals(request.getRefreshPolicy(), WriteRequest.RefreshPolicy.NONE);
    assertTrue(telemetry.opaqueId(request).orElseThrow().endsWith("|n=2"));

    BulkItemResponse ok =
        new BulkItemResponse(
            0,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId("idx", "uuid", 0), "1", 1L, 1L, 1L, true));
    BulkItemResponse failed =
        new BulkItemResponse(
            1,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("idx", "2", new RuntimeException("mapper_parsing")));
    listener.afterBulk(7L, request, new BulkResponse(new BulkItemResponse[] {ok, failed}, 12L));

    assertEquals(collector.spans.size(), 1);
    SpanData span = collector.spans.get(0);
    assertEquals(span.getName(), "index bulk");
    assertEquals(span.getAttributes().get(BulkTelemetry.ACTIONS), Long.valueOf(2));
    assertEquals(span.getAttributes().get(BulkTelemetry.TOOK_MS), Long.valueOf(12));
    assertEquals(span.getAttributes().get(BulkTelemetry.FAILURES), Long.valueOf(1));
    assertEquals(span.getStatus().getStatusCode(), StatusCode.ERROR);
    assertEquals(span.getLinks().size(), 1);
    assertEquals(telemetry.opaqueId(request).isPresent(), false);
  }

  @Test
  public void telemetryEndsBatchSpanOnTransportFailure() {
    BulkTelemetryTest.Collector collector = new BulkTelemetryTest.Collector();
    BulkTelemetry telemetry =
        BulkTelemetryTest.create(BulkTelemetryTest.tracer(collector), true, false, null);
    BulkListener listener = BulkListener.create(null, null, null, null, telemetry);

    BulkRequest request = twoActions();
    listener.beforeBulk(8L, request);
    listener.afterBulk(8L, request, new RuntimeException("connection reset"));

    assertEquals(collector.spans.size(), 1);
    SpanData span = collector.spans.get(0);
    assertEquals(span.getAttributes().get(BulkTelemetry.FAILURES), Long.valueOf(2));
    assertEquals(span.getStatus().getStatusCode(), StatusCode.ERROR);
    assertEquals(span.getStatus().getDescription(), "connection reset");

    // Success path with no failures leaves the status unset.
    BulkRequest second = twoActions();
    listener.beforeBulk(9L, second);
    BulkItemResponse ok =
        new BulkItemResponse(
            0,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId("idx", "uuid", 0), "1", 1L, 1L, 1L, true));
    listener.afterBulk(9L, second, new BulkResponse(new BulkItemResponse[] {ok}, 3L));
    assertEquals(collector.spans.size(), 2);
    assertEquals(collector.spans.get(1).getStatus().getStatusCode(), StatusCode.UNSET);
    assertEquals(
        collector.spans.get(1).getAttributes().get(BulkTelemetry.FAILURES), Long.valueOf(0));
  }

  @Test
  public void requeuedItemKeepsItsLinkInTheRetryBatch() {
    BulkTelemetryTest.Collector collector = new BulkTelemetryTest.Collector();
    BulkTelemetry telemetry =
        BulkTelemetryTest.create(BulkTelemetryTest.tracer(collector), true, false, null);
    // The shim's requeue path: carry the origin over, then re-add to a processor.
    List<DocWriteRequest<?>> requeued = new ArrayList<>();
    BulkItemRequeueSupport requeueSupport =
        new BulkItemRequeueSupport(
            true,
            3,
            req -> {
              telemetry.onRequeue(req);
              requeued.add(req);
            });
    BulkListener listener = BulkListener.create(null, null, null, requeueSupport, telemetry);

    BulkRequest request = twoActions();
    try (Scope ignored =
        BulkTelemetryTest.remoteSpan("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
            .makeCurrent()) {
      telemetry.onAdd(request.requests().get(1));
    }
    listener.beforeBulk(10L, request);
    BulkItemResponse ok =
        new BulkItemResponse(
            0,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId("idx", "uuid", 0), "1", 1L, 1L, 1L, true));
    BulkItemResponse rejected =
        new BulkItemResponse(
            1,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure(
                "idx", "2", new RuntimeException("rejected"), RestStatus.TOO_MANY_REQUESTS));
    listener.afterBulk(10L, request, new BulkResponse(new BulkItemResponse[] {ok, rejected}, 4L));
    assertEquals(requeued, List.of(request.requests().get(1)));
    assertEquals(telemetry.carriedCount(), 0, "the requeue consumed the carried origin");
    assertEquals(telemetry.pendingCount(), 1, "and it is pending again for the retry batch");

    BulkRequest retry = new BulkRequest().add(requeued.get(0));
    listener.beforeBulk(11L, retry);
    listener.afterBulk(11L, retry, new BulkResponse(new BulkItemResponse[] {ok}, 2L));

    assertEquals(collector.spans.size(), 2);
    SpanData retrySpan = collector.spans.get(1);
    assertEquals(retrySpan.getLinks().size(), 1);
    assertEquals(
        retrySpan.getLinks().get(0).getSpanContext().getTraceId(),
        "0af7651916cd43dd8448eb211c80319c");
  }

  private static BulkItemResponse failure(int index, String id, String message, RestStatus status) {
    return new BulkItemResponse(
        index,
        DocWriteRequest.OpType.INDEX,
        new BulkItemResponse.Failure("idx", id, new RuntimeException(message), status));
  }

  /** Every branch that gives up on a failed item drops its carried origin. */
  @Test
  public void givingUpOnAFailedItemForgetsItsCarriedOrigin() {
    BulkTelemetryTest.Collector collector = new BulkTelemetryTest.Collector();
    BulkTelemetry telemetry =
        BulkTelemetryTest.create(BulkTelemetryTest.tracer(collector), true, false, null);
    BulkItemRequeueSupport requeueSupport =
        new BulkItemRequeueSupport(true, 1, telemetry::onRequeue);
    BulkListener listener = BulkListener.create(null, null, null, requeueSupport, telemetry);
    BulkListener noRequeue = BulkListener.create(null, null, null, null, telemetry);

    // Not retriable (mapper_parsing, 400), version conflict with retries exhausted, and a missing
    // document: each item was added under a span, so each has an origin to carry and then drop.
    BulkRequest request =
        new BulkRequest()
            .add(new IndexRequest("idx").id("1").source(Map.of("a", 1)))
            .add(new IndexRequest("idx").id("2").source(Map.of("a", 2)))
            .add(new IndexRequest("idx").id("3").source(Map.of("a", 3)));
    try (Scope ignored =
        BulkTelemetryTest.remoteSpan("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
            .makeCurrent()) {
      request.requests().forEach(telemetry::onAdd);
    }
    // Exhaust the single allowed requeue attempt for item 2 so the conflict is given up on.
    assertTrue(requeueSupport.tryRequeue(request.requests().get(1)));
    telemetry.onRequeue(request.requests().get(1)); // nothing carried yet: a no-op
    listener.beforeBulk(1L, request);
    listener.afterBulk(
        1L,
        request,
        new BulkResponse(
            new BulkItemResponse[] {
              failure(0, "1", "mapper_parsing_exception", RestStatus.BAD_REQUEST),
              failure(1, "2", "version_conflict_engine_exception", RestStatus.CONFLICT),
              failure(2, "3", "document_missing_exception", RestStatus.NOT_FOUND)
            },
            2L));
    assertEquals(collector.spans.get(0).getAttributes().get(BulkTelemetry.FAILURES), 3L);
    assertEquals(telemetry.carriedCount(), 0, "nothing lingers once each item is given up on");
    assertEquals(telemetry.pendingCount(), 0, "and nothing was requeued");

    // Without requeue support the same failures are given up on immediately.
    BulkRequest second = twoActions();
    try (Scope ignored =
        BulkTelemetryTest.remoteSpan("0af7651916cd43dd8448eb211c80319c", "c7ad6b7169203332")
            .makeCurrent()) {
      second.requests().forEach(telemetry::onAdd);
    }
    noRequeue.beforeBulk(2L, second);
    noRequeue.afterBulk(
        2L,
        second,
        new BulkResponse(
            new BulkItemResponse[] {
              failure(0, "1", "rejected execution", RestStatus.TOO_MANY_REQUESTS),
              failure(1, "2", "mapper_parsing_exception", RestStatus.BAD_REQUEST)
            },
            2L));
    assertEquals(telemetry.carriedCount(), 0);
    assertEquals(telemetry.pendingCount(), 0);

    // More items than actions (defensive): the surplus failures have no action to forget.
    BulkRequest short1 = new BulkRequest().add(new IndexRequest("idx").id("1").source(Map.of()));
    listener.beforeBulk(3L, short1);
    listener.afterBulk(
        3L,
        short1,
        new BulkResponse(
            new BulkItemResponse[] {
              failure(0, "1", "mapper_parsing_exception", RestStatus.BAD_REQUEST),
              failure(1, "2", "mapper_parsing_exception", RestStatus.BAD_REQUEST)
            },
            1L));
    assertEquals(collector.spans.get(2).getAttributes().get(BulkTelemetry.FAILURES), 1L);
    assertEquals(telemetry.carriedCount(), 0);
  }

  @Test
  public void transportFailureForgetsWhatIsNotRequeued() {
    BulkTelemetryTest.Collector collector = new BulkTelemetryTest.Collector();
    BulkTelemetry telemetry =
        BulkTelemetryTest.create(BulkTelemetryTest.tracer(collector), true, false, null);
    // Requeue off: every action of a failed batch is given up on.
    BulkItemRequeueSupport off = new BulkItemRequeueSupport(false, 3, telemetry::onRequeue);
    BulkListener listener = BulkListener.create(null, null, null, off, telemetry);
    BulkRequest request = twoActions();
    try (Scope ignored =
        BulkTelemetryTest.remoteSpan("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
            .makeCurrent()) {
      request.requests().forEach(telemetry::onAdd);
    }
    listener.beforeBulk(1L, request);
    listener.afterBulk(1L, request, new RuntimeException("connection reset"));
    assertEquals(telemetry.carriedCount(), 0);
    assertEquals(telemetry.pendingCount(), 0);

    // A missing-document transport failure completes the actions: nothing to carry either.
    BulkRequest missing = twoActions();
    try (Scope ignored =
        BulkTelemetryTest.remoteSpan("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
            .makeCurrent()) {
      missing.requests().forEach(telemetry::onAdd);
    }
    listener.beforeBulk(2L, missing);
    listener.afterBulk(2L, missing, new RuntimeException("document_missing_exception"));
    assertEquals(telemetry.carriedCount(), 0);
    assertEquals(telemetry.pendingCount(), 0);
    assertEquals(collector.spans.size(), 2);

    // Requeue on: the actions are carried and then re-pended, not forgotten.
    BulkItemRequeueSupport on = new BulkItemRequeueSupport(true, 3, telemetry::onRequeue);
    BulkListener requeuing = BulkListener.create(null, null, null, on, telemetry);
    BulkRequest retried = twoActions();
    try (Scope ignored =
        BulkTelemetryTest.remoteSpan("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
            .makeCurrent()) {
      retried.requests().forEach(telemetry::onAdd);
    }
    requeuing.beforeBulk(3L, retried);
    requeuing.afterBulk(3L, retried, new RuntimeException("connection reset"));
    assertEquals(telemetry.carriedCount(), 0);
    assertEquals(telemetry.pendingCount(), 2);
  }

  @Test
  public void listenerWithoutTelemetryStillWorksOnRealRequests() {
    BulkListener listener = BulkListener.create(null, null, null, null);
    BulkRequest request = twoActions();
    listener.beforeBulk(1L, request);
    listener.afterBulk(1L, request, new RuntimeException("x"));
    BulkListener nullTelemetry = BulkListener.create(null, null, null, null, null);
    nullTelemetry.beforeBulk(2L, request);
    nullTelemetry.afterBulk(2L, request, new BulkResponse(new BulkItemResponse[0], 1L));
    // Disabled telemetry: item failures are still handled, nothing is counted for a span.
    BulkItemResponse failed =
        new BulkItemResponse(
            0,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("idx", "1", new RuntimeException("mapper_parsing")));
    nullTelemetry.beforeBulk(3L, request);
    nullTelemetry.afterBulk(3L, request, new BulkResponse(new BulkItemResponse[] {failed}, 1L));
    nullTelemetry.beforeBulk(4L, request);
    nullTelemetry.afterBulk(4L, request, new RuntimeException("document_missing_exception"));
  }
}
