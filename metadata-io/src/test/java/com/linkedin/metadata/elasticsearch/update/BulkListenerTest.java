package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkTelemetry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Map;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.core.index.shard.ShardId;
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
        BulkTelemetry.create(BulkTelemetryTest.tracer(collector), true, true, "gms");
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
        BulkTelemetry.create(BulkTelemetryTest.tracer(collector), true, false, null);
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
  public void listenerWithoutTelemetryStillWorksOnRealRequests() {
    BulkListener listener = BulkListener.create(null, null, null, null);
    BulkRequest request = twoActions();
    listener.beforeBulk(1L, request);
    listener.afterBulk(1L, request, new RuntimeException("x"));
    BulkListener nullTelemetry = BulkListener.create(null, null, null, null, null);
    nullTelemetry.beforeBulk(2L, request);
    nullTelemetry.afterBulk(2L, request, new BulkResponse(new BulkItemResponse[0], 1L));
  }
}
