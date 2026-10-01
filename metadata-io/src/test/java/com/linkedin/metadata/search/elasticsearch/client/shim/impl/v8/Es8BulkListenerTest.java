package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import com.linkedin.metadata.elasticsearch.update.BulkTelemetryTest;
import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkTelemetry;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.testng.annotations.Test;

/** Bulk-write attribution on the ES8 ingester listener; the item bookkeeping is unchanged. */
public class Es8BulkListenerTest {

  private static BulkRequest request() {
    return BulkRequest.of(
        b ->
            b.operations(
                BulkOperation.of(o -> o.index(i -> i.index("idx").id("1").document(Map.of()))),
                BulkOperation.of(o -> o.index(i -> i.index("idx").id("2").document(Map.of())))));
  }

  private static BulkResponseItem item(String id, int status, String errorType) {
    return BulkResponseItem.of(
        i -> {
          i.index("idx").id(id).status(status).operationType(OperationType.Index);
          if (errorType != null) {
            i.error(ErrorCause.of(e -> e.type(errorType).reason("bad")));
          }
          return i;
        });
  }

  private static final String TRACE = "0af7651916cd43dd8448eb211c80319c";

  @Test
  public void batchSpanLinksActionsAndRecordsItemFailures() {
    BulkTelemetryTest.Collector collector = new BulkTelemetryTest.Collector();
    BulkTelemetry telemetry =
        BulkTelemetryTest.create(BulkTelemetryTest.tracer(collector), true, false, null);
    Es8BulkListener listener = new Es8BulkListener(null, null, null, telemetry);

    IndexRequest first = new IndexRequest("idx").id("1").source(Map.of("a", 1));
    IndexRequest second = new IndexRequest("idx").id("2").source(Map.of("a", 2));
    try (Scope ignored = BulkTelemetryTest.remoteSpan(TRACE, "b7ad6b7169203331").makeCurrent()) {
      telemetry.onAdd(first);
    }
    List<Object> contexts = new ArrayList<>(List.of(first, second, "not a write request"));
    BulkRequest request = request();
    listener.beforeBulk(1L, request, contexts);
    listener.afterBulk(
        1L,
        request,
        contexts,
        BulkResponse.of(
            b ->
                b.errors(true)
                    .took(9L)
                    .items(item("1", 201, null), item("2", 400, "mapper_parsing_exception"))));

    assertEquals(collector.spans.size(), 1);
    SpanData span = collector.spans.get(0);
    assertEquals(span.getName(), "index bulk");
    String batchId = span.getAttributes().get(BulkTelemetry.BATCH_ID);
    assertNotNull(batchId);
    assertTrue(batchId.matches("^[0-9a-f]{8}-\\d+$"), batchId);
    assertEquals(span.getAttributes().get(BulkTelemetry.ACTIONS), Long.valueOf(2));
    assertEquals(span.getAttributes().get(BulkTelemetry.INDICES), List.of("idx"));
    assertEquals(span.getAttributes().get(BulkTelemetry.TOOK_MS), Long.valueOf(9));
    assertEquals(span.getAttributes().get(BulkTelemetry.FAILURES), Long.valueOf(1));
    assertEquals(span.getStatus().getStatusCode(), StatusCode.ERROR);
    assertEquals(span.getLinks().size(), 1);
    assertEquals(span.getLinks().get(0).getSpanContext().getTraceId(), TRACE);
    assertEquals(span.getLinks().get(0).getSpanContext().getSpanId(), "b7ad6b7169203331");
    assertFalse(span.getParentSpanContext().isValid());
  }

  @Test
  public void requeuedItemKeepsItsLinkInTheRetryBatch() {
    BulkTelemetryTest.Collector collector = new BulkTelemetryTest.Collector();
    BulkTelemetry telemetry =
        BulkTelemetryTest.create(BulkTelemetryTest.tracer(collector), true, false, null);
    List<DocWriteRequest<?>> requeued = new ArrayList<>();
    BulkItemRequeueSupport requeueSupport =
        new BulkItemRequeueSupport(
            true,
            3,
            req -> {
              telemetry.onRequeue(req);
              requeued.add(req);
            });
    Es8BulkListener listener = new Es8BulkListener(null, null, requeueSupport, telemetry);

    IndexRequest first = new IndexRequest("idx").id("1").source(Map.of("a", 1));
    try (Scope ignored = BulkTelemetryTest.remoteSpan(TRACE, "b7ad6b7169203331").makeCurrent()) {
      telemetry.onAdd(first);
    }
    List<Object> contexts = List.of(first);
    BulkRequest request = request();
    listener.beforeBulk(5L, request, contexts);
    listener.afterBulk(
        5L,
        request,
        contexts,
        BulkResponse.of(
            b -> b.errors(true).took(1L).items(item("1", 429, "es_rejected_execution_exception"))));
    assertEquals(requeued, List.of(first));

    BulkRequest retry = request();
    listener.beforeBulk(6L, retry, contexts);
    listener.afterBulk(
        6L,
        retry,
        contexts,
        BulkResponse.of(b -> b.errors(false).took(1L).items(item("1", 201, null))));

    assertEquals(collector.spans.size(), 2);
    assertEquals(collector.spans.get(1).getLinks().size(), 1);
    assertEquals(collector.spans.get(1).getLinks().get(0).getSpanContext().getTraceId(), TRACE);
  }

  @Test
  public void transportFailureAndSuccessPaths() {
    BulkTelemetryTest.Collector collector = new BulkTelemetryTest.Collector();
    BulkTelemetry telemetry =
        BulkTelemetryTest.create(BulkTelemetryTest.tracer(collector), true, false, null);
    Es8BulkListener listener = new Es8BulkListener(null, null, null, telemetry);

    BulkRequest failing = request();
    listener.beforeBulk(2L, failing, null);
    listener.afterBulk(2L, failing, null, new RuntimeException("connection reset"));

    BulkRequest fine = request();
    List<Object> contexts = List.of(new IndexRequest("idx").id("1").source(Map.of("a", 1)));
    listener.beforeBulk(3L, fine, contexts);
    listener.afterBulk(
        3L,
        fine,
        contexts,
        BulkResponse.of(b -> b.errors(false).took(2L).items(item("1", 201, null))));

    assertEquals(collector.spans.size(), 2);
    assertEquals(collector.spans.get(0).getStatus().getStatusCode(), StatusCode.ERROR);
    assertEquals(
        collector.spans.get(0).getAttributes().get(BulkTelemetry.ACTIONS), Long.valueOf(0));
    assertEquals(collector.spans.get(1).getStatus().getStatusCode(), StatusCode.UNSET);
    assertEquals(
        collector.spans.get(1).getAttributes().get(BulkTelemetry.FAILURES), Long.valueOf(0));
  }

  @Test
  public void listenersWithoutTelemetryEmitNothing() {
    Es8BulkListener plain = new Es8BulkListener(null);
    Es8BulkListener threeArg = new Es8BulkListener(null, null, null);
    Es8BulkListener nullTelemetry = new Es8BulkListener(null, null, null, null);
    BulkRequest request = request();
    for (Es8BulkListener l : List.of(plain, threeArg, nullTelemetry)) {
      l.beforeBulk(4L, request, null);
      l.afterBulk(
          4L, request, null, BulkResponse.of(b -> b.errors(false).took(1L).items(List.of())));
      // Disabled: item failures are handled without counting them for a span.
      List<Object> contexts = List.of(new IndexRequest("idx").id("1").source(Map.of("a", 1)));
      l.beforeBulk(5L, request, contexts);
      l.afterBulk(
          5L,
          request,
          contexts,
          BulkResponse.of(
              b -> b.errors(true).took(1L).items(item("1", 400, "mapper_parsing_exception"))));
      l.afterBulk(6L, request, contexts, new RuntimeException("reset"));
    }
  }
}
