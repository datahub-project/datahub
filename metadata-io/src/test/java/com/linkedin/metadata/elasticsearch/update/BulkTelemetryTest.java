package com.linkedin.metadata.elasticsearch.update;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.update.BulkTelemetry;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.BulkTelemetryConfig;
import io.datahubproject.metadata.context.RequestStats;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.testng.annotations.Test;

public class BulkTelemetryTest {

  /** Collects ended spans without the sdk-testing artifact. */
  public static final class Collector implements SpanProcessor {
    public final List<SpanData> spans = new CopyOnWriteArrayList<>();

    @Override
    public void onStart(Context parentContext, ReadWriteSpan span) {}

    @Override
    public boolean isStartRequired() {
      return false;
    }

    @Override
    public void onEnd(ReadableSpan span) {
      spans.add(span.toSpanData());
    }

    @Override
    public boolean isEndRequired() {
      return true;
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode forceFlush() {
      return CompletableResultCode.ofSuccess();
    }
  }

  public static Tracer tracer(Collector collector) {
    return SdkTracerProvider.builder().addSpanProcessor(collector).build().get("test");
  }

  public static Span remoteSpan(String traceId, String spanId) {
    return Span.wrap(
        SpanContext.createFromRemoteParent(
            traceId, spanId, TraceFlags.getSampled(), TraceState.getDefault()));
  }

  /** {@link BulkTelemetry#create} from the four settings; shared by the listener and shim tests. */
  public static BulkTelemetry create(
      Tracer tracer, boolean batchSpans, boolean opaqueId, String service) {
    return BulkTelemetry.create(BulkTelemetryConfig.of(tracer, batchSpans, opaqueId, service));
  }

  public static String header(RequestOptions options) {
    return options.getHeaders().stream()
        .filter(h -> h.getName().equals(ESUtils.OPAQUE_ID_HEADER))
        .map(h -> h.getValue())
        .findFirst()
        .orElse(null);
  }

  private static final String TRACE_A = "0af7651916cd43dd8448eb211c80319c";
  private static final String TRACE_B = "1bf7651916cd43dd8448eb211c80319d";

  @Test
  public void disabledInstanceIsSharedAndDoesNothing() {
    BulkTelemetry off = BulkTelemetry.disabled();
    assertSame(create(null, true, false, "svc"), off);
    assertSame(create(tracer(new Collector()), false, false, "svc"), off);
    assertSame(BulkTelemetry.create(BulkTelemetryConfig.DISABLED), off);
    assertFalse(off.isEnabled());
    assertFalse(BulkTelemetryConfig.DISABLED.isEnabled());
    assertFalse(BulkTelemetryConfig.of(null, true, false, null).spansEnabled());
    assertTrue(BulkTelemetryConfig.of(tracer(new Collector()), true, false, null).spansEnabled());
    assertTrue(BulkTelemetryConfig.of(null, false, true, null).isEnabled());

    BulkRequest request = new BulkRequest().add(new IndexRequest("idx").id("1").source("{}", 0));
    try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
      off.onAdd(request.requests().get(0));
    }
    off.beforeBulk(request, request.requests());
    assertEquals(off.opaqueId(request).isPresent(), false);
    assertSame(off.requestOptions(request, RequestOptions.DEFAULT), RequestOptions.DEFAULT);
    assertSame(off.makeCurrent(request), Scope.noop());
    off.afterBulk(request, 1L, 0L);
    off.afterBulk(request, new RuntimeException("x"));
    off.onRequeue(request.requests().get(0));
    assertEquals(off.openBatches(), 0);
    assertEquals(off.pendingCount(), 0);
  }

  @Test
  public void opaqueIdNamesServiceBatchAndActionCount() {
    BulkTelemetry t = create(null, false, true, " mae-consumer ");
    assertTrue(t.isEnabled());
    BulkRequest request =
        new BulkRequest()
            .add(new IndexRequest("idx").id("1").source("{}", 0))
            .add(new IndexRequest("idx").id("2").source("{}", 0));
    assertEquals(t.opaqueId(request).isPresent(), false, "unknown batch has no id");

    t.beforeBulk(request, request.requests());
    String id = t.opaqueId(request).orElseThrow();
    assertTrue(id.matches("bulk\\|mae-consumer\\|batch=[0-9a-f]{8}-1\\|n=2"), id);
    assertEquals(header(t.requestOptions(request, RequestOptions.DEFAULT)), id);

    BulkRequest second = new BulkRequest().add(new IndexRequest("idx").id("3").source("{}", 0));
    t.beforeBulk(second, second.requests());
    String id2 = t.opaqueId(second).orElseThrow();
    assertTrue(id2.endsWith("-2|n=1"), id2);
    assertEquals(id.substring(0, id.indexOf("-")), id2.substring(0, id2.indexOf("-")));
    assertEquals(t.openBatches(), 2);

    // Header only: the scope strips the request thread's context but there is no span.
    try (Scope ignored = t.makeCurrent(request)) {
      assertFalse(Span.current().getSpanContext().isValid());
    }
    t.afterBulk(request, 5L, 0L);
    t.afterBulk(second, new RuntimeException("boom"));
    assertEquals(t.openBatches(), 0);
    assertEquals(t.opaqueId(request).isPresent(), false, "ended batches are forgotten");
  }

  @Test
  public void blankServiceFallsBackToDefault() {
    BulkTelemetry t = create(null, false, true, "  ");
    BulkRequest request = new BulkRequest().add(new IndexRequest("idx").id("1").source("{}", 0));
    t.beforeBulk(request, request.requests());
    assertTrue(t.opaqueId(request).orElseThrow().startsWith("bulk|datahub|batch="));
    BulkTelemetry u = create(null, false, true, null);
    u.beforeBulk(request, request.requests());
    assertTrue(u.opaqueId(request).orElseThrow().startsWith("bulk|datahub|batch="));
  }

  @Test
  public void batchSpanCarriesAttributesLinksAndTook() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, true, "gms");

    IndexRequest a = new IndexRequest("datasetindex_v2").id("a").source("{}", 0);
    IndexRequest b = new IndexRequest("datasetindex_v2").id("b").source("{}", 0);
    IndexRequest c = new IndexRequest("graph_service_v1").id("c").source("{}", 0);
    IndexRequest noIndex = new IndexRequest().id("d").source("{}", 0);
    IndexRequest noTrace = new IndexRequest("chartindex_v2").id("e").source("{}", 0);

    try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
      t.onAdd(a);
      t.onAdd(b); // same origin twice: one link
    }
    try (Scope ignored = remoteSpan(TRACE_B, "c7ad6b7169203332").makeCurrent()) {
      t.onAdd(c);
      t.onAdd(noIndex);
    }
    t.onAdd(noTrace); // no span current: nothing remembered
    t.onAdd(null);
    assertEquals(t.pendingCount(), 4);

    BulkRequest request = new BulkRequest().add(a).add(b).add(c).add(noIndex).add(noTrace);
    // Even on a thread with an unrelated span current the batch span is a root.
    try (Scope ignored = remoteSpan(TRACE_B, "dead6b7169203333").makeCurrent()) {
      t.beforeBulk(request, request.requests());
    }
    assertEquals(t.pendingCount(), 0, "consumed by the batch");
    assertEquals(collector.spans.size(), 0, "not ended yet");

    try (Scope ignored = t.makeCurrent(request)) {
      assertTrue(Span.current().getSpanContext().isValid());
      assertEquals(RequestStats.current().isPresent(), false);
    }
    t.afterBulk(request, 42L, 0L);

    assertEquals(collector.spans.size(), 1);
    SpanData span = collector.spans.get(0);
    assertEquals(span.getName(), BulkTelemetry.SPAN_NAME);
    assertEquals(span.getKind(), SpanKind.INTERNAL);
    assertFalse(span.getParentSpanContext().isValid(), "root span");
    String batchId = span.getAttributes().get(BulkTelemetry.BATCH_ID);
    assertNotNull(batchId);
    assertEquals(
        t.opaqueId(request).isPresent(), false, "ended; but header earlier matched the span id");
    assertEquals(span.getAttributes().get(BulkTelemetry.ACTIONS), Long.valueOf(5));
    assertEquals(
        span.getAttributes().get(BulkTelemetry.INDICES),
        List.of("chartindex_v2", "datasetindex_v2", "graph_service_v1"));
    assertEquals(span.getAttributes().get(BulkTelemetry.TOOK_MS), Long.valueOf(42));
    assertEquals(span.getAttributes().get(BulkTelemetry.FAILURES), Long.valueOf(0));
    assertEquals(span.getStatus().getStatusCode(), StatusCode.UNSET);
    assertEquals(span.getLinks().size(), 2);
    assertEquals(span.getLinks().get(0).getSpanContext().getTraceId(), TRACE_A);
    assertEquals(span.getLinks().get(1).getSpanContext().getTraceId(), TRACE_B);
    assertEquals(span.getTotalRecordedLinks(), 2);
  }

  @Test
  public void opaqueIdMatchesSpanBatchId() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, true, "gms");
    BulkRequest request = new BulkRequest().add(new IndexRequest("idx").id("1").source("{}", 0));
    t.beforeBulk(request, request.requests());
    String id = t.opaqueId(request).orElseThrow();
    t.afterBulk(request, 1L, 0L);
    String batchId = collector.spans.get(0).getAttributes().get(BulkTelemetry.BATCH_ID);
    assertEquals(id, "bulk|gms|batch=" + batchId + "|n=1");
  }

  @Test
  public void itemFailuresAndTransportFailuresSetErrorStatus() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);

    BulkRequest partial = new BulkRequest().add(new IndexRequest("idx").id("1").source("{}", 0));
    t.beforeBulk(partial, partial.requests());
    assertEquals(t.opaqueId(partial).isPresent(), false, "header off");
    assertSame(t.requestOptions(partial, RequestOptions.DEFAULT), RequestOptions.DEFAULT);
    t.afterBulk(partial, -1L, 3L);

    BulkRequest failed =
        new BulkRequest()
            .add(new IndexRequest("idx").id("2").source("{}", 0))
            .add(new IndexRequest("idx").id("3").source("{}", 0));
    t.beforeBulk(failed, failed.requests());
    t.afterBulk(failed, new IllegalStateException("connection reset"));

    assertEquals(collector.spans.size(), 2);
    SpanData p = collector.spans.get(0);
    assertEquals(p.getAttributes().get(BulkTelemetry.TOOK_MS), Long.valueOf(0), "clamped");
    assertEquals(p.getAttributes().get(BulkTelemetry.FAILURES), Long.valueOf(3));
    assertEquals(p.getStatus().getStatusCode(), StatusCode.ERROR);
    assertEquals(p.getStatus().getDescription(), "3 item(s) failed");

    SpanData f = collector.spans.get(1);
    assertNull(f.getAttributes().get(BulkTelemetry.TOOK_MS));
    assertEquals(f.getAttributes().get(BulkTelemetry.FAILURES), Long.valueOf(2));
    assertEquals(f.getStatus().getStatusCode(), StatusCode.ERROR);
    assertEquals(f.getStatus().getDescription(), "connection reset");
    assertEquals(f.getEvents().size(), 1);
    assertEquals(f.getEvents().get(0).getName(), "exception");
  }

  @Test
  public void nullAndUnknownKeysAreIgnored() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, true, "gms");
    t.beforeBulk(null, List.of());
    t.beforeBulk(new BulkRequest(), null); // mocks return null request lists
    assertEquals(t.openBatches(), 1);
    assertEquals(t.opaqueId(null).isPresent(), false);
    assertSame(t.makeCurrent(null), Scope.noop());
    assertSame(t.makeCurrent(new Object()), Scope.noop());
    t.afterBulk(null, 1L, 0L);
    t.afterBulk(new Object(), 1L, 0L);
    t.afterBulk(null, new RuntimeException());
    t.afterBulk(new Object(), new RuntimeException());
    assertEquals(collector.spans.size(), 0);
  }

  @Test
  public void linksAndIndicesAreCapped() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);
    List<DocWriteRequest<?>> actions = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      IndexRequest r = new IndexRequest("index_" + String.format("%03d", i)).id("" + i);
      try (Scope ignored =
          remoteSpan(String.format("%032x", i + 1), String.format("%016x", i + 1)).makeCurrent()) {
        t.onAdd(r);
      }
      actions.add(r);
    }
    Object key = new Object();
    t.beforeBulk(key, actions);
    t.afterBulk(key, 7L, 0L);
    SpanData span = collector.spans.get(0);
    assertEquals(span.getLinks().size(), 64);
    assertEquals(span.getAttributes().get(BulkTelemetry.INDICES).size(), 32);
    assertEquals(span.getAttributes().get(BulkTelemetry.ACTIONS), Long.valueOf(100));
  }

  @Test
  public void linksAreOnePerTraceNotPerSpan() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);
    IndexRequest a = new IndexRequest("idx").id("a").source("{}", 0);
    IndexRequest b = new IndexRequest("idx").id("b").source("{}", 0);
    IndexRequest c = new IndexRequest("idx").id("c").source("{}", 0);
    // Two spans of the same trace (a request and one of its children) and one of another trace.
    try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
      t.onAdd(a);
    }
    try (Scope ignored = remoteSpan(TRACE_A, "c7ad6b7169203332").makeCurrent()) {
      t.onAdd(b);
    }
    try (Scope ignored = remoteSpan(TRACE_B, "d7ad6b7169203333").makeCurrent()) {
      t.onAdd(c);
    }
    BulkRequest request = new BulkRequest().add(a).add(b).add(c);
    t.beforeBulk(request, request.requests());
    t.afterBulk(request, 1L, 0L);
    SpanData span = collector.spans.get(0);
    assertEquals(span.getLinks().size(), 2, "one link per distinct trace");
    assertEquals(span.getLinks().get(0).getSpanContext().getTraceId(), TRACE_A);
    assertEquals(
        span.getLinks().get(0).getSpanContext().getSpanId(),
        "b7ad6b7169203331",
        "the first span seen for the trace");
    assertEquals(span.getLinks().get(1).getSpanContext().getTraceId(), TRACE_B);
  }

  @Test
  public void linksCapCountsDistinctTraces() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);
    List<DocWriteRequest<?>> actions = new ArrayList<>();
    // 70 distinct traces, two spans each: 140 actions, 64 links.
    for (int i = 0; i < 70; i++) {
      for (int j = 0; j < 2; j++) {
        IndexRequest r = new IndexRequest("idx").id(i + "-" + j);
        try (Scope ignored =
            remoteSpan(String.format("%032x", i + 1), String.format("%016x", i * 2 + j + 1))
                .makeCurrent()) {
          t.onAdd(r);
        }
        actions.add(r);
      }
    }
    // The cap applies to new traces only: a late span of an already-linked trace still dedupes.
    IndexRequest late = new IndexRequest("idx").id("late");
    try (Scope ignored = remoteSpan(String.format("%032x", 1), "ffffffffffffffff").makeCurrent()) {
      t.onAdd(late);
    }
    actions.add(late);
    Object key = new Object();
    t.beforeBulk(key, actions);
    t.afterBulk(key, 1L, 0L);
    SpanData span = collector.spans.get(0);
    assertEquals(span.getLinks().size(), 64);
    assertEquals(span.getLinks().get(0).getSpanContext().getSpanId(), String.format("%016x", 1));
    assertEquals(span.getAttributes().get(BulkTelemetry.ACTIONS), Long.valueOf(141));
  }

  @Test
  public void requeuedActionKeepsItsLinkInTheRetryBatch() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);
    IndexRequest a = new IndexRequest("idx").id("a").source("{}", 0);
    IndexRequest unlinked = new IndexRequest("idx").id("u").source("{}", 0);
    try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
      t.onAdd(a);
    }
    t.onAdd(unlinked);

    // First batch fails the item; the listener requeues it from inside afterBulk.
    BulkRequest first = new BulkRequest().add(a).add(unlinked);
    t.beforeBulk(first, first.requests());
    t.afterBulk(first, 3L, 1L);
    assertEquals(t.pendingCount(), 0);
    t.onRequeue(a);
    t.onRequeue(unlinked); // never had an origin: nothing to carry
    t.onRequeue(null);
    t.onRequeue(new IndexRequest("idx").id("unknown"));
    assertEquals(t.pendingCount(), 1, "only the linked action is remembered again");

    BulkRequest retry = new BulkRequest().add(a).add(unlinked);
    t.beforeBulk(retry, retry.requests());
    t.afterBulk(retry, 2L, 0L);

    assertEquals(collector.spans.size(), 2);
    assertEquals(collector.spans.get(0).getLinks().size(), 1);
    assertEquals(collector.spans.get(1).getLinks().size(), 1, "retry batch keeps the link");
    assertEquals(
        collector.spans.get(1).getLinks().get(0).getSpanContext().getTraceId(),
        collector.spans.get(0).getLinks().get(0).getSpanContext().getTraceId());
    assertNotEquals(
        collector.spans.get(1).getAttributes().get(BulkTelemetry.BATCH_ID),
        collector.spans.get(0).getAttributes().get(BulkTelemetry.BATCH_ID));
  }

  @Test
  public void requeueAfterTransportFailureAlsoKeepsTheLink() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);
    IndexRequest a = new IndexRequest("idx").id("a").source("{}", 0);
    try (Scope ignored = remoteSpan(TRACE_B, "b7ad6b7169203331").makeCurrent()) {
      t.onAdd(a);
    }
    BulkRequest first = new BulkRequest().add(a);
    t.beforeBulk(first, first.requests());
    t.afterBulk(first, new RuntimeException("reset"));
    t.onRequeue(a);
    BulkRequest retry = new BulkRequest().add(a);
    t.beforeBulk(retry, retry.requests());
    t.afterBulk(retry, 1L, 0L);
    assertEquals(collector.spans.get(1).getLinks().get(0).getSpanContext().getTraceId(), TRACE_B);
  }

  @Test
  public void recentBatchesAreBounded() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);
    IndexRequest old = new IndexRequest("idx").id("old").source("{}", 0);
    try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
      t.onAdd(old);
    }
    BulkRequest first = new BulkRequest().add(old);
    t.beforeBulk(first, first.requests());
    t.afterBulk(first, 1L, 1L);
    // RECENT_BATCHES more linked batches push the first one out; unlinked batches are not kept.
    for (int i = 0; i < BulkTelemetry.RECENT_BATCHES; i++) {
      IndexRequest r = new IndexRequest("idx").id("" + i).source("{}", 0);
      try (Scope ignored = remoteSpan(TRACE_B, String.format("%016x", i + 1)).makeCurrent()) {
        t.onAdd(r);
      }
      BulkRequest b = new BulkRequest().add(r);
      t.beforeBulk(b, b.requests());
      t.afterBulk(b, 1L, 0L);
      BulkRequest unlinked = new BulkRequest().add(new IndexRequest("idx").id("x" + i));
      t.beforeBulk(unlinked, unlinked.requests());
      t.afterBulk(unlinked, 1L, 0L);
    }
    t.onRequeue(old);
    assertEquals(t.pendingCount(), 0, "evicted: origin forgotten");
    // Header-only batches never enter the recent list either.
    BulkTelemetry headerOnly = create(null, false, true, "svc");
    BulkRequest h = new BulkRequest().add(old);
    headerOnly.beforeBulk(h, h.requests());
    headerOnly.afterBulk(h, 1L, 0L);
    headerOnly.onRequeue(old);
    assertEquals(headerOnly.pendingCount(), 0);
  }

  @Test
  public void pendingOriginsAreBounded() {
    BulkTelemetry t = create(tracer(new Collector()), true, false, null);
    try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
      for (int i = 0; i < 20_010; i++) {
        t.onAdd(new IndexRequest("idx").id("" + i));
      }
    }
    assertEquals(t.pendingCount(), 20_000);
    assertNotEquals(t.pendingCount(), 20_010);
  }
}
