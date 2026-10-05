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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
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
    off.afterBulk(request, 1L, List.of());
    off.afterBulk(request, new RuntimeException("x"));
    off.onRequeue(request.requests().get(0));
    off.forget(request.requests().get(0));
    assertEquals(off.openBatches(), 0);
    assertEquals(off.pendingCount(), 0);
    assertEquals(off.carriedCount(), 0);
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
    t.afterBulk(request, 5L, List.of());
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
    t.afterBulk(request, 42L, List.of());

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
    t.afterBulk(request, 1L, List.of());
    String batchId = collector.spans.get(0).getAttributes().get(BulkTelemetry.BATCH_ID);
    assertEquals(id, "bulk|gms|batch=" + batchId + "|n=1");
  }

  @Test
  public void itemFailuresAndTransportFailuresSetErrorStatus() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);

    BulkRequest partial =
        new BulkRequest()
            .add(new IndexRequest("idx").id("1").source("{}", 0))
            .add(new IndexRequest("idx").id("1b").source("{}", 0))
            .add(new IndexRequest("idx").id("1c").source("{}", 0));
    t.beforeBulk(partial, partial.requests());
    assertEquals(t.opaqueId(partial).isPresent(), false, "header off");
    assertSame(t.requestOptions(partial, RequestOptions.DEFAULT), RequestOptions.DEFAULT);
    t.afterBulk(partial, -1L, partial.requests());

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
    assertEquals(t.carriedCount(), 0, "failed actions without an origin carry nothing");

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
    BulkRequest nullList = new BulkRequest();
    t.beforeBulk(nullList, null); // mocks return null request lists
    assertEquals(t.openBatches(), 1);
    assertEquals(t.opaqueId(null).isPresent(), false);
    assertSame(t.makeCurrent(null), Scope.noop());
    assertSame(t.makeCurrent(new Object()), Scope.noop());
    t.afterBulk(null, 1L, List.of());
    t.afterBulk(new Object(), 1L, List.of());
    t.afterBulk(new Object(), 1L, null); // listeners with a null failure list
    Object known = new Object();
    t.beforeBulk(known, List.of());
    t.afterBulk(known, 1L, null); // a null failure list on a known batch counts as no failures
    assertEquals(collector.spans.size(), 1);
    assertEquals(collector.spans.get(0).getAttributes().get(BulkTelemetry.FAILURES), 0L);
    collector.spans.clear();
    t.forget(null);
    t.forget(new Object());
    t.onRequeue(new Object());
    t.afterBulk(null, new RuntimeException());
    t.afterBulk(new Object(), new RuntimeException());
    assertEquals(collector.spans.size(), 0);
    Reference.reachabilityFence(nullList); // its batch stays open, not abandoned, for the test
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
    t.afterBulk(key, 7L, List.of());
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
    t.afterBulk(request, 1L, List.of());
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
    t.afterBulk(key, 1L, List.of());
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
    t.afterBulk(first, 3L, first.requests());
    assertEquals(t.pendingCount(), 0);
    assertEquals(t.carriedCount(), 1, "only the linked failed action has an origin to carry");
    t.onRequeue(a);
    t.onRequeue(unlinked); // never had an origin: nothing to carry
    t.onRequeue(null);
    t.onRequeue(new IndexRequest("idx").id("unknown"));
    assertEquals(t.pendingCount(), 1, "only the linked action is remembered again");
    assertEquals(t.carriedCount(), 0, "consumed by the requeue");

    BulkRequest retry = new BulkRequest().add(a).add(unlinked);
    t.beforeBulk(retry, retry.requests());
    t.afterBulk(retry, 2L, List.of());

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
    assertEquals(t.carriedCount(), 1, "a transport failure fails, and carries, every action");
    t.onRequeue(a);
    assertEquals(t.carriedCount(), 0);
    BulkRequest retry = new BulkRequest().add(a);
    t.beforeBulk(retry, retry.requests());
    t.afterBulk(retry, 1L, List.of());
    assertEquals(collector.spans.get(1).getLinks().get(0).getSpanContext().getTraceId(), TRACE_B);
  }

  @Test
  public void carriedOriginSurvivesAnyNumberOfOtherCompletions() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);
    IndexRequest old = new IndexRequest("idx").id("old").source("{}", 0);
    try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
      t.onAdd(old);
    }
    BulkRequest first = new BulkRequest().add(old);
    t.beforeBulk(first, first.requests());
    t.afterBulk(first, 1L, first.requests());
    assertEquals(t.carriedCount(), 1);
    // With several processors flushing concurrently, many other batches (linked, failed or not) can
    // end between this one's end and its listener reaching the failed item. None of them touch it.
    for (int i = 0; i < 40; i++) {
      IndexRequest r = new IndexRequest("idx").id("" + i).source("{}", 0);
      try (Scope ignored = remoteSpan(TRACE_B, String.format("%016x", i + 1)).makeCurrent()) {
        t.onAdd(r);
      }
      BulkRequest b = new BulkRequest().add(r);
      t.beforeBulk(b, b.requests());
      t.afterBulk(b, 1L, i % 2 == 0 ? List.of() : b.requests());
      if (i % 2 == 1) {
        t.forget(r);
      }
      BulkRequest unlinked = new BulkRequest().add(new IndexRequest("idx").id("x" + i));
      t.beforeBulk(unlinked, unlinked.requests());
      t.afterBulk(unlinked, 1L, List.of());
    }
    assertEquals(t.carriedCount(), 1, "only the old failed action is still carried");
    t.onRequeue(old);
    assertEquals(t.pendingCount(), 1, "the origin survived");
    assertEquals(t.carriedCount(), 0);
    BulkRequest retry = new BulkRequest().add(old);
    t.beforeBulk(retry, retry.requests());
    t.afterBulk(retry, 1L, List.of());
    SpanData retrySpan = collector.spans.get(collector.spans.size() - 1);
    assertEquals(retrySpan.getLinks().size(), 1, "retry batch still links to the origin");
    assertEquals(retrySpan.getLinks().get(0).getSpanContext().getTraceId(), TRACE_A);
    assertEquals(retrySpan.getLinks().get(0).getSpanContext().getSpanId(), "b7ad6b7169203331");
  }

  @Test
  public void carriedIsEmptyAfterRequeueOrForgetAndSuccessesCarryNothing() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);
    IndexRequest requeued = new IndexRequest("idx").id("r").source("{}", 0);
    IndexRequest givenUp = new IndexRequest("idx").id("g").source("{}", 0);
    IndexRequest fine = new IndexRequest("idx").id("f").source("{}", 0);
    try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
      t.onAdd(requeued);
      t.onAdd(givenUp);
      t.onAdd(fine);
    }
    BulkRequest batch = new BulkRequest().add(requeued).add(givenUp).add(fine);
    t.beforeBulk(batch, batch.requests());
    t.afterBulk(batch, 1L, List.of(requeued, givenUp));
    assertEquals(t.carriedCount(), 2, "the successful action's origin went with the batch");
    assertEquals(t.openBatches(), 0, "the batch itself is dropped at afterBulk");
    t.forget(givenUp);
    assertEquals(t.carriedCount(), 1);
    t.forget(givenUp); // idempotent
    t.forget(fine); // never carried
    assertEquals(t.carriedCount(), 1);
    t.onRequeue(requeued);
    assertEquals(t.carriedCount(), 0);
    assertEquals(t.pendingCount(), 1);
    t.onRequeue(requeued); // a second requeue of the same action finds nothing more to carry
    assertEquals(t.pendingCount(), 1);
    t.onRequeue(fine); // not failed: nothing carried, nothing re-pended
    assertEquals(t.pendingCount(), 1);

    // Header-only mode holds no origins at all, so nothing is ever carried.
    BulkTelemetry headerOnly = create(null, false, true, "svc");
    BulkRequest h = new BulkRequest().add(givenUp);
    headerOnly.beforeBulk(h, h.requests());
    headerOnly.afterBulk(h, 1L, h.requests());
    headerOnly.onRequeue(givenUp);
    headerOnly.forget(givenUp);
    assertEquals(headerOnly.pendingCount(), 0);
    assertEquals(headerOnly.carriedCount(), 0);
    BulkRequest h2 = new BulkRequest().add(givenUp);
    headerOnly.beforeBulk(h2, h2.requests());
    headerOnly.afterBulk(h2, new RuntimeException("reset"));
    assertEquals(headerOnly.carriedCount(), 0);
  }

  @Test
  public void carriedOriginsAreBoundedKeepingTheNewest() {
    BulkTelemetry t = create(tracer(new Collector()), true, false, null);
    // Two failed batches of 15,000 linked actions each: 30,000 failures, MAX_PENDING carried.
    List<List<DocWriteRequest<?>>> batches = new ArrayList<>();
    for (int b = 0; b < 2; b++) {
      List<DocWriteRequest<?>> actions = new ArrayList<>();
      batches.add(actions);
      try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
        for (int i = 0; i < 15_000; i++) {
          IndexRequest r = new IndexRequest("idx").id(b + "-" + i);
          t.onAdd(r);
          actions.add(r);
        }
      }
      assertEquals(t.pendingCount(), 15_000);
      Object key = new Object();
      t.beforeBulk(key, actions);
      assertEquals(t.pendingCount(), 0);
      if (b == 0) {
        t.afterBulk(key, 1L, actions);
      } else {
        t.afterBulk(key, new RuntimeException("reset"));
      }
    }
    assertEquals(t.carriedCount(), 20_000);
    assertEquals(t.droppedOrigins(), 10_000L, "the oldest 10,000 were evicted");
    t.onRequeue(batches.get(0).get(0));
    assertEquals(t.pendingCount(), 0, "the oldest failure's origin was evicted");
    t.onRequeue(batches.get(1).get(14_999));
    assertEquals(t.pendingCount(), 1, "the newest failure's origin is kept");
  }

  @Test
  public void pendingOriginsAreBoundedKeepingTheNewest() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);
    List<DocWriteRequest<?>> actions = new ArrayList<>();
    for (int i = 0; i < 20_010; i++) {
      // A distinct trace for the first ten, so the batch's links show whether they survived.
      String trace = i < 10 ? TRACE_B : TRACE_A;
      try (Scope ignored = remoteSpan(trace, "b7ad6b7169203331").makeCurrent()) {
        IndexRequest r = new IndexRequest("idx").id("" + i);
        t.onAdd(r);
        actions.add(r);
      }
    }
    assertEquals(t.pendingCount(), 20_000);
    assertEquals(t.droppedOrigins(), 10L);
    Object key = new Object();
    t.beforeBulk(key, actions);
    t.afterBulk(key, 1L, List.of());
    assertEquals(collector.spans.get(0).getLinks().size(), 1, "linking did not stop at the cap");
    assertEquals(
        collector.spans.get(0).getLinks().get(0).getSpanContext().getTraceId(),
        TRACE_A,
        "the oldest origins were the ones evicted");
  }

  @Test
  public void unflushedActionsAreNotRetained() throws InterruptedException {
    BulkTelemetry t = create(tracer(new Collector()), true, false, null);
    List<WeakReference<IndexRequest>> probes = new ArrayList<>();
    try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
      for (int i = 0; i < 1_000; i++) {
        // Added, then never flushed: the processor rejected or dropped it.
        IndexRequest r = new IndexRequest("idx").id("" + i).source(Map.of("big", "x".repeat(100)));
        t.onAdd(r);
        probes.add(new WeakReference<>(r));
      }
    }
    collectUntil(() -> probes.stream().allMatch(p -> p.get() == null) && t.pendingCount() == 0);
    assertEquals(t.droppedOrigins(), 1_000L, "purged once collected, and counted");
  }

  @Test
  public void batchWhoseAfterBulkNeverRunsIsAbandonedNotLeaked() throws InterruptedException {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, true, "gms");
    WeakReference<Object> probe = startLostBatch(t);
    collectUntil(() -> probe.get() == null && t.openBatches() == 0);
    assertEquals(t.abandonedBatches(), 1L);
    assertEquals(collector.spans.size(), 1);
    SpanData span = collector.spans.get(0);
    assertEquals(span.getAttributes().get(BulkTelemetry.ABANDONED), Boolean.TRUE);
    assertEquals(span.getStatus().getStatusCode(), StatusCode.ERROR);
  }

  private static WeakReference<Object> startLostBatch(BulkTelemetry t) {
    BulkRequest request = new BulkRequest().add(new IndexRequest("idx").id("1").source("{}", 0));
    t.beforeBulk(request, request.requests());
    return new WeakReference<>(request);
  }

  @Test
  public void openBatchesAreBounded() {
    Collector collector = new Collector();
    BulkTelemetry t = create(tracer(collector), true, false, null);
    List<Object> keys = new ArrayList<>();
    for (int i = 0; i < 1_025; i++) {
      Object key = new Object();
      keys.add(key);
      t.beforeBulk(key, List.of());
    }
    assertEquals(t.openBatches(), 1_024);
    assertEquals(t.abandonedBatches(), 1L);
    assertEquals(collector.spans.size(), 1, "the oldest batch's span was ended as abandoned");
    assertEquals(collector.spans.get(0).getAttributes().get(BulkTelemetry.ABANDONED), Boolean.TRUE);
    t.afterBulk(keys.get(0), 1L, List.of());
    assertEquals(collector.spans.size(), 1, "its late afterBulk is ignored");
    t.afterBulk(keys.get(1_024), 1L, List.of());
    assertEquals(collector.spans.size(), 2);
  }

  @Test
  public void closeReleasesEverythingAndUnregistersMeters() {
    Collector collector = new Collector();
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    BulkTelemetry t =
        BulkTelemetry.create(
            BulkTelemetryConfig.builder()
                .tracer(tracer(collector))
                .batchSpans(true)
                .meterRegistry(registry)
                .build());
    IndexRequest pendingAction = new IndexRequest("idx").id("p");
    IndexRequest failedAction = new IndexRequest("idx").id("f");
    try (Scope ignored = remoteSpan(TRACE_A, "b7ad6b7169203331").makeCurrent()) {
      t.onAdd(failedAction);
      t.onAdd(pendingAction);
    }
    Object failedBatch = new Object();
    t.beforeBulk(failedBatch, List.of(failedAction));
    t.afterBulk(failedBatch, 1L, List.of(failedAction));
    Object openBatch = new Object();
    t.beforeBulk(openBatch, List.of());

    assertEquals(gauge(registry, "pending"), 1.0);
    assertEquals(gauge(registry, "carried"), 1.0);
    assertEquals(gauge(registry, "open_batches"), 1.0);
    assertEquals(counter(registry, "origins_dropped"), 0.0);
    assertEquals(counter(registry, "batches_abandoned"), 0.0);

    Reference.reachabilityFence(pendingAction);
    Reference.reachabilityFence(failedAction);
    t.close();
    assertEquals(t.pendingCount(), 0);
    assertEquals(t.carriedCount(), 0);
    assertEquals(t.openBatches(), 0);
    assertEquals(t.abandonedBatches(), 1L);
    assertEquals(collector.spans.size(), 2);
    assertEquals(collector.spans.get(1).getAttributes().get(BulkTelemetry.ABANDONED), Boolean.TRUE);
    assertTrue(registry.getMeters().isEmpty(), "meters are unregistered on close");
    t.close(); // idempotent
    assertEquals(collector.spans.size(), 2);
    t.afterBulk(openBatch, 1L, List.of());
    assertEquals(collector.spans.size(), 2, "a batch completing after close is ignored");
  }

  @Test
  public void closeWithoutSpansOrMeters() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    // Header only: no tracer, so no origins and no meters even with a registry.
    BulkTelemetry headerOnly =
        BulkTelemetry.create(
            BulkTelemetryConfig.builder().opaqueId(true).meterRegistry(registry).build());
    assertTrue(registry.getMeters().isEmpty());
    Object batch = new Object();
    headerOnly.beforeBulk(batch, List.of());
    headerOnly.close();
    assertEquals(headerOnly.openBatches(), 0);
    assertEquals(headerOnly.abandonedBatches(), 1L, "counted even without a span to end");
    // Spans without a registry: nothing to unregister.
    BulkTelemetry noMeters = create(tracer(new Collector()), true, false, null);
    noMeters.close();
    assertEquals(noMeters.abandonedBatches(), 0L);
    BulkTelemetry.disabled().close(); // no-op
  }

  private static double gauge(SimpleMeterRegistry registry, String name) {
    return registry.get(BulkTelemetry.METRIC_PREFIX + "." + name).gauge().value();
  }

  private static double counter(SimpleMeterRegistry registry, String name) {
    return registry.get(BulkTelemetry.METRIC_PREFIX + "." + name).functionCounter().count();
  }

  private static void collectUntil(BooleanSupplier done) throws InterruptedException {
    for (int i = 0; i < 100 && !done.getAsBoolean(); i++) {
      System.gc();
      Thread.sleep(10);
    }
    assertTrue(done.getAsBoolean(), "garbage was not collected");
  }
}
