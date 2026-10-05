package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.BulkTelemetryConfig;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.client.RequestOptions;

/**
 * Request attribution for bulk index writes.
 *
 * <p>Search-side calls run on the request thread, so {@code RequestStats} can tag them with the
 * request's trace id. Bulk writes cannot be tagged that way: the bulk processor collects actions
 * from many requests (or many Kafka records) and flushes them later from its own thread, where the
 * current span, if any, belongs to nobody in particular. This class gives each flushed batch an
 * identity of its own instead:
 *
 * <ul>
 *   <li>a batch id, {@code <process prefix>-<counter>}, sent to the store as {@code X-Opaque-Id} in
 *       the form {@code bulk|<service>|batch=<id>|n=<actions>} so the store's own logs and task
 *       list can name the batch (never a per-action UUID, which would defeat log grouping);
 *   <li>a root span named {@value #SPAN_NAME} covering the flush, with the batch id, action count,
 *       the distinct indices written, the store's {@code took} and the number of failed items;
 *   <li>OpenTelemetry span <em>links</em> from that span to the spans that were current when each
 *       action was added, so a backend can answer "which changes were in the slow batch" without
 *       pretending the batch belongs to any one request. One link per distinct trace, at most
 *       {@value #MAX_LINKS}.
 * </ul>
 *
 * <p>An action's origin span moves through four places, each bounded: {@link #onAdd} remembers it
 * in {@code pending}; {@link #beforeBulk} moves it into the batch, which holds it until the batch
 * ends; {@link #afterBulk(Object, long, Collection)} moves the origins of the <em>failed</em>
 * actions only (a transport failure fails them all) into {@code carried} and drops the batch; and
 * the listener, which requeues or gives up on each failed item inside its own {@code afterBulk},
 * either calls {@link #onRequeue}, which moves the origin back into {@code pending} so the retry
 * batch links to the same request, or {@link #forget}, which drops it. Nothing is kept for a
 * successful action once its batch ends, and nothing depends on how many other batches complete
 * between a failure and its requeue.
 *
 * <p>Memory is bounded and nothing here keeps a request alive. {@code pending}, {@code carried} and
 * the open batches are {@link WeakIdentityTable}s: keyed by identity through weak references, so an
 * action that never reaches a batch (its add was rejected, its processor closed) or a batch whose
 * {@code afterBulk} never runs is purged once the client lets go of it, and the source documents
 * are never retained by this class. Each table also has a fixed size; at the limit the oldest entry
 * is evicted, so tracking degrades for the oldest origins instead of stopping. At {@value
 * #MAX_PENDING} entries an origin table costs roughly 2 MB (a weak key and a map entry each; the
 * span contexts are shared by all actions added under the same span). A batch dropped that way, or
 * still open at {@link #close}, has its span ended with {@code datahub.bulk.abandoned=true}. With a
 * meter registry the table sizes and drop counts are published as gauges and counters (see {@link
 * #METRIC_PREFIX}).
 *
 * <p>Everything is off unless {@code telemetry.requestAttribution.enabled} is set (span) and {@code
 * telemetry.requestAttribution.opensearchOpaqueId} is set (header). When disabled every method is a
 * no-op and allocates nothing. Nothing here logs, blocks or throws.
 */
public final class BulkTelemetry {

  public static final String SPAN_NAME = "index bulk";
  public static final String DEFAULT_SERVICE = "datahub";

  public static final AttributeKey<String> BATCH_ID =
      AttributeKey.stringKey("datahub.bulk.batch_id");
  public static final AttributeKey<Long> ACTIONS = AttributeKey.longKey("datahub.bulk.actions");
  public static final AttributeKey<List<String>> INDICES =
      AttributeKey.stringArrayKey("datahub.bulk.indices");
  public static final AttributeKey<Long> TOOK_MS = AttributeKey.longKey("datahub.bulk.took_ms");
  public static final AttributeKey<Long> FAILURES = AttributeKey.longKey("datahub.bulk.failures");
  public static final AttributeKey<Boolean> ABANDONED =
      AttributeKey.booleanKey("datahub.bulk.abandoned");

  /**
   * Prefix of the meters published when a registry is configured, each tagged {@code
   * processor=<batch id prefix>}: gauges {@code .pending}, {@code .carried}, {@code .open_batches}
   * and counters {@code .origins_dropped} (origins evicted at the size limit or purged because the
   * action was collected unflushed) and {@code .batches_abandoned}.
   */
  public static final String METRIC_PREFIX = "datahub.bulk.telemetry";

  /**
   * Most span links on one batch span: one per distinct trace id (the first span seen for a trace),
   * so a request that added many actions is one link. Traces beyond this are counted, not linked.
   */
  static final int MAX_LINKS = 64;

  /** Most distinct index names recorded on one batch span. */
  static final int MAX_INDICES = 32;

  /**
   * Bound on actions whose originating span is remembered between add and flush ({@code pending}),
   * and separately on failed actions whose origin is held for a requeue ({@code carried}). At the
   * limit the oldest entry is evicted.
   */
  static final int MAX_PENDING = 20_000;

  /**
   * Bound on batches started and not yet ended. In-flight batches number at most the processors'
   * thread count times their concurrent requests; this only matters if {@code afterBulk} is lost.
   */
  static final int MAX_OPEN_BATCHES = 1_024;

  private static final BulkTelemetry DISABLED =
      new BulkTelemetry(null, false, DEFAULT_SERVICE, null);

  @Nullable private final Tracer tracer;
  private final boolean opaqueIdEnabled;
  private final String service;
  private final String prefix;
  private final AtomicLong seq = new AtomicLong();
  private final AtomicLong abandoned = new AtomicLong();

  // Weak identity tables: DocWriteRequest and BulkRequest do not define equals, we want the exact
  // instances the processor hands back in beforeBulk / afterBulk, and we must not keep them alive.
  private final WeakIdentityTable<SpanContext> pending = new WeakIdentityTable<>(MAX_PENDING, null);
  private final WeakIdentityTable<Batch> batches =
      new WeakIdentityTable<>(MAX_OPEN_BATCHES, this::abandon);

  /**
   * Origins of failed actions between the end of their batch and the listener's decision to requeue
   * ({@link #onRequeue}) or give up ({@link #forget}). Only populated when spans are on.
   */
  private final WeakIdentityTable<SpanContext> carried = new WeakIdentityTable<>(MAX_PENDING, null);

  @Nullable private final MeterRegistry meterRegistry;
  private final List<Meter> meters = new ArrayList<>();

  private BulkTelemetry(
      @Nullable Tracer tracer,
      boolean opaqueIdEnabled,
      @Nonnull String service,
      @Nullable MeterRegistry meterRegistry) {
    this.tracer = tracer;
    this.opaqueIdEnabled = opaqueIdEnabled;
    this.service = service;
    this.prefix = String.format("%08x", ThreadLocalRandom.current().nextInt());
    this.meterRegistry = tracer != null ? meterRegistry : null;
    if (this.meterRegistry != null) {
      registerMeters(this.meterRegistry);
    }
  }

  private void registerMeters(@Nonnull MeterRegistry registry) {
    meters.add(
        Gauge.builder(METRIC_PREFIX + ".pending", this, BulkTelemetry::pendingCount)
            .tag("processor", prefix)
            .register(registry));
    meters.add(
        Gauge.builder(METRIC_PREFIX + ".carried", this, BulkTelemetry::carriedCount)
            .tag("processor", prefix)
            .register(registry));
    meters.add(
        Gauge.builder(METRIC_PREFIX + ".open_batches", this, BulkTelemetry::openBatches)
            .tag("processor", prefix)
            .register(registry));
    meters.add(
        FunctionCounter.builder(
                METRIC_PREFIX + ".origins_dropped", this, BulkTelemetry::droppedOrigins)
            .tag("processor", prefix)
            .register(registry));
    meters.add(
        FunctionCounter.builder(
                METRIC_PREFIX + ".batches_abandoned", this, BulkTelemetry::abandonedBatches)
            .tag("processor", prefix)
            .register(registry));
  }

  /** The no-op instance used when attribution is off. */
  @Nonnull
  public static BulkTelemetry disabled() {
    return DISABLED;
  }

  /**
   * The instance for {@code config}: {@link #disabled()} unless it enables spans (with a tracer) or
   * the header. A blank service name falls back to {@value #DEFAULT_SERVICE}.
   */
  @Nonnull
  public static BulkTelemetry create(@Nonnull BulkTelemetryConfig config) {
    if (!config.isEnabled()) {
      return DISABLED;
    }
    String service = config.getServiceName();
    String svc = service == null || service.isBlank() ? DEFAULT_SERVICE : service.trim();
    return new BulkTelemetry(
        config.spansEnabled() ? config.getTracer() : null,
        config.isOpaqueId(),
        svc,
        config.getMeterRegistry());
  }

  public boolean isEnabled() {
    return tracer != null || opaqueIdEnabled;
  }

  /**
   * Remembers the span that is current while {@code action} is added to the processor, so the batch
   * it eventually lands in can link back to it. Called from the adding thread.
   */
  public void onAdd(@Nullable Object action) {
    if (tracer == null || action == null) {
      return;
    }
    SpanContext ctx = Span.current().getSpanContext();
    if (ctx.isValid()) {
      remember(action, ctx);
    }
  }

  /**
   * Carries a failed action's origin over to its next batch when the listener requeues it. Called
   * from the requeue path, before the action is re-added to the processor; consumes the carried
   * origin. No-op when spans are off or the action failed in no batch this instance ended.
   */
  public void onRequeue(@Nullable Object action) {
    if (tracer == null || action == null) {
      return;
    }
    SpanContext ctx = carried.remove(action);
    if (ctx != null) {
      remember(action, ctx);
    }
  }

  /**
   * Drops a failed action's carried origin when the listener gives up on it (not retriable, retries
   * exhausted, or requeue off), so it does not linger. No-op when spans are off or nothing is
   * carried for the action.
   */
  public void forget(@Nullable Object action) {
    if (tracer == null || action == null) {
      return;
    }
    carried.remove(action);
  }

  private void remember(@Nonnull Object action, @Nonnull SpanContext ctx) {
    pending.put(action, ctx);
  }

  /**
   * Starts the batch: assigns the id and, when spans are on, starts the {@value #SPAN_NAME} span
   * with links to the actions' origins. {@code batchKey} is the bulk request instance the processor
   * will pass to {@code afterBulk}.
   */
  public void beforeBulk(
      @Nullable Object batchKey, @Nullable Collection<? extends DocWriteRequest<?>> actions) {
    if (!isEnabled() || batchKey == null) {
      return;
    }
    int count = actions == null ? 0 : actions.size();
    String batchId = prefix + "-" + seq.incrementAndGet();
    Span span = null;
    Map<Object, SpanContext> origins = Collections.emptyMap();
    if (tracer != null) {
      // One link per trace: the first span seen for each trace id, in insertion order.
      Map<String, SpanContext> links = new LinkedHashMap<>();
      Set<String> indices = new TreeSet<>();
      origins = new IdentityHashMap<>();
      if (actions != null) {
        for (DocWriteRequest<?> action : actions) {
          SpanContext ctx = pending.remove(action);
          if (ctx != null) {
            origins.put(action, ctx);
            if (links.size() < MAX_LINKS || links.containsKey(ctx.getTraceId())) {
              links.putIfAbsent(ctx.getTraceId(), ctx);
            }
          }
          if (action.index() != null && indices.size() < MAX_INDICES) {
            indices.add(action.index());
          }
        }
      }
      // A root span on purpose: the flush thread's current span (if any) is unrelated to this
      // batch, and parenting to it is what produced orphaned client spans. The links are the join.
      SpanBuilder builder =
          tracer
              .spanBuilder(SPAN_NAME)
              .setNoParent()
              .setSpanKind(SpanKind.INTERNAL)
              .setAttribute(BATCH_ID, batchId)
              .setAttribute(ACTIONS, (long) count)
              .setAttribute(INDICES, new ArrayList<>(indices));
      for (SpanContext link : links.values()) {
        builder.addLink(link);
      }
      span = builder.startSpan();
    }
    batches.put(batchKey, new Batch(batchId, count, span, origins));
  }

  /** {@code X-Opaque-Id} value for the batch, or empty when the header is off or batch unknown. */
  @Nonnull
  public Optional<String> opaqueId(@Nullable Object batchKey) {
    if (!opaqueIdEnabled || batchKey == null) {
      return Optional.empty();
    }
    Batch batch = batches.get(batchKey);
    if (batch == null) {
      return Optional.empty();
    }
    return Optional.of("bulk|" + service + "|batch=" + batch.batchId + "|n=" + batch.actions);
  }

  /** {@code base} plus the batch's {@code X-Opaque-Id}, or {@code base} itself when none. */
  @Nonnull
  public RequestOptions requestOptions(@Nullable Object batchKey, @Nonnull RequestOptions base) {
    return opaqueId(batchKey)
        .map(id -> base.toBuilder().addHeader(ESUtils.OPAQUE_ID_HEADER, id).build())
        .orElse(base);
  }

  /**
   * Makes the batch the current context around the store call, so an agent's HTTP client span
   * becomes a child of the batch span rather than an orphan. The context is rooted: whatever
   * request happened to be current on this thread (an inline flush on a request thread) does not
   * leak its stats or trace into the batch. No-op when disabled or the batch is unknown.
   */
  @Nonnull
  public Scope makeCurrent(@Nullable Object batchKey) {
    if (!isEnabled() || batchKey == null) {
      return Scope.noop();
    }
    Batch batch = batches.get(batchKey);
    if (batch == null) {
      return Scope.noop();
    }
    Context ctx = batch.span != null ? Context.root().with(batch.span) : Context.root();
    return ctx.makeCurrent();
  }

  /**
   * Ends the batch after a response: the store's {@code took} and the actions whose items failed.
   * Their origins are carried for the listener, which must then {@link #onRequeue} or {@link
   * #forget} each one; successful actions' origins are dropped with the batch.
   */
  public void afterBulk(
      @Nullable Object batchKey, long tookMs, @Nullable Collection<?> failedActions) {
    Batch batch = end(batchKey);
    if (batch == null) {
      return;
    }
    long failures = 0;
    if (failedActions != null) {
      failures = failedActions.size();
      for (Object action : failedActions) {
        SpanContext ctx = batch.origins.get(action); // identity map: a null action finds nothing
        if (ctx != null) {
          carried.put(action, ctx);
        }
      }
    }
    batch.span.setAttribute(TOOK_MS, Math.max(0L, tookMs));
    batch.span.setAttribute(FAILURES, failures);
    if (failures > 0) {
      batch.span.setStatus(StatusCode.ERROR, failures + " item(s) failed");
    }
    batch.span.end();
  }

  /**
   * Ends the batch after a transport-level failure: every action counts as failed, so every origin
   * the batch held is carried for the listener's requeue-or-forget decision.
   */
  public void afterBulk(@Nullable Object batchKey, @Nonnull Throwable failure) {
    Batch batch = end(batchKey);
    if (batch == null) {
      return;
    }
    for (Map.Entry<Object, SpanContext> origin : batch.origins.entrySet()) {
      carried.put(origin.getKey(), origin.getValue());
    }
    batch.span.setAttribute(FAILURES, (long) batch.actions);
    batch.span.recordException(failure);
    batch.span.setStatus(StatusCode.ERROR, String.valueOf(failure.getMessage()));
    batch.span.end();
  }

  /**
   * Forgets the open batch. Returns the batch to end (it has a span), or null when there is nothing
   * to do: unknown key, or header-only mode where the batch has no span and no origins.
   */
  @Nullable
  private Batch end(@Nullable Object batchKey) {
    Batch batch = batchKey == null ? null : batches.remove(batchKey);
    if (batch == null || batch.span == null) {
      return null;
    }
    return batch;
  }

  /**
   * Ends the span of a batch that will never see {@code afterBulk}: evicted at {@value
   * #MAX_OPEN_BATCHES}, collected without ending, or still open at {@link #close}.
   */
  private void abandon(@Nonnull Batch batch) {
    abandoned.incrementAndGet();
    if (batch.span != null) {
      batch.span.setAttribute(ABANDONED, true);
      batch.span.setStatus(StatusCode.ERROR, "batch abandoned before completion");
      batch.span.end();
    }
  }

  /**
   * Releases everything this instance holds: forgets pending and carried origins, ends any batch
   * still open as abandoned and unregisters the meters. Call after the bulk processors are closed;
   * later calls are harmless no-ops.
   */
  public void close() {
    if (!isEnabled()) {
      return;
    }
    pending.clear();
    carried.clear();
    for (Batch batch : batches.clear()) {
      abandon(batch);
    }
    if (meterRegistry != null) {
      synchronized (meters) {
        for (Meter meter : meters) {
          meterRegistry.remove(meter);
        }
        meters.clear();
      }
    }
  }

  /** Number of actions whose origin span is remembered but not yet flushed (tests, diagnostics). */
  public int pendingCount() {
    return pending.size();
  }

  /**
   * Number of failed actions whose origin is held for a requeue that the listener has neither
   * requeued nor given up on yet (tests, diagnostics). Zero between flushes in steady state.
   */
  public int carriedCount() {
    return carried.size();
  }

  /** Number of batches started but not yet ended (tests, diagnostics). */
  public int openBatches() {
    return batches.size();
  }

  /**
   * Origins dropped without being linked: evicted at {@value #MAX_PENDING} or purged because their
   * action was collected before reaching a batch (rejected add, closed processor).
   */
  public long droppedOrigins() {
    return pending.dropped() + carried.dropped();
  }

  /** Batches whose span was ended without {@code afterBulk}; see {@link #close}. */
  public long abandonedBatches() {
    return abandoned.get();
  }

  private static final class Batch {
    private final String batchId;
    private final int actions;
    @Nullable private final Span span;

    /**
     * Origin span per action (identity), carried for failed actions at the end; empty when spans
     * are off. Strong keys are fine here: they are the batch's own actions, which the bulk request
     * holds anyway until the batch ends, and the batch itself is weakly keyed by that request.
     */
    private final Map<Object, SpanContext> origins;

    private Batch(
        String batchId, int actions, @Nullable Span span, Map<Object, SpanContext> origins) {
      this.batchId = batchId;
      this.actions = actions;
      this.span = span;
      this.origins = origins;
    }
  }
}
