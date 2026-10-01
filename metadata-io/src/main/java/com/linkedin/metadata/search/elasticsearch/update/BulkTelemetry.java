package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.search.utils.ESUtils;
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
import java.util.LinkedHashSet;
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
 *       pretending the batch belongs to any one request.
 * </ul>
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

  /** Most span links attached to one batch span; actions beyond this are counted, not linked. */
  static final int MAX_LINKS = 64;

  /** Most distinct index names recorded on one batch span. */
  static final int MAX_INDICES = 32;

  /** Bound on actions whose originating span is remembered between add and flush. */
  static final int MAX_PENDING = 20_000;

  private static final BulkTelemetry DISABLED = new BulkTelemetry(null, false, DEFAULT_SERVICE);

  @Nullable private final Tracer tracer;
  private final boolean opaqueIdEnabled;
  private final String service;
  private final String prefix;
  private final AtomicLong seq = new AtomicLong();

  // Identity maps: DocWriteRequest and BulkRequest do not define equals, and we want the exact
  // instances the processor hands back in beforeBulk / afterBulk.
  private final Map<Object, SpanContext> pending =
      Collections.synchronizedMap(new IdentityHashMap<>());
  private final Map<Object, Batch> batches = Collections.synchronizedMap(new IdentityHashMap<>());

  private BulkTelemetry(@Nullable Tracer tracer, boolean opaqueIdEnabled, @Nonnull String service) {
    this.tracer = tracer;
    this.opaqueIdEnabled = opaqueIdEnabled;
    this.service = service;
    this.prefix = String.format("%08x", ThreadLocalRandom.current().nextInt());
  }

  /** The no-op instance used when attribution is off. */
  @Nonnull
  public static BulkTelemetry disabled() {
    return DISABLED;
  }

  /**
   * @param tracer tracer for the batch span; ignored unless {@code batchSpans}
   * @param batchSpans emit an {@value #SPAN_NAME} span per flush
   * @param opaqueId send the batch id as {@code X-Opaque-Id}
   * @param service service name for the header, {@value #DEFAULT_SERVICE} when blank
   */
  @Nonnull
  public static BulkTelemetry create(
      @Nullable Tracer tracer, boolean batchSpans, boolean opaqueId, @Nullable String service) {
    Tracer spanTracer = batchSpans ? tracer : null;
    if (spanTracer == null && !opaqueId) {
      return DISABLED;
    }
    String svc = service == null || service.isBlank() ? DEFAULT_SERVICE : service.trim();
    return new BulkTelemetry(spanTracer, opaqueId, svc);
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
    if (ctx.isValid() && pending.size() < MAX_PENDING) {
      pending.put(action, ctx);
    }
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
    if (tracer != null) {
      Set<SpanContext> links = new LinkedHashSet<>();
      Set<String> indices = new TreeSet<>();
      if (actions != null) {
        for (DocWriteRequest<?> action : actions) {
          SpanContext ctx = pending.remove(action);
          if (ctx != null && links.size() < MAX_LINKS) {
            links.add(ctx);
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
      for (SpanContext link : links) {
        builder.addLink(link);
      }
      span = builder.startSpan();
    }
    batches.put(batchKey, new Batch(batchId, count, span));
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

  /** Ends the batch after a response: the store's {@code took} and the number of failed items. */
  public void afterBulk(@Nullable Object batchKey, long tookMs, long failures) {
    Batch batch = batchKey == null ? null : batches.remove(batchKey);
    if (batch == null || batch.span == null) {
      return;
    }
    batch.span.setAttribute(TOOK_MS, Math.max(0L, tookMs));
    batch.span.setAttribute(FAILURES, Math.max(0L, failures));
    if (failures > 0) {
      batch.span.setStatus(StatusCode.ERROR, failures + " item(s) failed");
    }
    batch.span.end();
  }

  /** Ends the batch after a transport-level failure: every action counts as failed. */
  public void afterBulk(@Nullable Object batchKey, @Nonnull Throwable failure) {
    Batch batch = batchKey == null ? null : batches.remove(batchKey);
    if (batch == null || batch.span == null) {
      return;
    }
    batch.span.setAttribute(FAILURES, (long) batch.actions);
    batch.span.recordException(failure);
    batch.span.setStatus(StatusCode.ERROR, String.valueOf(failure.getMessage()));
    batch.span.end();
  }

  /** Number of actions whose origin span is remembered but not yet flushed (tests, diagnostics). */
  public int pendingCount() {
    return pending.size();
  }

  /** Number of batches started but not yet ended (tests, diagnostics). */
  public int openBatches() {
    return batches.size();
  }

  private static final class Batch {
    private final String batchId;
    private final int actions;
    @Nullable private final Span span;

    private Batch(String batchId, int actions, @Nullable Span span) {
      this.batchId = batchId;
      this.actions = actions;
      this.span = span;
    }
  }
}
