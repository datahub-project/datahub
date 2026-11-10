package io.datahubproject.metadata.context;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Per-request accumulator for actor/operation attribution.
 *
 * <p>One instance is created at the start of an HTTP request (when {@code
 * telemetry.requestAttribution.enabled} is true) and carried in the OpenTelemetry {@link Context},
 * which GMS already propagates across its executors. Downstream code adds cheap facts to it as the
 * request runs: OpenSearch round-trip time, database connection hold time and the GraphQL arguments
 * that determine request size. At the end of the request {@link #finish(Span)} writes everything as
 * {@code datahub.*} attributes on the request's root span. Nothing here logs, blocks or throws;
 * when no accumulator is in scope every hook is a no-op, so unconfigured installs and non-HTTP code
 * paths are unaffected.
 */
public final class RequestStats {

  public static final ContextKey<RequestStats> CONTEXT_KEY =
      ContextKey.named("datahub.request.stats");

  // Attribute names. Kept under one prefix so a backend can promote them generically.
  public static final AttributeKey<Long> ES_CALLS = AttributeKey.longKey("datahub.es.calls");
  public static final AttributeKey<Double> ES_TIME_MS =
      AttributeKey.doubleKey("datahub.es.time_ms");
  public static final AttributeKey<Long> PG_CONN_CALLS =
      AttributeKey.longKey("datahub.pg.conn_calls");
  public static final AttributeKey<Double> PG_CONN_MS =
      AttributeKey.doubleKey("datahub.pg.conn_ms");
  public static final AttributeKey<Long> REQUEST_COUNT =
      AttributeKey.longKey("datahub.request.count");
  public static final AttributeKey<Long> REQUEST_START =
      AttributeKey.longKey("datahub.request.start");

  /** True when the server's async request timeout fired before the handler finished. */
  public static final AttributeKey<Boolean> REQUEST_TIMEOUT =
      AttributeKey.booleanKey("datahub.request.timeout");

  /** Distinct Postgres backend process ids this request's connections were served by. */
  public static final AttributeKey<List<Long>> PG_BACKEND_PIDS =
      AttributeKey.longArrayKey("datahub.pg.backend_pids");

  private static final int MAX_BACKEND_PIDS = 32;

  /** Bound on the variables walk so a pathological payload cannot make attribution expensive. */
  private static final int MAX_WALK_NODES = 50_000;

  private final boolean opaqueIdEnabled;

  private final AtomicLong esNanos = new AtomicLong();
  private final AtomicLong esCalls = new AtomicLong();
  private final AtomicLong dbNanos = new AtomicLong();
  private final AtomicLong dbCalls = new AtomicLong();
  private final AtomicLong esSeq = new AtomicLong();
  private final Set<Long> backendPids = ConcurrentHashMap.newKeySet();
  private volatile long timeoutAtNanos;

  @Nullable private volatile Span span;
  @Nullable private volatile String actorUrn;
  @Nullable private volatile String requestId;
  @Nullable private volatile Long requestCount;
  @Nullable private volatile Long requestStart;

  public RequestStats(boolean opaqueIdEnabled) {
    this.opaqueIdEnabled = opaqueIdEnabled;
  }

  /** The accumulator for the current request, if attribution is enabled and one is in scope. */
  @Nonnull
  public static Optional<RequestStats> current() {
    return Optional.ofNullable(Context.current().get(CONTEXT_KEY));
  }

  /**
   * Called once the actor and request id are known; also pins the span that receives the result.
   */
  public void attach(@Nullable Span span, @Nullable String actorUrn, @Nullable String requestId) {
    if (span != null && span.getSpanContext().isValid()) {
      this.span = span;
    }
    if (actorUrn != null) {
      this.actorUrn = actorUrn;
    }
    if (requestId != null) {
      this.requestId = requestId;
    }
  }

  public void recordSearch(long nanos) {
    esCalls.incrementAndGet();
    esNanos.addAndGet(Math.max(0, nanos));
  }

  public void recordDb(long nanos) {
    dbCalls.incrementAndGet();
    dbNanos.addAndGet(Math.max(0, nanos));
  }

  /**
   * Notes the Postgres backend process id ({@code pg_stat_activity.pid}, {@code %p} in the log
   * prefix) that served a borrowed connection, so statements in the server log join to this request
   * even without the OpenTelemetry agent's sqlcommenter comment.
   */
  public void recordDbBackendPid(long pid) {
    if (pid > 0 && backendPids.size() < MAX_BACKEND_PIDS) {
      backendPids.add(pid);
    }
  }

  /** Records that the async request timeout fired while the handler was still running. */
  public void markTimeout() {
    if (timeoutAtNanos == 0L) {
      timeoutAtNanos = System.nanoTime();
    }
  }

  /** {@code System.nanoTime()} when the timeout fired, or 0 when it did not. */
  public long getTimeoutAtNanos() {
    return timeoutAtNanos;
  }

  /** One-line summary of store usage so far, for log lines: "es=3 calls/120.4 ms, pg=2/5.1 ms". */
  @Nonnull
  public String summary() {
    return String.format(
        "es=%d calls/%.1f ms, pg=%d borrows/%.1f ms",
        esCalls.get(), esNanos.get() / 1_000_000.0d, dbCalls.get(), dbNanos.get() / 1_000_000.0d);
  }

  /**
   * Value for the OpenSearch {@code X-Opaque-Id} header, or empty when disabled or there is no
   * valid trace. Format: {@code trace=<traceId>|actor=<urn>|req=<requestId>|n=<call>}, where {@code
   * n} counts OpenSearch calls within the request so each slow-log line, task and Query Insights
   * record joins to one specific call and not just to the request. The prefix is chosen so it can
   * never collide with the {@code version|index|tempIndex} ids used by reindex tasks.
   */
  @Nonnull
  public Optional<String> opaqueId() {
    if (!opaqueIdEnabled) {
      return Optional.empty();
    }
    SpanContext ctx = Span.current().getSpanContext();
    if (!ctx.isValid()) {
      return Optional.empty();
    }
    StringBuilder sb = new StringBuilder(96).append("trace=").append(ctx.getTraceId());
    if (actorUrn != null) {
      sb.append("|actor=").append(actorUrn);
    }
    if (requestId != null) {
      sb.append("|req=").append(requestId);
    }
    sb.append("|n=").append(esSeq.incrementAndGet());
    return Optional.of(sb.toString());
  }

  /**
   * Pulls request-size arguments ({@code count}/{@code limit} and {@code start}) out of GraphQL
   * variables. Only the first occurrence of each key is used; the walk is bounded.
   */
  public void recordGraphqlVariables(@Nullable Map<String, Object> variables) {
    if (variables == null || variables.isEmpty()) {
      return;
    }
    Deque<Object> stack = new ArrayDeque<>();
    stack.push(variables);
    int nodes = 0;
    while (!stack.isEmpty() && nodes++ < MAX_WALK_NODES) {
      Object node = stack.pop();
      if (node instanceof Map) {
        for (Map.Entry<?, ?> e : ((Map<?, ?>) node).entrySet()) {
          Object k = e.getKey();
          Object v = e.getValue();
          if ("count".equals(k) || "limit".equals(k)) {
            if (requestCount == null && v instanceof Number) {
              requestCount = ((Number) v).longValue();
            }
          } else if ("start".equals(k)) {
            if (requestStart == null && v instanceof Number) {
              requestStart = ((Number) v).longValue();
            }
          } else if (v instanceof Map || v instanceof Collection) {
            stack.push(v);
          }
        }
      } else if (node instanceof Collection) {
        for (Object o : (Collection<?>) node) {
          if (o instanceof Map || o instanceof Collection) {
            stack.push(o);
          }
        }
      }
    }
  }

  /**
   * Writes the accumulated facts onto the pinned span, or {@code fallback} when none was pinned.
   */
  public void finish(@Nullable Span fallback) {
    Span target = span != null ? span : fallback;
    if (target == null) {
      return;
    }
    target.setAttribute(ES_CALLS, esCalls.get());
    target.setAttribute(ES_TIME_MS, esNanos.get() / 1_000_000.0d);
    target.setAttribute(PG_CONN_CALLS, dbCalls.get());
    target.setAttribute(PG_CONN_MS, dbNanos.get() / 1_000_000.0d);
    if (requestCount != null) {
      target.setAttribute(REQUEST_COUNT, requestCount);
    }
    if (requestStart != null) {
      target.setAttribute(REQUEST_START, requestStart);
    }
    if (timeoutAtNanos != 0L) {
      target.setAttribute(REQUEST_TIMEOUT, true);
    }
    if (!backendPids.isEmpty()) {
      List<Long> pids = new ArrayList<>(backendPids);
      pids.sort(null);
      target.setAttribute(PG_BACKEND_PIDS, pids);
    }
  }

  // Accessors used by tests and by the opaque-id builder.
  public long getEsCalls() {
    return esCalls.get();
  }

  public long getEsNanos() {
    return esNanos.get();
  }

  public long getDbCalls() {
    return dbCalls.get();
  }

  public long getDbNanos() {
    return dbNanos.get();
  }

  @Nullable
  public String getActorUrn() {
    return actorUrn;
  }

  @Nullable
  public String getRequestId() {
    return requestId;
  }

  @Nonnull
  public Set<Long> getBackendPids() {
    return backendPids;
  }

  @Nullable
  public Long getRequestCount() {
    return requestCount;
  }

  @Nullable
  public Long getRequestStart() {
    return requestStart;
  }
}
