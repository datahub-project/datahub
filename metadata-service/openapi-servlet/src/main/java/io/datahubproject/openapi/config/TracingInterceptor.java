package io.datahubproject.openapi.config;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.telemetry.RequestAttributionConfiguration;
import io.datahubproject.metadata.context.RequestStats;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.AsyncHandlerInterceptor;

@Component
public class TracingInterceptor implements AsyncHandlerInterceptor {
  static final String REQUEST_STATS_ATTR = TracingInterceptor.class.getName() + ".requestStats";
  static final String START_MARKER_SPAN = "datahub.request.start";

  @Nullable private final Tracer tracer;
  @Nullable private final RequestAttributionConfiguration attribution;

  private static final TextMapGetter<HttpServletRequest> SERVLET_HEADER_GETTER =
      new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(HttpServletRequest carrier) {
          return Collections.list(carrier.getHeaderNames());
        }

        @Nullable
        @Override
        public String get(@Nullable HttpServletRequest carrier, String key) {
          return carrier == null ? null : carrier.getHeader(key);
        }
      };

  public TracingInterceptor(final SystemTelemetryContext systemTelemetryContext) {
    this(systemTelemetryContext, null);
  }

  @Autowired
  public TracingInterceptor(
      final SystemTelemetryContext systemTelemetryContext,
      @Nullable final ConfigurationProvider configurationProvider) {
    this.tracer = systemTelemetryContext.getTracer();
    this.attribution =
        configurationProvider != null && configurationProvider.getTelemetry() != null
            ? configurationProvider.getTelemetry().getRequestAttribution()
            : null;
  }

  @Override
  public boolean preHandle(
      HttpServletRequest request, HttpServletResponse response, Object handler) {

    if (request.getDispatcherType() == DispatcherType.ASYNC) {
      Span existingSpan = (Span) request.getAttribute("span");
      if (existingSpan != null && tracer != null) {
        request.setAttribute(
            "otelScope",
            makeScopeWithSpan(
                existingSpan, (RequestStats) request.getAttribute(REQUEST_STATS_ATTR)));
      }
      return true;
    }

    if (tracer != null) {
      String spanName = request.getMethod() + " " + request.getRequestURI();
      // With request attribution on, continue an inbound W3C trace (traceparent/tracestate) so the
      // ingress, frontend and GMS share a trace id even without the OpenTelemetry Java agent.
      Context parent =
          attribution != null && attribution.isEnabled()
              ? W3CTraceContextPropagator.getInstance()
                  .extract(Context.current(), request, SERVLET_HEADER_GETTER)
              : Context.current();
      Span span =
          tracer
              .spanBuilder(spanName)
              .setAttribute("http.method", request.getMethod())
              .setAttribute("http.url", request.getRequestURI())
              .setParent(parent)
              .startSpan();

      RequestStats stats = newRequestStats();
      if (stats != null) {
        request.setAttribute(REQUEST_STATS_ATTR, stats);
      }
      request.setAttribute("span", span);
      request.setAttribute("otelScope", makeScopeWithSpan(span, stats));

      SystemTelemetryContext.enableLogTracing(request);

      if (span.getSpanContext().isValid()) {
        SpanContext spanContext = span.getSpanContext();
        String traceId = spanContext.getTraceId();
        String spanId = spanContext.getSpanId();

        // W3C Trace Context format
        String flags = spanContext.getTraceFlags().isSampled() ? "01" : "00";
        response.setHeader("traceparent", String.format("00-%s-%s-%s", traceId, spanId, flags));

        if (SystemTelemetryContext.isLogTracingEnabled()) {
          // Add trace context to MDC for logging
          MDC.put("telemetryId", String.format("[%s-%s] ", traceId, spanId));
        }
      }

      emitStartMarker(request);
    }

    return true;
  }

  @Override
  public void afterConcurrentHandlingStarted(
      HttpServletRequest request, HttpServletResponse response, Object handler) {
    Scope scope = (Scope) request.getAttribute("otelScope");
    if (scope != null) {
      scope.close();
      request.removeAttribute("otelScope");
    }
  }

  @Override
  public void afterCompletion(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {

    if (tracer != null) {
      Span span = (Span) request.getAttribute("span");
      if (span != null) {
        try {
          span.setAttribute("http.status_code", response.getStatus());

          if (ex != null) {
            span.setStatus(StatusCode.ERROR);
            span.recordException(ex);
          } else {
            if (response.getStatus() >= 400) {
              span.setStatus(StatusCode.ERROR);
            } else {
              span.setStatus(StatusCode.OK);
            }
          }
          RequestStats stats = (RequestStats) request.getAttribute(REQUEST_STATS_ATTR);
          if (stats != null) {
            stats.finish(span);
          }
        } finally {
          span.end();
        }
      }

      Scope scope = (Scope) request.getAttribute("otelScope");
      if (scope != null) {
        scope.close();
      }

      if (SystemTelemetryContext.isLogTracingEnabled()) {
        SystemTelemetryContext.clear();
        MDC.clear();
      }
    }
  }

  /** A fresh accumulator when attribution is enabled, otherwise null (all hooks become no-ops). */
  @Nullable
  private RequestStats newRequestStats() {
    if (attribution == null || !attribution.isEnabled()) {
      return null;
    }
    return new RequestStats(attribution.isOpensearchOpaqueId());
  }

  /**
   * Zero-length span announcing that a request has started, so a backend can list in-flight
   * requests before their server span arrives. Actor comes from the authentication filter, which
   * runs before this interceptor; the operation is not known yet, only the route.
   */
  private void emitStartMarker(HttpServletRequest request) {
    if (attribution == null || !attribution.isStartMarker() || tracer == null) {
      return;
    }
    // Only user-facing API surfaces; health probes and internal endpoints would only add noise.
    String uri = request.getRequestURI();
    if (uri == null
        || !(uri.startsWith("/api/")
            || uri.startsWith("/openapi/")
            || uri.startsWith("/entities")
            || uri.startsWith("/aspects")
            || uri.startsWith("/relationships"))) {
      return;
    }
    Authentication auth = AuthenticationContext.getAuthentication();
    String actor = auth != null && auth.getActor() != null ? auth.getActor().toUrnStr() : "";
    tracer
        .spanBuilder(START_MARKER_SPAN)
        .setAttribute("actor.urn", actor)
        .setAttribute("http.method", request.getMethod())
        .setAttribute("http.url", request.getRequestURI())
        .setAttribute("datahub.request.started_at", System.currentTimeMillis())
        .startSpan()
        .end();
  }

  private Scope makeScopeWithSpan(Span span, @Nullable RequestStats stats) {
    Context context =
        Context.current()
            .with(SystemTelemetryContext.EVENT_SOURCE_CONTEXT_KEY, new AtomicReference<>(""))
            .with(SystemTelemetryContext.SOURCE_IP_CONTEXT_KEY, new AtomicReference<>(""))
            .with(span);
    if (stats != null) {
      context = context.with(RequestStats.CONTEXT_KEY, stats);
    }
    return context.makeCurrent();
  }
}
