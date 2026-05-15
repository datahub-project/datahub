package io.datahubproject.openapi.config;

import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.AsyncHandlerInterceptor;

@Component
public class TracingInterceptor implements AsyncHandlerInterceptor {
  @Nullable private final Tracer tracer;

  public TracingInterceptor(final SystemTelemetryContext systemTelemetryContext) {
    this.tracer = systemTelemetryContext.getTracer();
  }

  @Override
  public boolean preHandle(
      HttpServletRequest request, HttpServletResponse response, Object handler) {

    if (request.getDispatcherType() == DispatcherType.ASYNC) {
      Span existingSpan = (Span) request.getAttribute("span");
      if (existingSpan != null && tracer != null) {
        request.setAttribute("otelScope", makeScopeWithSpan(existingSpan));
      }
      return true;
    }

    if (tracer != null) {
      String spanName = request.getMethod() + " " + request.getRequestURI();
      Span span =
          tracer
              .spanBuilder(spanName)
              .setAttribute("http.method", request.getMethod())
              .setAttribute("http.url", request.getRequestURI())
              .setParent(Context.root())
              .startSpan();

      request.setAttribute("span", span);
      request.setAttribute("otelScope", makeScopeWithSpan(span));

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

  private Scope makeScopeWithSpan(Span span) {
    return Context.current()
        .with(SystemTelemetryContext.EVENT_SOURCE_CONTEXT_KEY, new AtomicReference<>(""))
        .with(SystemTelemetryContext.SOURCE_IP_CONTEXT_KEY, new AtomicReference<>(""))
        .with(span)
        .makeCurrent();
  }
}
