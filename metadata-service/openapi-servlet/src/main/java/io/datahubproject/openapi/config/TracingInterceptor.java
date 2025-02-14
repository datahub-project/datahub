package io.datahubproject.openapi.config;

import io.datahubproject.metadata.context.TraceContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import javax.annotation.Nullable;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class TracingInterceptor implements HandlerInterceptor {
  @Nullable private final Tracer tracer;

  public TracingInterceptor(final TraceContext traceContext) {
    this.tracer = traceContext.getTracer();
  }

  @Override
  public boolean preHandle(
      HttpServletRequest request, HttpServletResponse response, Object handler) {

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
      span.makeCurrent();

      TraceContext.enableLogTracing(request);

      if (span.getSpanContext().isValid()) {
        SpanContext spanContext = span.getSpanContext();
        String traceId = spanContext.getTraceId();
        String spanId = spanContext.getSpanId();

        // W3C Trace Context format
        String flags = spanContext.getTraceFlags().isSampled() ? "01" : "00";
        response.setHeader("traceparent", String.format("00-%s-%s-%s", traceId, spanId, flags));

        if (TraceContext.isLogTracingEnabled()) {
          // Add trace context to MDC for logging
          MDC.put("telemetryId", String.format("[%s-%s] ", traceId, spanId));
        }
      }
    }

    return true;
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

      if (TraceContext.isLogTracingEnabled()) {
        TraceContext.clear();
        MDC.clear();
      }
    }
  }
}
