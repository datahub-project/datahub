package io.datahubproject.metadata.context;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringMap;
import com.linkedin.mxe.SystemMetadata;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TraceContextTest {
  @Mock private Tracer tracer;
  @Mock private HttpServletRequest request;
  @Mock private Span span;
  @Mock private SpanContext spanContext;

  private TraceContext traceContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    traceContext = TraceContext.builder().tracer(tracer).build();

    // Clear any existing thread local state
    TraceContext.clear();
  }

  @Test
  public void testEnableLogTracingWithHeader() {
    when(request.getHeader(TraceContext.TRACE_HEADER)).thenReturn("true");
    TraceContext.enableLogTracing(request);
    assertTrue(TraceContext.isLogTracingEnabled());
  }

  @Test
  public void testEnableLogTracingWithCookie() {
    when(request.getHeader(TraceContext.TRACE_HEADER)).thenReturn(null);
    Cookie cookie = new Cookie(TraceContext.TRACE_COOKIE, "true");
    when(request.getCookies()).thenReturn(new Cookie[] {cookie});
    TraceContext.enableLogTracing(request);
    assertTrue(TraceContext.isLogTracingEnabled());
  }

  @Test
  public void testEnableLogTracingDisabled() {
    when(request.getHeader(TraceContext.TRACE_HEADER)).thenReturn("false");
    when(request.getCookies()).thenReturn(null);
    TraceContext.enableLogTracing(request);
    assertFalse(TraceContext.isLogTracingEnabled());
  }

  @Test
  public void testWithTraceIdValidSpanContext() {
    SystemMetadata systemMetadata = new SystemMetadata();
    when(span.getSpanContext()).thenReturn(spanContext);
    when(spanContext.isValid()).thenReturn(true);
    when(spanContext.getTraceId()).thenReturn("test-trace-id");

    try (var mockedStatic = mockStatic(Span.class)) {
      mockedStatic.when(Span::current).thenReturn(span);
      SystemMetadata result = traceContext.withTraceId(systemMetadata, false);
      assertNotNull(result.getProperties());
      assertEquals(result.getProperties().get(TraceContext.TELEMETRY_TRACE_KEY), "test-trace-id");
    }
  }

  @Test
  public void testWithTraceIdInvalidSpanContext() {
    SystemMetadata systemMetadata = new SystemMetadata();
    when(span.getSpanContext()).thenReturn(spanContext);
    when(spanContext.isValid()).thenReturn(false);

    try (var mockedStatic = mockStatic(Span.class)) {
      mockedStatic.when(Span::current).thenReturn(span);
      SystemMetadata result = traceContext.withTraceId(systemMetadata, false);
      assertSame(result, systemMetadata);
    }
  }

  @Test
  public void testWithQueueSpanBatch() {
    // Setup
    List<SystemMetadata> batchMetadata = new ArrayList<>();
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setProperties(new StringMap());
    metadata1.getProperties().put(TraceContext.TELEMETRY_TRACE_KEY, "trace-1");
    metadata1.getProperties().put(TraceContext.TELEMETRY_QUEUE_SPAN_KEY, "span-1");
    metadata1.getProperties().put(TraceContext.TELEMETRY_LOG_KEY, "true");
    metadata1
        .getProperties()
        .put(TraceContext.TELEMETRY_ENQUEUED_AT, String.valueOf(System.currentTimeMillis()));

    SystemMetadata metadata2 = new SystemMetadata();
    metadata2.setProperties(new StringMap());
    metadata2.getProperties().put(TraceContext.TELEMETRY_TRACE_KEY, "trace-2");
    metadata2.getProperties().put(TraceContext.TELEMETRY_QUEUE_SPAN_KEY, "span-2");
    metadata2.getProperties().put(TraceContext.TELEMETRY_LOG_KEY, "false");
    metadata2
        .getProperties()
        .put(TraceContext.TELEMETRY_ENQUEUED_AT, String.valueOf(System.currentTimeMillis()));

    batchMetadata.add(metadata1);
    batchMetadata.add(metadata2);

    // Mock span builder chain for both consumer and processing spans
    io.opentelemetry.api.trace.SpanBuilder mockSpanBuilder =
        mock(io.opentelemetry.api.trace.SpanBuilder.class);
    when(mockSpanBuilder.setParent(any(Context.class))).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.setSpanKind(any())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.setAttribute(anyString(), anyString())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.setAttribute(anyString(), anyLong())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.addLink(any())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.startSpan()).thenReturn(span);

    when(tracer.spanBuilder(anyString())).thenReturn(mockSpanBuilder);
    when(span.setAttribute(anyString(), anyString())).thenReturn(span);
    when(span.setAttribute(anyString(), anyLong())).thenReturn(span);
    when(span.getSpanContext()).thenReturn(spanContext);

    // Execute & Verify - mainly checking that no exceptions are thrown
    traceContext.withQueueSpan(
        "test-operation",
        batchMetadata,
        "test-topic",
        () -> {
          // Do nothing
        });
  }

  @Test
  public void testWithSpanSupplier() {
    SpanBuilder mockSpanBuilder = mock(SpanBuilder.class);
    when(mockSpanBuilder.setAttribute(anyString(), anyString())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.startSpan()).thenReturn(span);
    when(tracer.spanBuilder(anyString())).thenReturn(mockSpanBuilder);

    when(span.setAttribute(anyString(), anyString())).thenReturn(span);
    when(span.setStatus(any())).thenReturn(span);
    when(span.makeCurrent()).thenReturn(mock(Scope.class));

    // Execute
    String result = traceContext.withSpan("test-operation", () -> "test-result", "attr1", "value1");

    // Verify
    assertEquals(result, "test-result");
    verify(mockSpanBuilder).startSpan();
    verify(span).end();
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testWithSpanSupplierException() {
    io.opentelemetry.api.trace.SpanBuilder mockSpanBuilder =
        mock(io.opentelemetry.api.trace.SpanBuilder.class);
    when(mockSpanBuilder.setAttribute(anyString(), anyString())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.startSpan()).thenReturn(span);
    when(tracer.spanBuilder(anyString())).thenReturn(mockSpanBuilder);

    when(span.setAttribute(anyString(), anyString())).thenReturn(span);
    when(span.setStatus(any(), anyString())).thenReturn(span);
    when(span.recordException(any(RuntimeException.class))).thenReturn(span);
    when(span.makeCurrent()).thenReturn(mock(Scope.class));

    try {
      traceContext.withSpan(
          "test-operation",
          () -> {
            throw new RuntimeException("test-exception");
          },
          "attr1",
          "value1");
    } finally {
      verify(mockSpanBuilder).startSpan();
      verify(span).setStatus(StatusCode.ERROR, "test-exception");
      verify(span).recordException(any(RuntimeException.class));
      verify(span).end();
    }
  }

  @Test
  public void testWithSpanRunnable() {
    SpanBuilder mockSpanBuilder = mock(SpanBuilder.class);
    when(mockSpanBuilder.setAttribute(anyString(), anyString())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.startSpan()).thenReturn(span);
    when(tracer.spanBuilder(anyString())).thenReturn(mockSpanBuilder);

    when(span.setAttribute(anyString(), anyString())).thenReturn(span);
    when(span.setStatus(any())).thenReturn(span);
    when(span.makeCurrent()).thenReturn(mock(Scope.class));

    AtomicBoolean executed = new AtomicBoolean(false);

    traceContext.withSpan("test-operation", () -> executed.set(true), "attr1", "value1");

    assertTrue(executed.get());
    verify(mockSpanBuilder).startSpan();
    verify(span).end();
  }

  @Test
  public void testWithSingleQueueSpan() {
    SystemMetadata metadata = new SystemMetadata();
    metadata.setProperties(new StringMap());
    metadata.getProperties().put(TraceContext.TELEMETRY_TRACE_KEY, "trace-1");
    metadata.getProperties().put(TraceContext.TELEMETRY_QUEUE_SPAN_KEY, "span-1");
    metadata.getProperties().put(TraceContext.TELEMETRY_LOG_KEY, "true");
    metadata
        .getProperties()
        .put(TraceContext.TELEMETRY_ENQUEUED_AT, String.valueOf(System.currentTimeMillis()));

    io.opentelemetry.api.trace.SpanBuilder mockSpanBuilder =
        mock(io.opentelemetry.api.trace.SpanBuilder.class);
    when(mockSpanBuilder.setParent(any(Context.class))).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.setSpanKind(any())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.setAttribute(anyString(), anyString())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.setAttribute(anyString(), anyLong())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.startSpan()).thenReturn(span);

    when(tracer.spanBuilder(anyString())).thenReturn(mockSpanBuilder);
    when(span.setAttribute(anyString(), anyString())).thenReturn(span);
    when(span.setAttribute(anyString(), anyLong())).thenReturn(span);
    when(span.makeCurrent()).thenReturn(mock(Scope.class));

    AtomicBoolean executed = new AtomicBoolean(false);

    traceContext.withQueueSpan(
        "test-operation", List.of(metadata), "test-topic", () -> executed.set(true));

    assertTrue(executed.get());
    verify(mockSpanBuilder, atLeast(1)).startSpan();
    verify(span, atLeast(1)).end();
  }

  @Test
  public void testWithProducerTrace() {
    SystemMetadata systemMetadata = new SystemMetadata();

    io.opentelemetry.api.trace.SpanBuilder mockSpanBuilder =
        mock(io.opentelemetry.api.trace.SpanBuilder.class);
    when(mockSpanBuilder.setParent(any(Context.class))).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.setSpanKind(any())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.setAttribute(anyString(), anyString())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.setAttribute(anyString(), anyLong())).thenReturn(mockSpanBuilder);
    when(mockSpanBuilder.startSpan()).thenReturn(span);

    when(tracer.spanBuilder(anyString())).thenReturn(mockSpanBuilder);
    when(span.setAttribute(anyString(), anyString())).thenReturn(span);
    when(span.setAttribute(anyString(), anyLong())).thenReturn(span);
    when(span.getSpanContext()).thenReturn(spanContext);
    when(spanContext.getSpanId()).thenReturn("test-span-id");
    when(spanContext.getTraceId()).thenReturn("test-trace-id");
    when(spanContext.isValid()).thenReturn(true);

    try (var mockedStatic = mockStatic(Span.class)) {
      mockedStatic.when(Span::current).thenReturn(span);

      SystemMetadata result =
          traceContext.withProducerTrace("test-operation", systemMetadata, "test-topic");

      assertNotNull(result.getProperties());
      assertTrue(result.getProperties().containsKey(TraceContext.TELEMETRY_TRACE_KEY));
      assertTrue(result.getProperties().containsKey(TraceContext.TELEMETRY_QUEUE_SPAN_KEY));
      assertTrue(result.getProperties().containsKey(TraceContext.TELEMETRY_LOG_KEY));
      assertTrue(result.getProperties().containsKey(TraceContext.TELEMETRY_ENQUEUED_AT));
      verify(mockSpanBuilder).startSpan();
      verify(span).end();
    }
  }
}
