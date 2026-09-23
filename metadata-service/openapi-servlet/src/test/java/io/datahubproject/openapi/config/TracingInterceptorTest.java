package io.datahubproject.openapi.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.testng.Assert.*;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.telemetry.RequestAttributionConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import io.datahubproject.metadata.context.RequestStats;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TracingInterceptorTest {

  @Mock private SystemTelemetryContext systemTelemetryContext;
  @Mock private Tracer tracer;
  @Mock private SpanBuilder spanBuilder;
  @Mock private Span span;
  @Mock private SpanContext spanContext;
  @Mock private TraceFlags traceFlags;
  @Mock private HttpServletRequest request;
  @Mock private HttpServletResponse response;
  @Mock private Scope scope;

  private TracingInterceptor interceptorWithTracer;
  private TracingInterceptor interceptorNoTracer;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    when(systemTelemetryContext.getTracer()).thenReturn(tracer);
    interceptorWithTracer = new TracingInterceptor(systemTelemetryContext);

    SystemTelemetryContext noTracerContext = mock(SystemTelemetryContext.class);
    when(noTracerContext.getTracer()).thenReturn(null);
    interceptorNoTracer = new TracingInterceptor(noTracerContext);

    when(tracer.spanBuilder(anyString())).thenReturn(spanBuilder);
    when(spanBuilder.setAttribute(anyString(), anyString())).thenReturn(spanBuilder);
    when(spanBuilder.setParent(any())).thenReturn(spanBuilder);
    when(spanBuilder.startSpan()).thenReturn(span);

    when(span.storeInContext(any(Context.class))).thenAnswer(inv -> inv.getArgument(0));

    when(span.getSpanContext()).thenReturn(spanContext);
    when(spanContext.isValid()).thenReturn(false);

    when(request.getDispatcherType()).thenReturn(DispatcherType.REQUEST);
    when(request.getMethod()).thenReturn("GET");
    when(request.getRequestURI()).thenReturn("/api/v1/entities");
    when(request.getCookies()).thenReturn(null);
    when(request.getHeader(SystemTelemetryContext.TRACE_HEADER)).thenReturn(null);
  }

  // --- preHandle: normal (non-async) dispatch ---

  @Test
  public void testPreHandle_normalDispatch_withTracer_invalidSpanContext() {
    when(spanContext.isValid()).thenReturn(false);

    boolean result = interceptorWithTracer.preHandle(request, response, new Object());

    assertTrue(result);
    verify(request).setAttribute(eq("span"), eq(span));
    verify(request).setAttribute(eq("otelScope"), any(Scope.class));
    // traceparent header should NOT be set since spanContext is invalid
    verify(response, never()).setHeader(eq("traceparent"), anyString());
  }

  @Test
  public void testPreHandle_normalDispatch_withTracer_validSpanContext_notSampled() {
    when(spanContext.isValid()).thenReturn(true);
    when(spanContext.getTraceId()).thenReturn("aabbccddeeff00112233445566778899");
    when(spanContext.getSpanId()).thenReturn("0011223344556677");
    when(spanContext.getTraceFlags()).thenReturn(traceFlags);
    when(traceFlags.isSampled()).thenReturn(false);

    boolean result = interceptorWithTracer.preHandle(request, response, new Object());

    assertTrue(result);
    verify(response)
        .setHeader(
            eq("traceparent"), eq("00-aabbccddeeff00112233445566778899-0011223344556677-00"));
  }

  @Test
  public void testPreHandle_normalDispatch_withTracer_validSpanContext_sampled() {
    when(spanContext.isValid()).thenReturn(true);
    when(spanContext.getTraceId()).thenReturn("aabbccddeeff00112233445566778899");
    when(spanContext.getSpanId()).thenReturn("0011223344556677");
    when(spanContext.getTraceFlags()).thenReturn(traceFlags);
    when(traceFlags.isSampled()).thenReturn(true);

    boolean result = interceptorWithTracer.preHandle(request, response, new Object());

    assertTrue(result);
    verify(response)
        .setHeader(
            eq("traceparent"), eq("00-aabbccddeeff00112233445566778899-0011223344556677-01"));
  }

  @Test
  public void testPreHandle_normalDispatch_noTracer() {
    boolean result = interceptorNoTracer.preHandle(request, response, new Object());

    assertTrue(result);
    verify(request, never()).setAttribute(eq("span"), any());
    verify(response, never()).setHeader(anyString(), anyString());
  }

  // --- preHandle: ASYNC dispatch ---

  @Test
  public void testPreHandle_asyncDispatch_withExistingSpan() {
    when(request.getDispatcherType()).thenReturn(DispatcherType.ASYNC);
    when(request.getAttribute("span")).thenReturn(span);

    boolean result = interceptorWithTracer.preHandle(request, response, new Object());

    assertTrue(result);
    verify(request).setAttribute(eq("otelScope"), any(Scope.class));
    verify(tracer, never()).spanBuilder(anyString());
  }

  @Test
  public void testPreHandle_asyncDispatch_noExistingSpan() {
    when(request.getDispatcherType()).thenReturn(DispatcherType.ASYNC);
    when(request.getAttribute("span")).thenReturn(null);

    boolean result = interceptorWithTracer.preHandle(request, response, new Object());

    assertTrue(result);
    verify(request, never()).setAttribute(eq("otelScope"), any());
    verify(tracer, never()).spanBuilder(anyString());
  }

  @Test
  public void testPreHandle_asyncDispatch_noTracer() {
    when(request.getDispatcherType()).thenReturn(DispatcherType.ASYNC);
    when(request.getAttribute("span")).thenReturn(span);

    boolean result = interceptorNoTracer.preHandle(request, response, new Object());

    assertTrue(result);
    verify(request, never()).setAttribute(eq("otelScope"), any());
  }

  // --- afterConcurrentHandlingStarted ---

  @Test
  public void testAfterConcurrentHandlingStarted_withScope() {
    when(request.getAttribute("otelScope")).thenReturn(scope);

    interceptorWithTracer.afterConcurrentHandlingStarted(request, response, new Object());

    verify(scope).close();
    verify(request).removeAttribute("otelScope");
  }

  @Test
  public void testAfterConcurrentHandlingStarted_noScope() {
    when(request.getAttribute("otelScope")).thenReturn(null);

    interceptorWithTracer.afterConcurrentHandlingStarted(request, response, new Object());

    verify(scope, never()).close();
    verify(request, never()).removeAttribute(anyString());
  }

  // --- afterCompletion ---

  @Test
  public void testAfterCompletion_withTracer_successStatus() {
    when(request.getAttribute("span")).thenReturn(span);
    when(request.getAttribute("otelScope")).thenReturn(null);
    when(response.getStatus()).thenReturn(200);

    interceptorWithTracer.afterCompletion(request, response, new Object(), null);

    verify(span).setAttribute("http.status_code", 200);
    verify(span).setStatus(StatusCode.OK);
    verify(span).end();
  }

  @Test
  public void testAfterCompletion_withTracer_errorStatus() {
    when(request.getAttribute("span")).thenReturn(span);
    when(request.getAttribute("otelScope")).thenReturn(null);
    when(response.getStatus()).thenReturn(500);

    interceptorWithTracer.afterCompletion(request, response, new Object(), null);

    verify(span).setAttribute("http.status_code", 500);
    verify(span).setStatus(StatusCode.ERROR);
    verify(span).end();
  }

  @Test
  public void testAfterCompletion_withTracer_clientErrorStatus() {
    when(request.getAttribute("span")).thenReturn(span);
    when(request.getAttribute("otelScope")).thenReturn(null);
    when(response.getStatus()).thenReturn(404);

    interceptorWithTracer.afterCompletion(request, response, new Object(), null);

    verify(span).setAttribute("http.status_code", 404);
    verify(span).setStatus(StatusCode.ERROR);
    verify(span).end();
  }

  @Test
  public void testAfterCompletion_withTracer_withException() {
    RuntimeException ex = new RuntimeException("test error");
    when(request.getAttribute("span")).thenReturn(span);
    when(request.getAttribute("otelScope")).thenReturn(null);
    when(response.getStatus()).thenReturn(200);

    interceptorWithTracer.afterCompletion(request, response, new Object(), ex);

    verify(span).setStatus(StatusCode.ERROR);
    verify(span).recordException(ex);
    verify(span).end();
  }

  @Test
  public void testAfterCompletion_withTracer_closesResidualScope() {
    when(request.getAttribute("span")).thenReturn(span);
    when(request.getAttribute("otelScope")).thenReturn(scope);
    when(response.getStatus()).thenReturn(200);

    interceptorWithTracer.afterCompletion(request, response, new Object(), null);

    verify(scope).close();
    verify(span).end();
  }

  @Test
  public void testAfterCompletion_withTracer_noSpan() {
    when(request.getAttribute("span")).thenReturn(null);
    when(request.getAttribute("otelScope")).thenReturn(null);

    interceptorWithTracer.afterCompletion(request, response, new Object(), null);

    verify(span, never()).end();
  }

  @Test
  public void testAfterCompletion_noTracer() {
    interceptorNoTracer.afterCompletion(request, response, new Object(), null);

    verify(span, never()).end();
    verify(request, never()).getAttribute(anyString());
  }

  // --- request attribution (telemetry.requestAttribution.*) ---

  private TracingInterceptor attributed(boolean enabled, boolean marker) {
    return attributed(enabled, marker, enabled);
  }

  private TracingInterceptor attributed(boolean enabled, boolean marker, boolean continuation) {
    RequestAttributionConfiguration attribution = new RequestAttributionConfiguration();
    attribution.setEnabled(enabled);
    attribution.setStartMarker(marker);
    TelemetryConfiguration telemetry = new TelemetryConfiguration();
    telemetry.setRequestAttribution(attribution);
    telemetry.setTraceContinuation(continuation);
    ConfigurationProvider provider = new ConfigurationProvider();
    provider.setTelemetry(telemetry);
    return new TracingInterceptor(systemTelemetryContext, provider);
  }

  @Test
  public void testAttribution_disabled_noStatsAndFreshParent() {
    when(request.getHeader("traceparent"))
        .thenReturn("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    TracingInterceptor interceptor = attributed(false, false);
    interceptor.preHandle(request, response, new Object());
    verify(request, never()).setAttribute(eq(TracingInterceptor.REQUEST_STATS_ATTR), any());
    // parent is the plain current context, i.e. the inbound header is ignored
    verify(spanBuilder)
        .setParent(argThat(ctx -> !Span.fromContext(ctx).getSpanContext().isValid()));
  }

  @Test
  public void
      testAttribution_enabled_withContinuation_continuesInboundTraceparentAndFinishesStats() {
    when(request.getHeader("traceparent"))
        .thenReturn("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    when(request.getHeaderNames())
        .thenReturn(java.util.Collections.enumeration(java.util.List.of("traceparent")));
    TracingInterceptor interceptor = attributed(true, false);
    interceptor.preHandle(request, response, new Object());

    verify(spanBuilder)
        .setParent(
            argThat(
                ctx ->
                    "0af7651916cd43dd8448eb211c80319c"
                        .equals(Span.fromContext(ctx).getSpanContext().getTraceId())));
    org.mockito.ArgumentCaptor<Object> statsCaptor =
        org.mockito.ArgumentCaptor.forClass(Object.class);
    verify(request).setAttribute(eq(TracingInterceptor.REQUEST_STATS_ATTR), statsCaptor.capture());
    RequestStats stats = (RequestStats) statsCaptor.getValue();
    assertNotNull(stats);

    when(request.getAttribute("span")).thenReturn(span);
    when(request.getAttribute(TracingInterceptor.REQUEST_STATS_ATTR)).thenReturn(stats);
    when(response.getStatus()).thenReturn(200);
    interceptor.afterCompletion(request, response, new Object(), null);
    verify(span).setAttribute(eq(RequestStats.ES_CALLS), eq(0L));
    verify(span).end();
  }

  @Test
  public void testAttribution_startMarker_onlyOnApiPaths() {
    SpanBuilder markerBuilder = mock(SpanBuilder.class);
    Span markerSpan = mock(Span.class);
    when(tracer.spanBuilder(TracingInterceptor.START_MARKER_SPAN)).thenReturn(markerBuilder);
    when(markerBuilder.setAttribute(anyString(), anyString())).thenReturn(markerBuilder);
    when(markerBuilder.setAttribute(anyString(), anyLong())).thenReturn(markerBuilder);
    when(markerBuilder.startSpan()).thenReturn(markerSpan);

    TracingInterceptor interceptor = attributed(true, true);
    when(request.getRequestURI()).thenReturn("/api/graphql");
    interceptor.preHandle(request, response, new Object());
    verify(markerSpan, times(1)).end();

    when(request.getRequestURI()).thenReturn("/health");
    interceptor.preHandle(request, response, new Object());
    verify(markerSpan, times(1)).end(); // unchanged: not an API path

    // marker requested but no tracer at all
    SystemTelemetryContext noTracer = mock(SystemTelemetryContext.class);
    when(noTracer.getTracer()).thenReturn(null);
    RequestAttributionConfiguration attribution = new RequestAttributionConfiguration();
    attribution.setEnabled(true);
    attribution.setStartMarker(true);
    TelemetryConfiguration telemetry = new TelemetryConfiguration();
    telemetry.setRequestAttribution(attribution);
    ConfigurationProvider provider = new ConfigurationProvider();
    provider.setTelemetry(telemetry);
    when(request.getRequestURI()).thenReturn("/openapi/v3/entity/dataset");
    new TracingInterceptor(noTracer, provider).preHandle(request, response, new Object());
    verify(markerSpan, times(1)).end();
  }

  @Test
  public void testTraceContinuation_independentOfAttribution() {
    when(request.getHeader("traceparent"))
        .thenReturn("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    when(request.getHeaderNames())
        .thenReturn(java.util.Collections.enumeration(java.util.List.of("traceparent")));
    TracingInterceptor interceptor = attributed(false, false, true);
    interceptor.preHandle(request, response, new Object());
    verify(spanBuilder)
        .setParent(
            argThat(
                ctx ->
                    "0af7651916cd43dd8448eb211c80319c"
                        .equals(Span.fromContext(ctx).getSpanContext().getTraceId())));
    verify(request, never()).setAttribute(eq(TracingInterceptor.REQUEST_STATS_ATTR), any());
  }

  @Test
  public void testAttribution_constructorToleratesMissingConfig() {
    ConfigurationProvider provider = new ConfigurationProvider(); // telemetry is null
    TracingInterceptor a = new TracingInterceptor(systemTelemetryContext, provider);
    assertTrue(a.preHandle(request, response, new Object()));
    TracingInterceptor b = new TracingInterceptor(systemTelemetryContext, null);
    assertTrue(b.preHandle(request, response, new Object()));
    verify(request, never()).setAttribute(eq(TracingInterceptor.REQUEST_STATS_ATTR), any());
  }

  @Test
  public void testAttribution_asyncDispatchReusesStats() {
    TracingInterceptor interceptor = attributed(true, false);
    RequestStats stats = new RequestStats(false);
    when(request.getDispatcherType()).thenReturn(DispatcherType.ASYNC);
    when(request.getAttribute("span")).thenReturn(span);
    when(request.getAttribute(TracingInterceptor.REQUEST_STATS_ATTR)).thenReturn(stats);
    assertTrue(interceptor.preHandle(request, response, new Object()));
    verify(request).setAttribute(eq("otelScope"), any());
  }

  @Test
  public void testServletHeaderGetter() {
    when(request.getHeaderNames())
        .thenReturn(java.util.Collections.enumeration(java.util.List.of("a", "b")));
    when(request.getHeader("a")).thenReturn("1");
    assertEquals(
        java.util.List.copyOf(
            (java.util.Collection<String>) TracingInterceptor.SERVLET_HEADER_GETTER.keys(request)),
        java.util.List.of("a", "b"));
    assertEquals(TracingInterceptor.SERVLET_HEADER_GETTER.get(request, "a"), "1");
    assertNull(TracingInterceptor.SERVLET_HEADER_GETTER.get(null, "a"));
  }
}
