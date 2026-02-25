package io.datahubproject.metadata.context;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.net.HttpHeaders;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import jakarta.servlet.http.HttpServletRequest;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.MDC;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RequestContextTraceTest {

  private HttpServletRequest mockHttpRequest;
  private MetricUtils mockMetricUtils;
  private MockedStatic<Span> mockSpan;
  private MockedStatic<MDC> mockMDC;

  @BeforeMethod
  public void setup() {
    // Mock HTTP Servlet Request
    mockHttpRequest = Mockito.mock(HttpServletRequest.class);
    when(mockHttpRequest.getHeader(HttpHeaders.USER_AGENT)).thenReturn("Mozilla/5.0");
    when(mockHttpRequest.getHeader(HttpHeaders.X_FORWARDED_FOR)).thenReturn("192.168.1.100");
    when(mockHttpRequest.getRemoteAddr()).thenReturn("10.0.0.1");

    // Mock MetricUtils
    mockMetricUtils = Mockito.mock(MetricUtils.class);

    // Set up static mocks
    mockSpan = mockStatic(Span.class);
    mockMDC = mockStatic(MDC.class);
  }

  @AfterMethod
  public void tearDown() {
    if (mockSpan != null) {
      mockSpan.close();
    }
    if (mockMDC != null) {
      mockMDC.close();
    }
  }

  @Test
  public void testTraceIdWithValidSpan() {
    // Create mock span and span context
    Span currentSpan = mock(Span.class);
    SpanContext spanContext = mock(SpanContext.class);

    // Set up the span to return valid span context
    when(currentSpan.getSpanContext()).thenReturn(spanContext);
    when(spanContext.isValid()).thenReturn(true);
    when(spanContext.getTraceId()).thenReturn("1234567890abcdef1234567890abcdef");

    // Set up setAttribute to return the span for chaining
    when(currentSpan.setAttribute(anyString(), anyString())).thenReturn(currentSpan);

    // Configure static mock
    mockSpan.when(Span::current).thenReturn(currentSpan);

    // Build RequestContext
    RequestContext context =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", null)
            .metricUtils(mockMetricUtils)
            .build();

    // Verify trace ID was set
    assertEquals(context.getTraceId(), "1234567890abcdef1234567890abcdef");

    // Verify MDC was called
    mockMDC.verify(() -> MDC.put("traceId", "1234567890abcdef1234567890abcdef"));
  }

  @Test
  public void testTraceIdWithInvalidSpan() {
    // Create mock span with invalid context
    Span currentSpan = mock(Span.class);
    SpanContext spanContext = mock(SpanContext.class);

    when(currentSpan.getSpanContext()).thenReturn(spanContext);
    when(spanContext.isValid()).thenReturn(false);
    when(currentSpan.setAttribute(anyString(), anyString())).thenReturn(currentSpan);

    mockSpan.when(Span::current).thenReturn(currentSpan);

    // Build RequestContext
    RequestContext context =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", null, "test-request")
            .metricUtils(mockMetricUtils)
            .build();

    // Verify trace ID is null when span is invalid
    assertNull(context.getTraceId());

    // Verify MDC.put was never called
    mockMDC.verify(() -> MDC.put(anyString(), anyString()), never());
  }

  @Test
  public void testTraceIdWithNullSpan() {
    // Configure Span.current() to return null
    mockSpan.when(Span::current).thenReturn(null);

    // The constructor safely handles null spans
    RequestContext context =
        RequestContext.builder()
            .buildOpenapi(Constants.DATAHUB_ACTOR, mockHttpRequest, "GetMetadata", "chart")
            .metricUtils(mockMetricUtils)
            .build();

    // Verify trace ID is null when span is null
    assertNull(context.getTraceId());

    // Verify the context was created successfully
    assertEquals(context.getActorUrn(), Constants.DATAHUB_ACTOR);
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.OPENAPI);

    // Verify MDC.put was never called
    mockMDC.verify(() -> MDC.put(anyString(), anyString()), never());
  }

  @Test
  public void testTraceIdWithNullSpanContext() {
    // Create mock span that returns null span context
    Span currentSpan = mock(Span.class);
    when(currentSpan.getSpanContext()).thenReturn(null);
    when(currentSpan.setAttribute(anyString(), anyString())).thenReturn(currentSpan);

    mockSpan.when(Span::current).thenReturn(currentSpan);

    // Build RequestContext
    RequestContext context =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", null, "test-request")
            .metricUtils(mockMetricUtils)
            .build();

    // Verify trace ID is null
    assertNull(context.getTraceId());

    // Verify MDC.put was never called
    mockMDC.verify(() -> MDC.put(anyString(), anyString()), never());
  }

  @Test
  public void testTraceIdInToString() {
    // Create mock span with valid trace ID
    Span currentSpan = mock(Span.class);
    SpanContext spanContext = mock(SpanContext.class);

    when(currentSpan.getSpanContext()).thenReturn(spanContext);
    when(spanContext.isValid()).thenReturn(true);
    when(spanContext.getTraceId()).thenReturn("fedcba0987654321fedcba0987654321");
    when(currentSpan.setAttribute(anyString(), anyString())).thenReturn(currentSpan);

    mockSpan.when(Span::current).thenReturn(currentSpan);

    // Build RequestContext
    RequestContext context =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", null)
            .metricUtils(mockMetricUtils)
            .build();

    // Verify toString includes trace ID
    String toString = context.toString();
    assertTrue(toString.contains("traceId='fedcba0987654321fedcba0987654321'"));
  }

  @Test
  public void testTraceIdWithNoOpSpan() {
    // Create a no-op span (typically returned when no tracing is active)
    Span currentSpan = mock(Span.class);
    SpanContext spanContext =
        SpanContext.create(
            "00000000000000000000000000000000", // All zeros trace ID
            "0000000000000000", // All zeros span ID
            TraceFlags.getDefault(),
            TraceState.getDefault());

    when(currentSpan.getSpanContext()).thenReturn(spanContext);
    when(currentSpan.setAttribute(anyString(), anyString())).thenReturn(currentSpan);

    mockSpan.when(Span::current).thenReturn(currentSpan);

    // Build RequestContext
    RequestContext context =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", null, "test-request")
            .metricUtils(mockMetricUtils)
            .build();

    // No-op span context is considered invalid, so trace ID should be null
    assertNull(context.getTraceId());
  }

  @Test
  public void testMultipleRequestContextsWithSameSpan() {
    // Create mock span
    Span currentSpan = mock(Span.class);
    SpanContext spanContext = mock(SpanContext.class);

    when(currentSpan.getSpanContext()).thenReturn(spanContext);
    when(spanContext.isValid()).thenReturn(true);
    when(spanContext.getTraceId()).thenReturn("abcdef1234567890abcdef1234567890");
    when(currentSpan.setAttribute(anyString(), anyString())).thenReturn(currentSpan);

    mockSpan.when(Span::current).thenReturn(currentSpan);

    // Build multiple RequestContexts
    RequestContext context1 =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:user1", mockHttpRequest, "Query1", null)
            .metricUtils(mockMetricUtils)
            .build();

    RequestContext context2 =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:user2", null, "test-request")
            .metricUtils(mockMetricUtils)
            .build();

    // Both should have the same trace ID
    assertEquals(context1.getTraceId(), "abcdef1234567890abcdef1234567890");
    assertEquals(context2.getTraceId(), "abcdef1234567890abcdef1234567890");

    // MDC should have been called twice
    mockMDC.verify(() -> MDC.put("traceId", "abcdef1234567890abcdef1234567890"), times(2));
  }

  @Test
  public void testRequestContextBuilderSetsSpanAttributes() {
    // Create mock span
    Span currentSpan = mock(Span.class);
    when(currentSpan.setAttribute(anyString(), anyString())).thenReturn(currentSpan);

    mockSpan.when(Span::current).thenReturn(currentSpan);

    // Build RequestContext
    RequestContext context =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", null)
            .metricUtils(mockMetricUtils)
            .build();

    // Verify span attributes were set in the builder
    verify(currentSpan).setAttribute("user.id", "urn:li:corpuser:testuser");
    verify(currentSpan).setAttribute("request.api", "GRAPHQL");
    verify(currentSpan).setAttribute("request.id", "GetUserQuery");
  }

  @Test
  public void testTraceIdWithExceptionDuringExtraction() {
    // The constructor code doesn't wrap the trace ID extraction in try-catch,
    // but in real scenarios, OpenTelemetry operations shouldn't throw exceptions.
    // This test verifies what happens if they do.

    // Create mock span that throws exception on getSpanContext()
    Span currentSpan = mock(Span.class);
    when(currentSpan.getSpanContext()).thenThrow(new RuntimeException("Test exception"));
    when(currentSpan.setAttribute(anyString(), anyString())).thenReturn(currentSpan);

    mockSpan.when(Span::current).thenReturn(currentSpan);

    // The constructor will throw because it doesn't catch exceptions from span operations
    assertThrows(
        RuntimeException.class,
        () -> {
          RequestContext.builder()
              .buildRestli("urn:li:corpuser:testuser", null, "test-request")
              .metricUtils(mockMetricUtils)
              .build();
        });
  }

  @Test
  public void testTraceIdPersistenceAcrossAPIs() {
    // Test that the same trace ID is used across different API types
    Span currentSpan = mock(Span.class);
    SpanContext spanContext = mock(SpanContext.class);

    when(currentSpan.getSpanContext()).thenReturn(spanContext);
    when(spanContext.isValid()).thenReturn(true);
    when(spanContext.getTraceId()).thenReturn("1111222233334444aaaabbbbccccdddd");
    when(currentSpan.setAttribute(anyString(), anyString())).thenReturn(currentSpan);

    mockSpan.when(Span::current).thenReturn(currentSpan);

    // Create contexts for different APIs
    RequestContext graphqlContext =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:user", mockHttpRequest, "Query", null)
            .metricUtils(mockMetricUtils)
            .build();

    RequestContext restliContext =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:user", null, "action")
            .metricUtils(mockMetricUtils)
            .build();

    RequestContext openapiContext =
        RequestContext.builder()
            .buildOpenapi("urn:li:corpuser:user", mockHttpRequest, "operation", "entity")
            .metricUtils(mockMetricUtils)
            .build();

    // All should have the same trace ID
    assertEquals(graphqlContext.getTraceId(), "1111222233334444aaaabbbbccccdddd");
    assertEquals(restliContext.getTraceId(), "1111222233334444aaaabbbbccccdddd");
    assertEquals(openapiContext.getTraceId(), "1111222233334444aaaabbbbccccdddd");
  }

  @Test
  public void testRequestContextWithNoActiveTracing() {
    // In production, when no tracing is configured, OpenTelemetry returns a no-op span
    // that safely handles all operations without throwing exceptions
    Span noOpSpan = Span.getInvalid(); // This is OpenTelemetry's no-op span

    mockSpan.when(Span::current).thenReturn(noOpSpan);

    // Build RequestContext - should work without exceptions
    RequestContext context =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", null)
            .metricUtils(mockMetricUtils)
            .build();

    // The no-op span has an invalid span context, so trace ID should be null
    assertNull(context.getTraceId());

    // Verify the request context was created successfully
    assertEquals(context.getActorUrn(), "urn:li:corpuser:testuser");
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.GRAPHQL);
    assertEquals(context.getRequestID(), "GetUserQuery");
  }

  @Test
  public void testNullMetricUtilsDoesNotAffectTraceId() {
    // Create mock span
    Span currentSpan = mock(Span.class);
    SpanContext spanContext = mock(SpanContext.class);

    when(currentSpan.getSpanContext()).thenReturn(spanContext);
    when(spanContext.isValid()).thenReturn(true);
    when(spanContext.getTraceId()).thenReturn("validtraceid1234validtraceid1234");
    when(currentSpan.setAttribute(anyString(), anyString())).thenReturn(currentSpan);

    mockSpan.when(Span::current).thenReturn(currentSpan);

    // Build RequestContext without MetricUtils
    RequestContext context =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", null, "test-request")
            .metricUtils(null) // null MetricUtils
            .build();

    // Trace ID should still be extracted
    assertEquals(context.getTraceId(), "validtraceid1234validtraceid1234");
  }
}
