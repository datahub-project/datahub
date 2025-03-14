package io.datahubproject.metadata.context;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.net.HttpHeaders;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.restli.server.ResourceContext;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RequestContextTest {

  private HttpServletRequest mockHttpRequest;
  private ResourceContext mockResourceContext;
  private Map<String, String> mockHeaders;
  private MockedStatic<MetricUtils> mockedMetrics;
  private com.codahale.metrics.Counter mockCounter;

  @BeforeMethod
  public void setup() {
    // Mock HTTP Servlet Request
    mockHttpRequest = Mockito.mock(HttpServletRequest.class);
    when(mockHttpRequest.getHeader(HttpHeaders.USER_AGENT)).thenReturn("Mozilla/5.0");
    when(mockHttpRequest.getHeader(HttpHeaders.X_FORWARDED_FOR)).thenReturn("192.168.1.100");
    when(mockHttpRequest.getRemoteAddr()).thenReturn("10.0.0.1");

    // Mock ResourceContext
    mockResourceContext = Mockito.mock(ResourceContext.class);
    mockHeaders = new HashMap<>();
    mockHeaders.put(HttpHeaders.USER_AGENT, "RestClient/1.0");
    mockHeaders.put(HttpHeaders.X_FORWARDED_FOR, "192.168.1.101");
    when(mockResourceContext.getRequestHeaders()).thenReturn(mockHeaders);
    when(mockResourceContext.getRawRequestContext())
        .thenReturn(mock(com.linkedin.r2.message.RequestContext.class));
    when(mockResourceContext.getRawRequestContext().getLocalAttr("REMOTE_ADDR"))
        .thenReturn("10.0.0.2");

    // Mock MetricUtils for testing captureAPIMetrics
    mockCounter = Mockito.mock(com.codahale.metrics.Counter.class);
    mockedMetrics = Mockito.mockStatic(MetricUtils.class);
    mockedMetrics.when(() -> MetricUtils.counter(anyString())).thenReturn(mockCounter);
  }

  @Test
  public void testBuilderDefault() {
    // Instead of directly calling .build(), use buildRestli method
    RequestContext context =
        RequestContext.builder()
            .buildRestli(
                "urn:li:corpuser:testuser",
                null, // null ResourceContext
                "test-request",
                "TestEntity");

    assertEquals(context.getActorUrn(), "urn:li:corpuser:testuser");
    assertEquals(context.getSourceIP(), ""); // Empty string since ResourceContext is null
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.RESTLI);
    assertEquals(context.getRequestID(), "test-request([TestEntity])");
    assertEquals(context.getUserAgent(), ""); // Empty string since ResourceContext is null
    assertNotNull(context.getAgentClass());
  }

  @Test
  public void testBuildGraphql() {
    Map<String, Object> variables = new HashMap<>();
    variables.put("key", "value");

    RequestContext context =
        RequestContext.builder()
            .buildGraphql(Constants.DATAHUB_ACTOR, mockHttpRequest, "GetUserQuery", variables);

    assertEquals(context.getActorUrn(), Constants.DATAHUB_ACTOR);
    assertEquals(context.getSourceIP(), "192.168.1.100");
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.GRAPHQL);
    assertEquals(context.getRequestID(), "GetUserQuery");
    assertEquals(context.getUserAgent(), "Mozilla/5.0");
  }

  @Test
  public void testBuildRestliWithAction() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(Constants.SYSTEM_ACTOR, mockResourceContext, "GetMetadata");

    assertEquals(context.getActorUrn(), Constants.SYSTEM_ACTOR);
    assertEquals(context.getSourceIP(), "192.168.1.101");
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.RESTLI);
    assertEquals(context.getRequestID(), "GetMetadata");
    assertEquals(context.getUserAgent(), "RestClient/1.0");
  }

  @Test
  public void testBuildRestliWithActionAndEntity() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(Constants.SYSTEM_ACTOR, mockResourceContext, "GetMetadata", "dataset");

    assertEquals(context.getActorUrn(), Constants.SYSTEM_ACTOR);
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.RESTLI);
    assertEquals(context.getRequestID(), "GetMetadata([dataset])");
  }

  @Test
  public void testBuildRestliWithActionAndEntityArray() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(
                Constants.SYSTEM_ACTOR,
                mockResourceContext,
                "GetMetadata",
                new String[] {"dataset", "dashboard"});

    assertEquals(context.getActorUrn(), Constants.SYSTEM_ACTOR);
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.RESTLI);
    assertEquals(context.getRequestID(), "GetMetadata([dataset, dashboard])");
  }

  @Test
  public void testBuildRestliWithActionAndEntityCollection() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(
                Constants.SYSTEM_ACTOR,
                mockResourceContext,
                "GetMetadata",
                Arrays.asList("dataset", "dashboard", "chart"));

    assertEquals(context.getActorUrn(), Constants.SYSTEM_ACTOR);
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.RESTLI);
    assertEquals(context.getRequestID(), "GetMetadata([dataset, dashboard, chart])");
  }

  @Test
  public void testBuildRestliWithNullResourceContext() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(Constants.SYSTEM_ACTOR, null, "GetMetadata", "dataset");

    assertEquals(context.getActorUrn(), Constants.SYSTEM_ACTOR);
    assertEquals(context.getSourceIP(), "");
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.RESTLI);
    assertEquals(context.getUserAgent(), "");
  }

  @Test
  public void testBuildOpenapiWithEntity() {
    RequestContext context =
        RequestContext.builder()
            .buildOpenapi(Constants.DATAHUB_ACTOR, mockHttpRequest, "GetMetadata", "chart");

    assertEquals(context.getActorUrn(), Constants.DATAHUB_ACTOR);
    assertEquals(context.getSourceIP(), "192.168.1.100");
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.OPENAPI);
    assertEquals(context.getRequestID(), "GetMetadata([chart])");
    assertEquals(context.getUserAgent(), "Mozilla/5.0");
  }

  @Test
  public void testBuildOpenapiWithEntityCollection() {
    RequestContext context =
        RequestContext.builder()
            .buildOpenapi(
                Constants.DATAHUB_ACTOR,
                mockHttpRequest,
                "GetMetadata",
                Arrays.asList("dataset", "chart"));

    assertEquals(context.getActorUrn(), Constants.DATAHUB_ACTOR);
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.OPENAPI);
    assertEquals(context.getRequestID(), "GetMetadata([dataset, chart])");
  }

  @Test
  public void testBuildOpenapiWithNullRequest() {
    RequestContext context =
        RequestContext.builder()
            .buildOpenapi(Constants.DATAHUB_ACTOR, null, "GetMetadata", "chart");

    assertEquals(context.getActorUrn(), Constants.DATAHUB_ACTOR);
    assertEquals(context.getSourceIP(), "");
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.OPENAPI);
    assertEquals(context.getUserAgent(), "");
  }

  @Test
  public void testBuildRequestIdWithNoEntities() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(
                Constants.SYSTEM_ACTOR, mockResourceContext, "GetMetadata", (List<String>) null);

    assertEquals(context.getRequestID(), "GetMetadata");
  }

  @Test
  public void testBuildRequestIdWithEmptyEntities() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(
                Constants.SYSTEM_ACTOR,
                mockResourceContext,
                "GetMetadata",
                Collections.emptyList());

    assertEquals(context.getRequestID(), "GetMetadata");
  }

  @Test
  public void testGetCacheKeyComponent() {
    RequestContext context = RequestContext.TEST;
    assertFalse(context.getCacheKeyComponent().isPresent());
  }

  @Test
  public void testToString() {
    // Using buildRestli instead of direct build()
    RequestContext context =
        RequestContext.builder().buildRestli("urn:li:corpuser:testuser", null, "test-request");

    String toString = context.toString();
    assertTrue(toString.contains("actorUrn='urn:li:corpuser:testuser'"));
    assertTrue(toString.contains("sourceIP=''"));
    assertTrue(toString.contains("requestAPI=RESTLI"));
    assertTrue(toString.contains("requestID='test-request'"));
    assertTrue(toString.contains("userAgent=''"));
    assertTrue(toString.contains("agentClass='"));
  }

  @Test
  public void testTestConstant() {
    RequestContext test = RequestContext.TEST;
    assertEquals(test.getRequestID(), "test");
    assertEquals(test.getRequestAPI(), RequestContext.RequestAPI.TEST);
  }

  @Test
  public void testCaptureAPIMetricsForSystemUser() {
    // Using buildRestli instead of direct build()
    RequestContext context =
        RequestContext.builder().buildRestli(Constants.SYSTEM_ACTOR, null, "test-request");

    // Verify that the counter was incremented
    verify(mockCounter, times(1)).inc();
  }

  @Test
  public void testCaptureAPIMetricsForDatahubUser() {
    // Using buildRestli instead of direct build()
    RequestContext context =
        RequestContext.builder().buildRestli(Constants.DATAHUB_ACTOR, null, "test-request");

    // Verify that the counter was incremented
    verify(mockCounter, times(1)).inc();
  }

  @Test
  public void testCaptureAPIMetricsForRegularUser() {
    // Using buildRestli instead of direct build()
    RequestContext context =
        RequestContext.builder().buildRestli("urn:li:corpuser:testuser", null, "test-request");

    // Verify that the counter was incremented
    verify(mockCounter, times(1)).inc();
  }

  @Test
  public void testEmptyUserAgent() {
    // Mock a ResourceContext with empty user agent
    ResourceContext emptyUAResourceContext = Mockito.mock(ResourceContext.class);
    Map<String, String> emptyUAHeaders = new HashMap<>();
    emptyUAHeaders.put(HttpHeaders.USER_AGENT, "");
    when(emptyUAResourceContext.getRequestHeaders()).thenReturn(emptyUAHeaders);
    when(emptyUAResourceContext.getRawRequestContext())
        .thenReturn(mock(com.linkedin.r2.message.RequestContext.class));
    when(emptyUAResourceContext.getRawRequestContext().getLocalAttr("REMOTE_ADDR"))
        .thenReturn("10.0.0.2");

    RequestContext context =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", emptyUAResourceContext, "test-request");

    // Verify that the agentClass is set to "Unknown"
    assertEquals("Unknown", context.getAgentClass());

    // Verify that a metric was captured with "unknown" agent class
    verify(mockCounter, times(1)).inc();
  }

  @Test
  public void testNullUserAgent() {
    try {
      // Create a ResourceContext with null headers map
      ResourceContext nullUAResourceContext = Mockito.mock(ResourceContext.class);
      when(nullUAResourceContext.getRequestHeaders()).thenReturn(Collections.emptyMap());
      when(nullUAResourceContext.getRawRequestContext())
          .thenReturn(mock(com.linkedin.r2.message.RequestContext.class));
      when(nullUAResourceContext.getRawRequestContext().getLocalAttr("REMOTE_ADDR"))
          .thenReturn("10.0.0.2");

      RequestContext context =
          RequestContext.builder()
              .buildRestli("urn:li:corpuser:testuser", nullUAResourceContext, "test-request");

      // Verify agent class is set to "Unknown" for null user agent
      assertEquals("Unknown", context.getAgentClass());

      // Verify a metric was captured
      verify(mockCounter, atLeastOnce()).inc();
    } catch (NullPointerException e) {
      fail("RequestContext should handle null userAgent gracefully");
    }
  }

  @Test
  public void testAgentClassPopulated() {
    // Create a direct instance of RequestContext with a browser user agent
    RequestContext context =
        new RequestContext(
            "urn:li:corpuser:testuser",
            "192.168.1.1",
            RequestContext.RequestAPI.TEST,
            "test-request",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

    // Verify the agentClass is populated (exact value will depend on the UserAgentAnalyzer library
    // results)
    assertNotNull(context.getAgentClass());
    // The agent class should be "Browser" for this user agent string, but we're only checking it's
    // not null
    assertFalse(context.getAgentClass().isEmpty());
  }

  @Test
  public void testAgentClassWithMetrics() {
    // Create a ResourceContext with browser user agent
    ResourceContext browserUAResourceContext = Mockito.mock(ResourceContext.class);
    Map<String, String> browserUAHeaders = new HashMap<>();
    browserUAHeaders.put(
        HttpHeaders.USER_AGENT,
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");
    when(browserUAResourceContext.getRequestHeaders()).thenReturn(browserUAHeaders);
    when(browserUAResourceContext.getRawRequestContext())
        .thenReturn(mock(com.linkedin.r2.message.RequestContext.class));
    when(browserUAResourceContext.getRawRequestContext().getLocalAttr("REMOTE_ADDR"))
        .thenReturn("10.0.0.2");

    RequestContext context =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", browserUAResourceContext, "test-request");

    // Verify the agentClass is populated
    assertNotNull(context.getAgentClass());

    // Verify a metric was captured
    verify(mockCounter, times(1)).inc();
  }

  @AfterMethod
  public void tearDown() {
    if (mockedMetrics != null) {
      mockedMetrics.close();
    }
  }
}
