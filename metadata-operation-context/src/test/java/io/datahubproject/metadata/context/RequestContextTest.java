package io.datahubproject.metadata.context;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.restli.server.ResourceContext;
import io.datahubproject.metadata.context.usage.AuthChannel;
import io.datahubproject.metadata.context.usage.UsageOperation;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RequestContextTest {

  private HttpServletRequest mockHttpRequest;
  private ResourceContext mockResourceContext;
  private Map<String, String> mockHeaders;
  private MetricUtils mockMetricUtils;

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
    mockMetricUtils = Mockito.mock(MetricUtils.class);
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
                "TestEntity")
            .metricUtils(mockMetricUtils)
            .build();

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
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", variables)
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getActorUrn(), "urn:li:corpuser:testuser");
    assertEquals(context.getSourceIP(), "192.168.1.100");
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.GRAPHQL);
    assertEquals(context.getRequestID(), "GetUserQuery");
    assertEquals(context.getUserAgent(), "Mozilla/5.0");
  }

  @Test
  public void testBuildRestliWithAction() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(Constants.SYSTEM_ACTOR, mockResourceContext, "GetMetadata")
            .metricUtils(mockMetricUtils)
            .build();

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
            .buildRestli(Constants.SYSTEM_ACTOR, mockResourceContext, "GetMetadata", "dataset")
            .metricUtils(mockMetricUtils)
            .build();

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
                new String[] {"dataset", "dashboard"})
            .metricUtils(mockMetricUtils)
            .build();

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
                Arrays.asList("dataset", "dashboard", "chart"))
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getActorUrn(), Constants.SYSTEM_ACTOR);
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.RESTLI);
    assertEquals(context.getRequestID(), "GetMetadata([dataset, dashboard, chart])");
  }

  @Test
  public void testBuildRestliWithNullResourceContext() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(Constants.SYSTEM_ACTOR, null, "GetMetadata", "dataset")
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getActorUrn(), Constants.SYSTEM_ACTOR);
    assertEquals(context.getSourceIP(), "");
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.RESTLI);
    assertEquals(context.getUserAgent(), "");
  }

  @Test
  public void testBuildOpenapiWithEntity() {
    RequestContext context =
        RequestContext.builder()
            .buildOpenapi("urn:li:corpuser:testuser", mockHttpRequest, "GetMetadata", "chart")
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getActorUrn(), "urn:li:corpuser:testuser");
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
                "urn:li:corpuser:testuser",
                mockHttpRequest,
                "GetMetadata",
                Arrays.asList("dataset", "chart"))
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getActorUrn(), "urn:li:corpuser:testuser");
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.OPENAPI);
    assertEquals(context.getRequestID(), "GetMetadata([dataset, chart])");
  }

  @Test
  public void testBuildOpenapiWithNullRequest() {
    RequestContext context =
        RequestContext.builder()
            .buildOpenapi("urn:li:corpuser:testuser", null, "GetMetadata", "chart")
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getActorUrn(), "urn:li:corpuser:testuser");
    assertEquals(context.getSourceIP(), "");
    assertEquals(context.getRequestAPI(), RequestContext.RequestAPI.OPENAPI);
    assertEquals(context.getUserAgent(), "");
  }

  @Test
  public void testBuildRequestIdWithNoEntities() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(
                Constants.SYSTEM_ACTOR, mockResourceContext, "GetMetadata", (List<String>) null)
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getRequestID(), "GetMetadata");
  }

  @Test
  public void testBuildRequestIdWithEmptyEntities() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli(
                Constants.SYSTEM_ACTOR, mockResourceContext, "GetMetadata", Collections.emptyList())
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getRequestID(), "GetMetadata");
  }

  @Test
  public void testGetCacheKeyComponent() {
    RequestContext context = RequestContext.TEST.build();
    assertFalse(context.getCacheKeyComponent().isPresent());
  }

  @Test
  public void testToString() {
    // Using buildRestli instead of direct build()
    RequestContext context =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", null, "test-request")
            .metricUtils(mockMetricUtils)
            .build();

    String toString = context.toString();
    assertTrue(toString.contains("actorUrn='urn:li:corpuser:testuser'"));
    assertTrue(toString.contains("sourceIP=''"));
    assertTrue(toString.contains("requestAPI=RESTLI"));
    assertTrue(toString.contains("requestID='test-request'"));
    assertTrue(toString.contains("userAgent=''"));
    assertTrue(toString.contains("agentClass="));
  }

  @Test
  public void testToStringIncludesUsageFieldsWhenTagged() {
    RequestContext context =
        RequestContext.builder()
            .buildOpenapi("urn:li:corpuser:testuser", mockHttpRequest, "postEntities", "dataset")
            .usageOperation(UsageOperation.METADATA_INGEST.key())
            .usageIdentity("urn:li:corpuser:datahub")
            .authChannel(AuthChannel.PAT)
            .inputBytes(128L)
            .outputBytes(512L)
            .requestBodyMaterialized(true)
            .responseBodyMaterialized(true)
            .usageQuantity(3)
            .metricUtils(mockMetricUtils)
            .build();

    String toString = context.toString();
    assertTrue(toString.contains("usageOperation='metadata_ingest'"));
    assertTrue(toString.contains("authChannel=PAT"));
    assertTrue(toString.contains("inputBytes=128"));
    assertTrue(toString.contains("usageQuantity=3"));
    assertFalse(toString.contains("usageIdentity"));
    assertFalse(toString.contains("outputBytes"));
    assertFalse(toString.contains("requestBodyMaterialized"));
    assertFalse(toString.contains("responseBodyMaterialized"));
  }

  @Test
  public void testToStringOmitsUsageFieldsWhenUntagged() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", null, "test-request")
            .metricUtils(mockMetricUtils)
            .build();

    String toString = context.toString();
    assertFalse(toString.contains("usageOperation"));
    assertFalse(toString.contains("authChannel"));
    assertFalse(toString.contains("usageQuantity"));
  }

  @Test
  public void testTestConstant() {
    RequestContext test = RequestContext.TEST.build();
    assertEquals(test.getRequestID(), "test");
    assertEquals(test.getRequestAPI(), RequestContext.RequestAPI.TEST);
  }

  @Test
  public void testCaptureAPIMetricsForSystemUser() {
    RequestContext.builder()
        .buildRestli(Constants.SYSTEM_ACTOR, null, "test-request")
        .metricUtils(mockMetricUtils)
        .build();

    verify(mockMetricUtils, atLeastOnce())
        .increment(eq("requestContext_system_unknown_restli"), eq(1.0d));
    verify(mockMetricUtils, atLeastOnce())
        .incrementMicrometer(
            eq(MetricUtils.DATAHUB_REQUEST_COUNT),
            eq(1.0d),
            eq("user_category"),
            eq("system"),
            eq("agent_class"),
            eq("unknown"),
            eq("request_api"),
            eq("restli"));
  }

  @Test
  public void testCaptureAPIMetricsForDatahubUser() {
    RequestContext.builder()
        .buildRestli("urn:li:corpuser:testuser", null, "test-request")
        .metricUtils(mockMetricUtils)
        .build();

    verify(mockMetricUtils, atLeastOnce())
        .increment(eq("requestContext_regular_unknown_restli"), eq(1.0d));
    verify(mockMetricUtils, atLeastOnce())
        .incrementMicrometer(
            eq(MetricUtils.DATAHUB_REQUEST_COUNT),
            eq(1.0d),
            eq("user_category"),
            eq("regular"),
            eq("agent_class"),
            eq("unknown"),
            eq("request_api"),
            eq("restli"));
  }

  @Test
  public void testCaptureAPIMetricsForRegularUser() {
    RequestContext.builder()
        .buildRestli("urn:li:corpuser:testuser", null, "test-request")
        .metricUtils(mockMetricUtils)
        .build();

    verify(mockMetricUtils, atLeastOnce())
        .increment(eq("requestContext_regular_unknown_restli"), eq(1.0d));
    verify(mockMetricUtils, atLeastOnce())
        .incrementMicrometer(
            eq(MetricUtils.DATAHUB_REQUEST_COUNT),
            eq(1.0d),
            eq("user_category"),
            eq("regular"),
            eq("agent_class"),
            eq("unknown"),
            eq("request_api"),
            eq("restli"));
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
            .buildRestli("urn:li:corpuser:testuser", emptyUAResourceContext, "test-request")
            .metricUtils(mockMetricUtils)
            .build();

    // Verify that the agentClass is set to "Unknown"
    assertEquals(context.getAgentClass(), AgentClass.UNKNOWN);

    // Verify that a metric was captured with "unknown" agent class
    verify(mockMetricUtils, atLeastOnce())
        .increment(eq("requestContext_regular_unknown_restli"), eq(1.0d));
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
              .buildRestli("urn:li:corpuser:testuser", nullUAResourceContext, "test-request")
              .metricUtils(mockMetricUtils)
              .build();

      // Verify agent class is set to "Unknown" for null user agent
      assertEquals(context.getAgentClass(), AgentClass.UNKNOWN);

      // Verify a metric was captured
      verify(mockMetricUtils, atLeastOnce())
          .increment(eq("requestContext_regular_unknown_restli"), eq(1.0d));
    } catch (NullPointerException e) {
      fail("RequestContext should handle null userAgent gracefully");
    }
  }

  @Test
  public void testAgentClassPopulated() {
    // Create a direct instance of RequestContext with a browser user agent
    RequestContext context =
        new RequestContext(
            mockMetricUtils,
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
    assertNotEquals(context.getAgentClass(), AgentClass.UNKNOWN);
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
            .buildRestli("urn:li:corpuser:testuser", browserUAResourceContext, "test-request")
            .metricUtils(mockMetricUtils)
            .build();

    // Verify the agentClass is populated
    assertNotNull(context.getAgentClass());

    // Verify a metric was captured
    verify(mockMetricUtils, atLeastOnce())
        .increment(eq("requestContext_regular_browser_restli"), eq(1d));
  }

  @Test
  public void testRequestContextWithDataHubSDK() {
    // Set up DataHub SDK user agent
    when(mockHttpRequest.getHeader(HttpHeaders.USER_AGENT))
        .thenReturn("DataHub-Client/1.0.0 (sdk; actions; 1.0.1)");

    RequestContext context =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", null)
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getAgentClass(), AgentClass.SDK);
    assertEquals(context.getAgentName(), "Actions");

    // Verify metrics were captured correctly
    verify(mockMetricUtils, atLeastOnce())
        .increment(eq("requestContext_regular_sdk_graphql"), eq(1.0d));
  }

  @Test
  public void testRequestContextWithDataHubCLI() {
    // Set up DataHub CLI user agent
    when(mockHttpRequest.getHeader(HttpHeaders.USER_AGENT))
        .thenReturn("DataHub-Client/1.0.0 (cli; datahub; 1.0.1)");

    RequestContext context =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", null)
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getAgentClass(), AgentClass.CLI);
    assertEquals(context.getAgentName(), "Datahub");

    // Verify metrics were captured correctly
    verify(mockMetricUtils, atLeastOnce())
        .increment(eq("requestContext_regular_cli_graphql"), eq(1.0d));
  }

  @Test
  public void testRequestContextWithDataHubIngestion() {
    // Set up DataHub INGESTION user agent
    when(mockHttpRequest.getHeader(HttpHeaders.USER_AGENT))
        .thenReturn("DataHub-Client/1.0.0 (ingestion; actions; 1.0.1)");

    RequestContext context =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", null)
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getAgentClass(), AgentClass.INGESTION);
    assertEquals(context.getAgentName(), "Actions");

    // Verify metrics were captured correctly
    verify(mockMetricUtils, atLeastOnce())
        .increment(eq("requestContext_regular_ingestion_graphql"), eq(1.0d));
  }

  @Test
  public void testRequestContextWithDataHubBotPreservesAgentName() {
    when(mockHttpRequest.getHeader(HttpHeaders.USER_AGENT))
        .thenReturn("DataHub-Client/1.0.0 (bot; dh/automation-client; 1.0.1)");

    RequestContext context =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", null)
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getAgentClass(), AgentClass.ROBOT);
    assertEquals(context.getAgentName(), "DH/Automation-Client");
  }

  @Test
  public void testBuildRestliAppliesWireInputFromContentLengthHeader() {
    ResourceContext resourceContext = Mockito.mock(ResourceContext.class);
    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaders.CONTENT_LENGTH, "256");
    when(resourceContext.getRequestHeaders()).thenReturn(headers);
    when(resourceContext.getRawRequestContext())
        .thenReturn(mock(com.linkedin.r2.message.RequestContext.class));
    when(resourceContext.getRawRequestContext().getLocalAttr("REMOTE_ADDR")).thenReturn("10.0.0.2");

    RequestContext context =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", resourceContext, "ingest", "dataset")
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getInputBytes(), Long.valueOf(256));
    assertTrue(context.isRequestBodyMaterialized());
  }

  @Test
  public void testResolveResponseOutputBytesFromContentLength() {
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    when(response.getHeader(HttpHeaders.CONTENT_LENGTH)).thenReturn("42");
    when(response.getHeader(HttpHeaders.TRANSFER_ENCODING)).thenReturn(null);

    assertEquals(RequestContext.resolveResponseOutputBytes(response), Long.valueOf(42));
  }

  @Test
  public void testResolveResponseOutputBytesPrefersMeasuredBytes() {
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    when(response.getHeader(HttpHeaders.CONTENT_LENGTH)).thenReturn("100");
    when(response.getHeader(HttpHeaders.TRANSFER_ENCODING)).thenReturn("chunked");

    assertEquals(RequestContext.resolveResponseOutputBytes(response, 512L), Long.valueOf(512));
  }

  @Test
  public void testResolveResponseOutputBytesNullForChunkedResponse() {
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    when(response.getHeader(HttpHeaders.CONTENT_LENGTH)).thenReturn("100");
    when(response.getHeader(HttpHeaders.TRANSFER_ENCODING)).thenReturn("chunked");

    assertNull(RequestContext.resolveResponseOutputBytes(response));
  }

  @Test
  public void testBuildOpenapiAppliesWireInputFromContentLength() {
    when(mockHttpRequest.getContentLengthLong()).thenReturn(128L);
    when(mockHttpRequest.getHeader(HttpHeaders.TRANSFER_ENCODING)).thenReturn(null);

    RequestContext context =
        RequestContext.builder()
            .buildOpenapi("urn:li:corpuser:testuser", mockHttpRequest, "createEntity", "dataset")
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getInputBytes(), Long.valueOf(128));
    assertTrue(context.isRequestBodyMaterialized());
  }

  @Test
  public void testBuildGraphqlAppliesWireInputFromContentLength() {
    when(mockHttpRequest.getContentLengthLong()).thenReturn(64L);
    when(mockHttpRequest.getHeader(HttpHeaders.TRANSFER_ENCODING)).thenReturn(null);

    RequestContext context =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", null)
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getInputBytes(), Long.valueOf(64));
    assertTrue(context.isRequestBodyMaterialized());
  }

  @Test
  public void testBuildOpenapiSkipsWireInputForChunkedRequest() {
    when(mockHttpRequest.getContentLengthLong()).thenReturn(128L);
    when(mockHttpRequest.getHeader(HttpHeaders.TRANSFER_ENCODING)).thenReturn("chunked");

    RequestContext context =
        RequestContext.builder()
            .buildOpenapi("urn:li:corpuser:testuser", mockHttpRequest, "createEntity", "dataset")
            .metricUtils(mockMetricUtils)
            .build();

    assertNull(context.getInputBytes());
    assertFalse(context.isRequestBodyMaterialized());
  }

  @Test
  public void testMaterializedInputUtf8BytesFillsChunkedWireGap() {
    when(mockHttpRequest.getContentLengthLong()).thenReturn(128L);
    when(mockHttpRequest.getHeader(HttpHeaders.TRANSFER_ENCODING)).thenReturn("chunked");

    String body = "[{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:file,chunked,PROD)\"}]";
    RequestContext context =
        RequestContext.builder()
            .buildOpenapi("urn:li:corpuser:testuser", mockHttpRequest, "createEntity", "dataset")
            .withMaterializedInputUtf8Bytes(body)
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(
        context.getInputBytes(),
        Long.valueOf(body.getBytes(java.nio.charset.StandardCharsets.UTF_8).length));
    assertTrue(context.isRequestBodyMaterialized());
  }

  @Test
  public void testMaterializedInputUtf8BytesDoesNotOverrideContentLength() {
    when(mockHttpRequest.getContentLengthLong()).thenReturn(128L);
    when(mockHttpRequest.getHeader(HttpHeaders.TRANSFER_ENCODING)).thenReturn(null);

    RequestContext context =
        RequestContext.builder()
            .buildOpenapi("urn:li:corpuser:testuser", mockHttpRequest, "createEntity", "dataset")
            .withMaterializedInputUtf8Bytes("much-longer-body-than-content-length-header")
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getInputBytes(), Long.valueOf(128));
    assertTrue(context.isRequestBodyMaterialized());
  }

  @Test
  public void testMetricsCapturingWithDataHubAgents() {
    // Test that metrics are captured correctly for DataHub agents
    when(mockHttpRequest.getHeader(HttpHeaders.USER_AGENT))
        .thenReturn("DataHub-Client/1.0.0 (sdk; datahub; 1.0.1)");

    RequestContext context =
        RequestContext.builder()
            .buildGraphql("urn:li:corpuser:testuser", mockHttpRequest, "GetUserQuery", null)
            .metricUtils(mockMetricUtils)
            .build();

    // Verify the counter was incremented with the appropriate metric name
    // The metric name should contain the lowercase agent class
    verify(mockMetricUtils, times(1)).increment(matches(".*sdk.*"), eq(1d));
  }

  @Test
  public void testResolveIngestUsageQuantityFromJsonArray() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    assertEquals(RequestContext.resolveIngestUsageQuantity("[1,2,3]", mapper), 3L);
    assertEquals(RequestContext.resolveIngestUsageQuantity("{\"a\":1}", mapper), 1L);
    assertEquals(RequestContext.resolveIngestUsageQuantity("not-json", mapper), 1L);
  }

  @Test
  public void testResolveIngestUsageQuantityFromObjectNode() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root =
        mapper.readTree(
            "{\"dataset\":[{\"urn\":\"a\"},{\"urn\":\"b\"}],\"chart\":{\"urn\":\"c\"}}");
    assertEquals(RequestContext.resolveIngestUsageQuantity(root), 3L);
    assertEquals(RequestContext.resolveIngestUsageQuantity(mapper.readTree("[1]")), 1L);
  }

  @Test
  public void testUsageMdcAndClear() {
    RequestContext.builder()
        .buildRestli("urn:li:corpuser:testuser", null, "test-request")
        .withUsageOperation(UsageOperation.METADATA_WRITE)
        .authChannel(AuthChannel.PAT)
        .withUsageQuantity(4)
        .metricUtils(mockMetricUtils)
        .build();

    assertEquals(org.slf4j.MDC.get(RequestContext.MDC_USAGE_OPERATION), "metadata_write");
    assertEquals(org.slf4j.MDC.get(RequestContext.MDC_AUTH_CHANNEL), "pat");
    assertEquals(org.slf4j.MDC.get(RequestContext.MDC_USAGE_QUANTITY), "4");

    RequestContext.clearUsageFieldsFromMdc();
    assertNull(org.slf4j.MDC.get(RequestContext.MDC_USAGE_OPERATION));
    assertNull(org.slf4j.MDC.get(RequestContext.MDC_AUTH_CHANNEL));
    assertNull(org.slf4j.MDC.get(RequestContext.MDC_USAGE_QUANTITY));
  }

  @Test
  public void testAuthChannelDimensionValue() {
    assertEquals(AuthChannel.SESSION.dimensionValue(), "session");
    assertEquals(AuthChannel.UNKNOWN.dimensionValue(), "unknown");
  }

  @Test
  public void testWithMaterializedOutputBytes() {
    RequestContext context =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", null, "test-request")
            .withMaterializedOutputBytes(99L)
            .metricUtils(mockMetricUtils)
            .build();

    assertEquals(context.getOutputBytes(), Long.valueOf(99));
    assertTrue(context.isResponseBodyMaterialized());
  }

  @Test
  public void testPeekInputBytesAndUsageQuantityClamp() {
    RequestContext.RequestContextBuilder builder =
        RequestContext.builder()
            .buildRestli("urn:li:corpuser:testuser", null, "test-request")
            .withUsageQuantity(0);
    assertNull(builder.peekInputBytes());
    assertEquals(builder.peekUsageOperation(), null);

    RequestContext context = builder.metricUtils(mockMetricUtils).build();
    assertEquals(context.getUsageQuantity(), 1L);
  }
}
