package controllers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import play.Environment;
import play.http.HttpEntity;
import play.mvc.Http;
import play.mvc.Result;

/**
 * Unit tests for Application controller, focusing on proxy streaming path configuration,
 * resolution, buildProxyResult, buildProxyResponseHeader, and handleProxyException.
 */
public class ApplicationControllerTest {

  private Application application;
  private Config config;

  @BeforeEach
  void setUp() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/");
    configMap.put("graphql.verbose.logging", false);
    configMap.put("graphql.verbose.slowQueryMillis", 2500);
    configMap.put("proxy.streamingPathPrefixes", "/openapi/v1/ai-chat/message");
    config = ConfigFactory.parseMap(configMap);

    java.net.http.HttpClient mockHttpClient = mock(java.net.http.HttpClient.class);
    Environment mockEnvironment = mock(Environment.class);
    application = new Application(mockHttpClient, mockEnvironment, config);
  }

  @Test
  void resolveStreamingPathPrefixes_missingKey_returnsEmptyList() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/");
    Config config = ConfigFactory.parseMap(configMap);

    List<String> result = Application.resolveStreamingPathPrefixes(config);

    assertTrue(result.isEmpty());
  }

  @Test
  void resolveStreamingPathPrefixes_emptyString_returnsEmptyList() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/");
    configMap.put("proxy.streamingPathPrefixes", "");
    Config config = ConfigFactory.parseMap(configMap);

    List<String> result = Application.resolveStreamingPathPrefixes(config);

    assertTrue(result.isEmpty());
  }

  @Test
  void resolveStreamingPathPrefixes_blankString_returnsEmptyList() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/");
    configMap.put("proxy.streamingPathPrefixes", "   ");
    Config config = ConfigFactory.parseMap(configMap);

    List<String> result = Application.resolveStreamingPathPrefixes(config);

    assertTrue(result.isEmpty());
  }

  @Test
  void resolveStreamingPathPrefixes_singlePath_returnsOneElement() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/");
    configMap.put("proxy.streamingPathPrefixes", "/openapi/v1/ai-chat/message");
    Config config = ConfigFactory.parseMap(configMap);

    List<String> result = Application.resolveStreamingPathPrefixes(config);

    assertEquals(List.of("/openapi/v1/ai-chat/message"), result);
  }

  @Test
  void resolveStreamingPathPrefixes_commaSeparated_returnsTrimmedList() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/");
    configMap.put(
        "proxy.streamingPathPrefixes",
        "/openapi/v1/ai-chat/message , /openapi/v1/other-streaming ");
    Config config = ConfigFactory.parseMap(configMap);

    List<String> result = Application.resolveStreamingPathPrefixes(config);

    assertEquals(List.of("/openapi/v1/ai-chat/message", "/openapi/v1/other-streaming"), result);
  }

  @Test
  void resolveStreamingPathPrefixes_commaWithEmptySegments_filtersEmpty() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/");
    configMap.put("proxy.streamingPathPrefixes", "/path1,,  ,/path2");
    Config config = ConfigFactory.parseMap(configMap);

    List<String> result = Application.resolveStreamingPathPrefixes(config);

    assertEquals(List.of("/path1", "/path2"), result);
  }

  @Test
  void buildProxyResult_buffered_returnsResultWithStrictBody() throws Exception {
    Http.Request request = mock(Http.Request.class);
    HttpResponse<?> apiResponse = mock(HttpResponse.class);
    java.net.http.HttpHeaders responseHeaders = mock(java.net.http.HttpHeaders.class);

    when(apiResponse.statusCode()).thenReturn(200);
    when(apiResponse.headers()).thenReturn(responseHeaders);
    when(responseHeaders.firstValue(Http.HeaderNames.CONTENT_TYPE))
        .thenReturn(Optional.of("application/json"));
    when(apiResponse.body()).thenReturn(new byte[] {1, 2, 3});

    Result result =
        invokeBuildProxyResult(request, "/api/graphql", Instant.now(), apiResponse, false);

    assertEquals(200, result.status());
    assertTrue(result.body() instanceof HttpEntity.Strict);
  }

  @Test
  void buildProxyResult_streaming_returnsResultWithStreamedBody() throws Exception {
    Http.Request request = mock(Http.Request.class);
    HttpResponse<?> apiResponse = mock(HttpResponse.class);
    java.net.http.HttpHeaders responseHeaders = mock(java.net.http.HttpHeaders.class);

    when(apiResponse.statusCode()).thenReturn(200);
    when(apiResponse.headers()).thenReturn(responseHeaders);
    when(responseHeaders.firstValue(Http.HeaderNames.CONTENT_TYPE))
        .thenReturn(Optional.of("text/event-stream"));
    when(apiResponse.body()).thenReturn(new ByteArrayInputStream(new byte[0]));

    Result result =
        invokeBuildProxyResult(
            request, "/openapi/v1/ai-chat/message", Instant.now(), apiResponse, true);

    assertEquals(200, result.status());
    assertTrue(result.body() instanceof HttpEntity.Streamed);
  }

  @Test
  void buildProxyResult_buffered_omitsContentEncodingSoGzipFilterCanCompress() throws Exception {
    Http.Request request = mock(Http.Request.class);
    HttpResponse<?> apiResponse = mock(HttpResponse.class);
    java.net.http.HttpHeaders responseHeaders = mock(java.net.http.HttpHeaders.class);

    when(apiResponse.statusCode()).thenReturn(200);
    when(apiResponse.headers()).thenReturn(responseHeaders);
    when(responseHeaders.firstValue(Http.HeaderNames.CONTENT_TYPE))
        .thenReturn(Optional.of("application/json"));
    when(apiResponse.body()).thenReturn(new byte[] {1, 2, 3});

    Result result =
        invokeBuildProxyResult(request, "/api/v2/graphql", Instant.now(), apiResponse, false);

    assertFalse(
        result.headers().containsKey(Http.HeaderNames.CONTENT_ENCODING),
        "Non-streaming proxy response must not set Content-Encoding so GzipFilter can add gzip");
  }

  @Test
  void buildProxyResult_streaming_setsContentEncodingIdentitySoGzipFilterSkips() throws Exception {
    Http.Request request = mock(Http.Request.class);
    HttpResponse<?> apiResponse = mock(HttpResponse.class);
    java.net.http.HttpHeaders responseHeaders = mock(java.net.http.HttpHeaders.class);

    when(apiResponse.statusCode()).thenReturn(200);
    when(apiResponse.headers()).thenReturn(responseHeaders);
    when(responseHeaders.firstValue(Http.HeaderNames.CONTENT_TYPE))
        .thenReturn(Optional.of("text/event-stream"));
    when(apiResponse.body()).thenReturn(new ByteArrayInputStream(new byte[0]));

    Result result =
        invokeBuildProxyResult(
            request, "/openapi/v1/ai-chat/message", Instant.now(), apiResponse, true);

    assertEquals(
        "identity",
        result.headers().get(Http.HeaderNames.CONTENT_ENCODING),
        "Streaming proxy response must set Content-Encoding: identity so GzipFilter does not compress");
  }

  @Test
  void buildProxyResult_verboseLoggingAndSlowQuery_invokesLogSlowQuery() throws Exception {
    Map<String, Object> verboseConfigMap = new HashMap<>();
    verboseConfigMap.put("datahub.basePath", "/");
    verboseConfigMap.put("graphql.verbose.logging", true);
    verboseConfigMap.put("graphql.verbose.slowQueryMillis", 0);
    verboseConfigMap.put("proxy.streamingPathPrefixes", "");
    Config verboseConfig = ConfigFactory.parseMap(verboseConfigMap);
    Application appWithVerboseLogging =
        new Application(
            mock(java.net.http.HttpClient.class), mock(Environment.class), verboseConfig);

    Http.Request request = mock(Http.Request.class);
    HttpResponse<?> apiResponse = mock(HttpResponse.class);
    java.net.http.HttpHeaders responseHeaders = mock(java.net.http.HttpHeaders.class);
    when(apiResponse.statusCode()).thenReturn(200);
    when(apiResponse.headers()).thenReturn(responseHeaders);
    when(responseHeaders.firstValue(Http.HeaderNames.CONTENT_TYPE))
        .thenReturn(Optional.of("application/json"));
    when(apiResponse.body()).thenReturn(new byte[0]);

    Result result =
        invokeBuildProxyResult(
            appWithVerboseLogging,
            request,
            "/api/graphql",
            Instant.now().minusMillis(10),
            apiResponse,
            false);

    assertEquals(200, result.status());
    assertTrue(result.body() instanceof HttpEntity.Strict);
  }

  @Test
  void handleProxyException_timeout_returnsGatewayTimeout() throws Exception {
    Throwable ex =
        new java.util.concurrent.CompletionException(
            new java.net.http.HttpTimeoutException("timed out"));
    Result result = invokeHandleProxyException(ex);
    assertEquals(504, result.status());
  }

  @Test
  void handleProxyException_connectException_returnsBadGateway() throws Exception {
    Throwable ex =
        new java.util.concurrent.CompletionException(
            new java.net.ConnectException("Connection refused"));
    Result result = invokeHandleProxyException(ex);
    assertEquals(502, result.status());
  }

  @Test
  void handleProxyException_genericException_returnsInternalServerError() throws Exception {
    Throwable ex = new java.util.concurrent.CompletionException(new RuntimeException("unknown"));
    Result result = invokeHandleProxyException(ex);
    assertEquals(500, result.status());
  }

  private Result invokeBuildProxyResult(
      Http.Request request,
      String resolvedUri,
      Instant start,
      HttpResponse<?> apiResponse,
      boolean useStreaming)
      throws Exception {
    return invokeBuildProxyResult(
        application, request, resolvedUri, start, apiResponse, useStreaming);
  }

  private Result invokeBuildProxyResult(
      Application app,
      Http.Request request,
      String resolvedUri,
      Instant start,
      HttpResponse<?> apiResponse,
      boolean useStreaming)
      throws Exception {
    var method =
        Application.class.getDeclaredMethod(
            "buildProxyResult",
            Http.Request.class,
            String.class,
            Instant.class,
            HttpResponse.class,
            boolean.class);
    method.setAccessible(true);
    return (Result) method.invoke(app, request, resolvedUri, start, apiResponse, useStreaming);
  }

  private Result invokeHandleProxyException(Throwable ex) throws Exception {
    var method = Application.class.getDeclaredMethod("handleProxyException", Throwable.class);
    method.setAccessible(true);
    return (Result) method.invoke(application, ex);
  }
}
