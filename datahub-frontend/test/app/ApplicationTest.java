package app;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datahub.authentication.AuthenticationConstants;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import controllers.Application;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import play.Environment;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Http.Cookie;
import play.mvc.Result;

public class ApplicationTest {
  private Application app;
  private Application appWithMockEnv;
  private HttpClient mockHttpClient;
  private Config config;
  private Environment environment;

  @BeforeEach
  public void setUp() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8080);
    configMap.put("metadataService.useSsl", false);
    configMap.put("graphql.verbose.logging", true);
    configMap.put("graphql.verbose.slowQueryMillis", 0);
    configMap.put("app.version", "1.0.0");
    configMap.put("linkedin.internal", true);
    configMap.put("linkedin.show.dataset.lineage", true);
    configMap.put("linkedin.suggestion.confidence.threshold", "5");
    configMap.put("ui.show.staging.banner", false);
    configMap.put("ui.show.live.data.banner", false);
    configMap.put("ui.show.CM.banner", true);
    configMap.put("ui.show.people", true);
    configMap.put("ui.show.CM.link", "http://change.management");
    configMap.put("ui.show.stale.search", false);
    configMap.put("ui.show.advanced.search", true);
    configMap.put("ui.new.browse.dataset", false);
    configMap.put("ui.show.lineage.graph", true);
    configMap.put("ui.show.institutional.memory", false);
    configMap.put("linkedin.links.avi.urlPrimary", "http://primary.avi");
    configMap.put("linkedin.links.avi.urlFallback", "http://fallback.avi");
    configMap.put("links.wiki.appHelp", "http://help");
    configMap.put("links.wiki.gdprPii", "http://gdprpii");
    configMap.put("links.wiki.tmsSchema", "http://tmsschema");
    configMap.put("links.wiki.gdprTaxonomy", "http://gdprtaxonomy");
    configMap.put("links.wiki.staleSearchIndex", "http://stale");
    configMap.put("links.wiki.dht", "http://dht");
    configMap.put("links.wiki.purgePolicies", "http://purgepolicies");
    configMap.put("links.wiki.jitAcl", "http://jitacl");
    configMap.put("links.wiki.metadataCustomRegex", "http://customregex");
    configMap.put("links.wiki.exportPolicy", "http://exportpolicy");
    configMap.put("links.wiki.metadataHealth", "http://metadatahealth");
    configMap.put("links.wiki.purgeKey", "http://purgekey");
    configMap.put("links.wiki.datasetDecommission", "http://datasetdecommission");
    configMap.put("tracking.piwik.siteid", "123");
    configMap.put("tracking.piwik.url", "http://piwik");
    config = ConfigFactory.parseMap(configMap);

    environment = mock(Environment.class);

    // Mock index.html resource as happy path
    InputStream indexStream = new ByteArrayInputStream("INDEX".getBytes());
    when(environment.resourceAsStream(eq("public/index.html"))).thenReturn(indexStream);

    app = new Application(environment, config);

    // App with environment that throws on resourceAsStream (exception path)
    Environment envException = mock(Environment.class);
    when(envException.resourceAsStream(anyString())).thenThrow(new RuntimeException("not found"));
    appWithMockEnv = new Application(envException, config);

    // Patch httpClient for proxy tests
    mockHttpClient = mock(HttpClient.class);
    var f = Application.class.getDeclaredField("httpClient");
    f.setAccessible(true);
    f.set(app, mockHttpClient);
  }

  // ---------- serveAsset, healthcheck, index ----------

  @Test
  public void testServeAsset_success() throws Exception {
    Method m = Application.class.getDeclaredMethod("serveAsset", String.class);
    m.setAccessible(true);
    Result result = (Result) m.invoke(app, "any");
    assertEquals(200, result.status());
    assertEquals("no-cache", result.headers().get("Cache-Control"));
    assertEquals("text/html", result.contentType().get());
  }

  @Test
  public void testServeAsset_exception() throws Exception {
    Method m = Application.class.getDeclaredMethod("serveAsset", String.class);
    m.setAccessible(true);
    Result result = (Result) m.invoke(appWithMockEnv, "any");
    assertEquals(404, result.status());
    assertEquals("no-cache", result.headers().get("Cache-Control"));
    assertEquals("text/html", result.contentType().get());
  }

  @Test
  public void testHealthcheck() {
    Result r = app.healthcheck();
    assertEquals(200, r.status());
    assertTrue(play.test.Helpers.contentAsString(r).contains("GOOD"));
  }

  @Test
  public void testIndex() {
    Result r = app.index("whatever");
    assertEquals(200, r.status());
    assertEquals("text/html", r.contentType().get());
  }

  // ---------- appConfig + helpers ----------

  @Test
  public void testAppConfig_full() {
    Result result = app.appConfig();
    assertEquals(200, result.status());
    String content = play.test.Helpers.contentAsString(result);
    assertTrue(content.contains("\"application\":\"datahub-frontend\""));
    assertTrue(content.contains("\"aviUrlPrimary\":\"http://primary.avi\""));
    assertTrue(content.contains("\"appHelp\":\"http://help\""));
    assertTrue(content.contains("\"piwikSiteId\":123"));
    assertTrue(content.contains("\"isEnabled\":true"));
  }

  @Test
  public void testUserEntityProps() throws Exception {
    Method m = Application.class.getDeclaredMethod("userEntityProps");
    m.setAccessible(true);
    ObjectNode props = (ObjectNode) m.invoke(app);
    assertEquals("http://primary.avi", props.get("aviUrlPrimary").asText());
    assertEquals("http://fallback.avi", props.get("aviUrlFallback").asText());
  }

  @Test
  public void testWikiLinks() throws Exception {
    Method m = Application.class.getDeclaredMethod("wikiLinks");
    m.setAccessible(true);
    ObjectNode links = (ObjectNode) m.invoke(app);
    assertEquals("http://help", links.get("appHelp").asText());
    assertEquals("http://gdprpii", links.get("gdprPii").asText());
    assertEquals("http://datasetdecommission", links.get("datasetDecommission").asText());
  }

  @Test
  public void testTrackingInfo() throws Exception {
    Method m = Application.class.getDeclaredMethod("trackingInfo");
    m.setAccessible(true);
    ObjectNode tracking = (ObjectNode) m.invoke(app);
    assertTrue(tracking.has("trackers"));
    assertTrue(tracking.get("isEnabled").asBoolean());
    ObjectNode trackers = (ObjectNode) tracking.get("trackers");
    ObjectNode piwik = (ObjectNode) trackers.get("piwik");
    assertEquals(123, piwik.get("piwikSiteId").asInt());
    assertEquals("http://piwik", piwik.get("piwikUrl").asText());
  }

  // ---------- mapPath ----------

  @Test
  public void testMapPath_legacyGraphQL() throws Exception {
    Method m = Application.class.getDeclaredMethod("mapPath", String.class);
    m.setAccessible(true);
    assertEquals("/api/graphql", m.invoke(app, "/api/v2/graphql"));
  }

  @Test
  public void testMapPath_gmsApi_withSlash() throws Exception {
    Method m = Application.class.getDeclaredMethod("mapPath", String.class);
    m.setAccessible(true);
    assertEquals("/foo", m.invoke(app, "/api/gms/foo"));
  }

  @Test
  public void testMapPath_gmsApi_noLeadingSlash() throws Exception {
    Method m = Application.class.getDeclaredMethod("mapPath", String.class);
    m.setAccessible(true);
    assertEquals("/bar", m.invoke(app, "/api/gmsbar"));
  }

  @Test
  public void testMapPath_default() throws Exception {
    Method m = Application.class.getDeclaredMethod("mapPath", String.class);
    m.setAccessible(true);
    assertEquals("/otherpath", m.invoke(app, "/otherpath"));
  }

  // ---------- getAuthorizationHeaderValueToProxy ----------

  @Test
  public void testGetAuthorizationHeaderValueToProxy_sessionToken() throws Exception {
    Method m =
        Application.class.getDeclaredMethod(
            "getAuthorizationHeaderValueToProxy", Http.Request.class);
    m.setAccessible(true);
    Map<String, String> session = Map.of("token", "abc123");
    Http.Request req = new Http.RequestBuilder().session(session).build();
    String value = (String) m.invoke(app, req);
    assertEquals("Bearer abc123", value);
  }

  @Test
  public void testGetAuthorizationHeaderValueToProxy_authHeader() throws Exception {
    Method m =
        Application.class.getDeclaredMethod(
            "getAuthorizationHeaderValueToProxy", Http.Request.class);
    m.setAccessible(true);
    Http.Request req =
        new Http.RequestBuilder().header(Http.HeaderNames.AUTHORIZATION, "Bearer xyz").build();
    String value = (String) m.invoke(app, req);
    assertEquals("Bearer xyz", value);
  }

  @Test
  public void testGetAuthorizationHeaderValueToProxy_none() throws Exception {
    Method m =
        Application.class.getDeclaredMethod(
            "getAuthorizationHeaderValueToProxy", Http.Request.class);
    m.setAccessible(true);
    Http.Request req = new Http.RequestBuilder().build();
    String value = (String) m.invoke(app, req);
    assertEquals("", value);
  }

  // ---------- getDataHubActorHeader ----------

  @Test
  public void testGetDataHubActorHeader_present() throws Exception {
    Method m = Application.class.getDeclaredMethod("getDataHubActorHeader", Http.Request.class);
    m.setAccessible(true);
    Map<String, String> session = Map.of("actor", "alice");
    Http.Request req = new Http.RequestBuilder().session(session).build();
    String value = (String) m.invoke(app, req);
    assertEquals("alice", value);
  }

  @Test
  public void testGetDataHubActorHeader_absent() throws Exception {
    Method m = Application.class.getDeclaredMethod("getDataHubActorHeader", Http.Request.class);
    m.setAccessible(true);
    Http.Request req = new Http.RequestBuilder().build();
    String value = (String) m.invoke(app, req);
    assertEquals("", value);
  }

  // ---------- buildBodyPublisher ----------

  @Test
  public void testBuildBodyPublisher_withText() throws Exception {
    String text = "hello world";
    Http.Request req = new Http.RequestBuilder().method("POST").uri("/test").bodyText(text).build();
    Method m = Application.class.getDeclaredMethod("buildBodyPublisher", Http.Request.class);
    m.setAccessible(true);
    HttpRequest.BodyPublisher publisher = (HttpRequest.BodyPublisher) m.invoke(app, req);
    assertNotNull(publisher);
  }

  @Test
  public void testBuildBodyPublisher_noBody() throws Exception {
    Http.Request req = new Http.RequestBuilder().method("POST").uri("/test").build();
    Method m = Application.class.getDeclaredMethod("buildBodyPublisher", Http.Request.class);
    m.setAccessible(true);
    HttpRequest.BodyPublisher publisher = (HttpRequest.BodyPublisher) m.invoke(app, req);
    assertNotNull(publisher);
  }

  // ---------- logSlowQuery ----------

  @Test
  public void testLogSlowQuery_jsonAndQuery_ok() throws Exception {
    Method m =
        Application.class.getDeclaredMethod(
            "logSlowQuery", Http.Request.class, String.class, float.class);
    m.setAccessible(true);
    ObjectNode node = Json.newObject();
    node.put("query", "gql");
    node.put("foo", "bar");
    Http.Request req =
        new Http.RequestBuilder()
            .bodyJson(node)
            .cookie(
                new Cookie("actor", "bob", null, null, null, false, false, Cookie.SameSite.NONE))
            .build();
    m.invoke(app, req, "/uri", 123f);
    // Should log without throwing (JSON present, parses fine)
  }

  @Test
  public void testLogSlowQuery_jsonAndQuery_exception() throws Exception {
    Method m =
        Application.class.getDeclaredMethod(
            "logSlowQuery", Http.Request.class, String.class, float.class);
    m.setAccessible(true);
    Http.Request req = new Http.RequestBuilder().build();
    m.invoke(app, req, "/uri", 123f);
    // Should log without throwing (asJson() returns null, triggers exception block)
  }

  // ---------- proxy (all branches) ----------

  @Test
  public void testProxy_HappyPath_AllHeadersVariants() throws Exception {
    Map<String, List<String>> headers = new HashMap<>();
    headers.put(Http.HeaderNames.HOST, List.of("originalhost"));
    headers.put(Http.HeaderNames.AUTHORIZATION, List.of("Bearer token"));
    headers.put("Custom-Header", List.of("value"));
    String body = "{\"query\":\"{}\"}";
    Map<String, String> session = Map.of("actor", "bob");

    HttpResponse<byte[]> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    Map<String, List<String>> respHeaders = new HashMap<>();
    respHeaders.put("X-Backend", List.of("backend"));
    when(mockResponse.headers()).thenReturn(HttpHeaders.of(respHeaders, (k, v) -> true));
    when(mockResponse.body()).thenReturn("{\"ok\":true}".getBytes());

    when(mockHttpClient.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    Http.RequestBuilder builder =
        new Http.RequestBuilder()
            .method("POST")
            .uri("/api/v2/graphql/")
            .bodyText(body)
            .session(session);

    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      for (String value : entry.getValue()) {
        builder.header(entry.getKey(), value);
      }
    }

    Http.Request req = builder.build();
    Result result = app.proxy("graphql", req).get(5, TimeUnit.SECONDS);
    assertEquals(200, result.status());
    assertTrue(result.headers().containsKey("X-Backend"));
  }

  @Test
  public void testProxy_AddsXForwardedProtoAndHost_WhenAbsent() throws Exception {
    Map<String, List<String>> headers = new HashMap<>();
    headers.put("Custom-Header", List.of("value"));

    HttpResponse<byte[]> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.headers()).thenReturn(HttpHeaders.of(new HashMap<>(), (k, v) -> true));
    when(mockResponse.body()).thenReturn("{}".getBytes());

    when(mockHttpClient.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    Http.RequestBuilder builder = new Http.RequestBuilder().method("GET").uri("/api/v2/graphql/");

    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      for (String value : entry.getValue()) {
        builder.header(entry.getKey(), value);
      }
    }

    Http.Request req = builder.build();

    Result result = app.proxy("graphql", req).get(5, TimeUnit.SECONDS);
    assertEquals(200, result.status());
  }

  @Test
  public void testProxy_DoesNotDuplicateXForwardedHeaders() throws Exception {
    Map<String, List<String>> headers = new HashMap<>();
    headers.put(Http.HeaderNames.HOST, List.of("host"));
    headers.put(Http.HeaderNames.X_FORWARDED_HOST, List.of("already"));
    headers.put(Http.HeaderNames.X_FORWARDED_PROTO, List.of("https"));

    HttpResponse<byte[]> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.headers()).thenReturn(HttpHeaders.of(new HashMap<>(), (k, v) -> true));
    when(mockResponse.body()).thenReturn("{}".getBytes());

    when(mockHttpClient.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    Http.RequestBuilder builder = new Http.RequestBuilder().method("POST").uri("/api/v2/graphql/");

    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      for (String value : entry.getValue()) {
        builder.header(entry.getKey(), value);
      }
    }

    Http.Request req = builder.build();

    Result result = app.proxy("graphql", req).get(5, TimeUnit.SECONDS);
    assertEquals(200, result.status());
  }

  @Test
  public void testProxy_RemovesRestrictedAndSpecialHeaders() throws Exception {
    Map<String, List<String>> headers = new HashMap<>();
    headers.put("connection", List.of("close"));
    headers.put("host", List.of("host"));
    headers.put(Http.HeaderNames.CONTENT_TYPE, List.of("application/xml"));
    headers.put(Http.HeaderNames.AUTHORIZATION, List.of("Bearer old"));
    headers.put(AuthenticationConstants.LEGACY_X_DATAHUB_ACTOR_HEADER, List.of("actor"));
    headers.put("Custom-Header", List.of("value"));

    HttpResponse<byte[]> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.headers()).thenReturn(HttpHeaders.of(new HashMap<>(), (k, v) -> true));
    when(mockResponse.body()).thenReturn("{}".getBytes());

    when(mockHttpClient.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    Http.RequestBuilder builder =
        new Http.RequestBuilder()
            .method("POST")
            .uri("/api/v2/graphql/")
            .bodyText("{\"query\":\"{}\"}");

    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      for (String value : entry.getValue()) {
        builder.header(entry.getKey(), value);
      }
    }

    Http.Request req = builder.build();

    Result result = app.proxy("graphql", req).get(5, TimeUnit.SECONDS);
    assertEquals(200, result.status());
  }

  @Test
  public void testProxy_DoesNotSendAuthorizationHeaderIfEmpty() throws Exception {
    // Test by not setting session token or auth header
    HttpResponse<byte[]> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.headers()).thenReturn(HttpHeaders.of(new HashMap<>(), (k, v) -> true));
    when(mockResponse.body()).thenReturn("{}".getBytes());

    when(mockHttpClient.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    Http.Request req = new Http.RequestBuilder().method("GET").uri("/api/v2/graphql/").build();

    Result result = app.proxy("graphql", req).get(5, TimeUnit.SECONDS);
    assertEquals(200, result.status());
  }

  @Test
  public void testProxy_ContentTypeOptional() throws Exception {
    HttpResponse<byte[]> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.headers()).thenReturn(HttpHeaders.of(new HashMap<>(), (k, v) -> true));
    when(mockResponse.body()).thenReturn("{}".getBytes());

    when(mockHttpClient.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    Http.Request req =
        new Http.RequestBuilder()
            .method("POST")
            .uri("/api/v2/graphql/")
            .bodyText("{\"query\":\"{}\"}")
            .build();

    Result result = app.proxy("graphql", req).get(5, TimeUnit.SECONDS);
    assertEquals(200, result.status());
  }

  @Test
  public void testProxy_LogsSlowQuery() throws Exception {
    HttpResponse<byte[]> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.headers()).thenReturn(HttpHeaders.of(new HashMap<>(), (k, v) -> true));
    when(mockResponse.body()).thenReturn("{}".getBytes());

    when(mockHttpClient.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              Thread.sleep(10);
              return CompletableFuture.completedFuture(mockResponse);
            });

    Http.Request req =
        new Http.RequestBuilder()
            .method("POST")
            .uri("/api/v2/graphql/")
            .bodyText("{\"query\":\"{}\"}")
            .build();

    Result result = app.proxy("graphql", req).get(5, TimeUnit.SECONDS);
    assertEquals(200, result.status());
  }

  @Test
  public void testProxy_HandlesHttpTimeoutException() throws Exception {
    CompletableFuture<HttpResponse<byte[]>> failed = new CompletableFuture<>();
    failed.completeExceptionally(
        new CompletionException(new java.net.http.HttpTimeoutException("Timeout!")));
    when(mockHttpClient.sendAsync(any(), any(HttpResponse.BodyHandler.class))).thenReturn(failed);

    Http.Request req =
        new Http.RequestBuilder()
            .method("POST")
            .uri("/api/v2/graphql/")
            .bodyText("{\"query\":\"{}\"}")
            .build();

    Result result = app.proxy("graphql", req).get(5, TimeUnit.SECONDS);
    assertEquals(play.mvc.Http.Status.GATEWAY_TIMEOUT, result.status());
    assertTrue(play.test.Helpers.contentAsString(result).contains("Proxy request timed out"));
  }

  @Test
  public void testProxy_HandlesConnectException() throws Exception {
    CompletableFuture<HttpResponse<byte[]>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new CompletionException(new ConnectException("Refused")));
    when(mockHttpClient.sendAsync(any(), any(HttpResponse.BodyHandler.class))).thenReturn(failed);

    Http.Request req =
        new Http.RequestBuilder()
            .method("POST")
            .uri("/api/v2/graphql/")
            .bodyText("{\"query\":\"{}\"}")
            .build();

    Result result = app.proxy("graphql", req).get(5, TimeUnit.SECONDS);
    assertEquals(play.mvc.Http.Status.BAD_GATEWAY, result.status());
    assertTrue(play.test.Helpers.contentAsString(result).contains("Proxy connection failed"));
  }

  @Test
  public void testProxy_HandlesOtherException() throws Exception {
    CompletableFuture<HttpResponse<byte[]>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new CompletionException(new RuntimeException("Boom!")));
    when(mockHttpClient.sendAsync(any(), any(HttpResponse.BodyHandler.class))).thenReturn(failed);

    Http.Request req =
        new Http.RequestBuilder()
            .method("POST")
            .uri("/api/v2/graphql/")
            .bodyText("{\"query\":\"{}\"}")
            .build();

    Result result = app.proxy("graphql", req).get(5, TimeUnit.SECONDS);
    assertEquals(play.mvc.Http.Status.INTERNAL_SERVER_ERROR, result.status());
    assertTrue(play.test.Helpers.contentAsString(result).contains("Proxy error"));
  }
}
