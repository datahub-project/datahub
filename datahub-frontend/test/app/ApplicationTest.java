package app;

import static org.junit.jupiter.api.Assertions.*;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.fakeRequest;
import static play.test.Helpers.route;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import controllers.Application;
import controllers.routes;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.http.OAuth2HttpRequest;
import no.nav.security.mock.oauth2.http.OAuth2HttpResponse;
import no.nav.security.mock.oauth2.http.Route;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;
import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.Environment;
import play.Mode;
import play.inject.guice.GuiceApplicationBuilder;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;
import play.test.TestBrowser;
import play.test.WithBrowser;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SetEnvironmentVariable(key = "DATAHUB_SECRET", value = "test")
@SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVER", value = "")
@SetEnvironmentVariable(key = "DATAHUB_ANALYTICS_ENABLED", value = "false")
@SetEnvironmentVariable(key = "AUTH_OIDC_ENABLED", value = "true")
@SetEnvironmentVariable(key = "AUTH_OIDC_JIT_PROVISIONING_ENABLED", value = "false")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_ID", value = "testclient")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_SECRET", value = "testsecret")
@SetEnvironmentVariable(key = "AUTH_VERBOSE_LOGGING", value = "true")
public class ApplicationTest extends WithBrowser {
  private static final Logger logger = LoggerFactory.getLogger(ApplicationTest.class);
  private static final String ISSUER_ID = "testIssuer";

  // For unit tests on controller methods
  private Application unitApp;

  @BeforeEach
  public void unitSetup() {
    Map<String, Object> configMap = new HashMap<>();
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
    Config config = ConfigFactory.parseMap(configMap);
    Environment env = Environment.simple();
    unitApp = new Application(env, config);
  }

  // --- Play/Browser/Integration Test Section ---

  @Override
  protected play.Application provideApplication() {
    return new GuiceApplicationBuilder()
        .configure("metadataService.port", String.valueOf(gmsServerPort()))
        .configure("auth.baseUrl", "http://localhost:" + providePort())
        .configure(
            "auth.oidc.discoveryUri",
            "http://localhost:"
                + oauthServerPort()
                + "/testIssuer/.well-known/openid-configuration")
        .in(new Environment(Mode.TEST))
        .build();
  }

  @Override
  protected TestBrowser provideBrowser(int port) {
    HtmlUnitDriver webClient = new HtmlUnitDriver();
    webClient.setJavascriptEnabled(false);
    return Helpers.testBrowser(webClient, providePort());
  }

  public int oauthServerPort() {
    return providePort() + 1;
  }

  public int gmsServerPort() {
    return providePort() + 2;
  }

  private MockOAuth2Server oauthServer;
  private Thread oauthServerThread;
  private CompletableFuture<Void> oauthServerStarted;
  private MockWebServer gmsServer;
  private String wellKnownUrl;
  private static final String TEST_USER = "urn:li:corpuser:testUser@myCompany.com";
  private static final String TEST_TOKEN = "faketoken_YCpYIrjQH4sD3_rAc3VPPFg4";

  @BeforeAll
  public void init() throws IOException {
    // Start Mock GMS
    gmsServer = new MockWebServer();
    gmsServer.enqueue(new MockResponse().setResponseCode(404));
    gmsServer.enqueue(new MockResponse().setResponseCode(404));
    gmsServer.enqueue(new MockResponse().setResponseCode(404));
    gmsServer.enqueue(new MockResponse().setBody(String.format("{\"value\":\"%s\"}", TEST_USER)));
    gmsServer.enqueue(
        new MockResponse().setBody(String.format("{\"accessToken\":\"%s\"}", TEST_TOKEN)));
    gmsServer.start(gmsServerPort());

    // Start Mock Identity Provider
    startMockOauthServer();
    // Start Play Frontend
    startServer();
    // Start Browser
    createBrowser();

    Awaitility.await().timeout(Durations.TEN_SECONDS).until(() -> app != null);
  }

  @AfterAll
  public void shutdown() throws IOException {
    if (gmsServer != null) {
      logger.info("Shutdown Mock GMS");
      gmsServer.shutdown();
    }
    logger.info("Shutdown Play Frontend");
    stopServer();
    if (oauthServer != null) {
      logger.info("Shutdown MockOAuth2Server");
      oauthServer.shutdown();
    }
    if (oauthServerThread != null && oauthServerThread.isAlive()) {
      logger.info("Shutdown MockOAuth2Server thread");
      oauthServerThread.interrupt();
      try {
        oauthServerThread.join(2000);
      } catch (InterruptedException e) {
        logger.warn("Shutdown MockOAuth2Server thread failed to join.");
      }
    }
  }

  private void startMockOauthServer() {
    Route[] routes =
        new Route[] {
          new Route() {
            @Override
            public boolean match(@NotNull OAuth2HttpRequest oAuth2HttpRequest) {
              return "HEAD".equals(oAuth2HttpRequest.getMethod())
                  && (String.format("/%s/.well-known/openid-configuration", ISSUER_ID)
                          .equals(oAuth2HttpRequest.getUrl().url().getPath())
                      || String.format("/%s/token", ISSUER_ID)
                          .equals(oAuth2HttpRequest.getUrl().url().getPath()));
            }

            @Override
            public OAuth2HttpResponse invoke(OAuth2HttpRequest oAuth2HttpRequest) {
              return new OAuth2HttpResponse(
                  Headers.of(
                      Map.of(
                          "Content-Type", "application/json",
                          "Cache-Control", "no-store",
                          "Pragma", "no-cache",
                          "Content-Length", "-1")),
                  200,
                  null,
                  null);
            }
          }
        };
    oauthServer = new MockOAuth2Server(routes);
    oauthServerStarted = new CompletableFuture<>();
    oauthServerThread =
        new Thread(
            () -> {
              try {
                oauthServer.enqueueCallback(
                    new DefaultOAuth2TokenCallback(
                        ISSUER_ID,
                        "testUser",
                        "JWT",
                        List.of(),
                        Map.of(
                            "email", "testUser@myCompany.com",
                            "groups", "myGroup"),
                        600));
                oauthServer.start(InetAddress.getByName("localhost"), oauthServerPort());
                oauthServerStarted.complete(null);
                while (!Thread.currentThread().isInterrupted() && testServer.isRunning()) {
                  try {
                    Thread.sleep(1000);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                  }
                }
              } catch (Exception e) {
                oauthServerStarted.completeExceptionally(e);
              }
            });
    oauthServerThread.setDaemon(true);
    oauthServerThread.start();
    oauthServerStarted
        .orTimeout(10, TimeUnit.SECONDS)
        .whenComplete(
            (result, throwable) -> {
              if (throwable != null) {
                if (throwable instanceof TimeoutException) {
                  throw new RuntimeException(
                      "MockOAuth2Server failed to start within timeout", throwable);
                }
                throw new RuntimeException("MockOAuth2Server failed to start", throwable);
              }
            });
    wellKnownUrl = oauthServer.wellKnownUrl(ISSUER_ID).toString();
    try {
      URL url = new URL(wellKnownUrl);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      int responseCode = conn.getResponseCode();
      logger.info("Well-known endpoint response code: {}", responseCode);
      if (responseCode != 200) {
        throw new RuntimeException(
            "MockOAuth2Server not accessible. Response code: " + responseCode);
      }
      logger.info("Successfully started MockOAuth2Server.");
    } catch (Exception e) {
      throw new RuntimeException("Failed to connect to MockOAuth2Server", e);
    }
  }

  @Test
  public void testHealth() {
    Http.RequestBuilder request = fakeRequest(routes.Application.healthcheck());
    Result result = route(app, request);
    assertEquals(OK, result.status());
  }

  @Test
  public void testIndex() {
    Http.RequestBuilder request = fakeRequest(routes.Application.index(""));
    Result result = route(app, request);
    assertEquals(OK, result.status());
  }

  @Test
  public void testIndexNotFound() {
    Http.RequestBuilder request = fakeRequest(routes.Application.index("/other"));
    Result result = route(app, request);
    assertEquals(NOT_FOUND, result.status());
  }

  @Test
  public void testOpenIdConfig() {
    assertEquals(
        "http://localhost:" + oauthServerPort() + "/testIssuer/.well-known/openid-configuration",
        wellKnownUrl);
  }

  @Test
  public void testHappyPathOidc() throws ParseException {
    browser.goTo("/authenticate");
    assertEquals("", browser.url());
    Cookie actorCookie = browser.getCookie("actor");
    assertEquals(TEST_USER, actorCookie.getValue());
    Cookie sessionCookie = browser.getCookie("PLAY_SESSION");
    String jwtStr = sessionCookie.getValue();
    JWT jwt = JWTParser.parse(jwtStr);
    JWTClaimsSet claims = jwt.getJWTClaimsSet();
    Map<String, String> data = (Map<String, String>) claims.getClaim("data");
    assertEquals(TEST_TOKEN, data.get("token"));
    assertEquals(TEST_USER, data.get("actor"));
    assertTrue(
        claims
                .getExpirationTime()
                .compareTo(new Date(System.currentTimeMillis() + (24 * 60 * 60 * 1000)))
            < 0);
  }

  @Test
  public void testAPI() throws ParseException {
    testHappyPathOidc();
    int requestCount = gmsServer.getRequestCount();

    // Prepare the mock backend to respond with HTTP 200 and an empty JSON body
    // when the proxy forwards the /api/v2/graphql/ request.
    gmsServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
    browser.goTo("/api/v2/graphql/");
    assertEquals(++requestCount, gmsServer.getRequestCount());
  }

  @Test
  public void testAPI_backendConnectionError() throws ParseException, IOException {
    testHappyPathOidc();
    int requestCount = gmsServer.getRequestCount();
    gmsServer.shutdown();
    browser.goTo("/api/v2/graphql/");
    assertEquals(requestCount, gmsServer.getRequestCount());
    // Optionally: assert browser shows connection error
    // assertTrue(browser.getPageSource().contains("Proxy connection failed"));
  }

  @Test
  public void testOidcRedirectToRequestedUrl() {
    browser.goTo("/authenticate?redirect_uri=%2Fcontainer%2Furn%3Ali%3Acontainer%3ADATABASE");
    assertEquals("container/urn:li:container:DATABASE", browser.url());
  }

  @Test
  public void testInvalidRedirectUrl() {
    browser.goTo("/authenticate?redirect_uri=https%3A%2F%2Fwww.google.com");
    assertEquals("", browser.url());
    browser.goTo("/authenticate?redirect_uri=file%3A%2F%2FmyFile");
    assertEquals("", browser.url());
    browser.goTo("/authenticate?redirect_uri=ftp%3A%2F%2FsomeFtp");
    assertEquals("", browser.url());
    browser.goTo("/authenticate?redirect_uri=localhost%3A9002%2Flogin");
    assertEquals("", browser.url());
  }

  // ---------- UNIT TESTS FOR CONTROLLER METHODS ----------

  @Test
  public void testAppConfigUnit() {
    Result result = unitApp.appConfig();
    assertEquals(OK, result.status(), "Should return HTTP 200 OK");
    String content = Helpers.contentAsString(result);
    assertTrue(content.contains("\"application\":\"datahub-frontend\""));
    assertTrue(content.contains("\"appVersion\":\"1.0.0\""));
    assertTrue(content.contains("\"isInternal\":true"));
    assertTrue(content.contains("\"shouldShowDatasetLineage\":true"));
    assertTrue(content.contains("\"userEntityProps\""));
    assertTrue(content.contains("\"wikiLinks\""));
    assertTrue(content.contains("\"tracking\""));
  }

  @Test
  public void testUserEntityPropsDirect() throws Exception {
    Method m = Application.class.getDeclaredMethod("userEntityProps");
    m.setAccessible(true);
    ObjectNode props = (ObjectNode) m.invoke(unitApp);
    assertEquals("http://primary.avi", props.get("aviUrlPrimary").asText());
    assertEquals("http://fallback.avi", props.get("aviUrlFallback").asText());
  }

  @Test
  public void testWikiLinksDirect() throws Exception {
    Method m = Application.class.getDeclaredMethod("wikiLinks");
    m.setAccessible(true);
    ObjectNode links = (ObjectNode) m.invoke(unitApp);
    assertEquals("http://help", links.get("appHelp").asText());
    assertEquals("http://gdprpii", links.get("gdprPii").asText());
    assertEquals("http://datasetdecommission", links.get("datasetDecommission").asText());
  }

  @Test
  public void testTrackingInfoDirect() throws Exception {
    Method m = Application.class.getDeclaredMethod("trackingInfo");
    m.setAccessible(true);
    ObjectNode tracking = (ObjectNode) m.invoke(unitApp);
    assertTrue(tracking.has("trackers"), "trackers field should exist");
    assertTrue(tracking.has("isEnabled"), "isEnabled field should exist");
    assertTrue(tracking.get("isEnabled").asBoolean(), "isEnabled should be true");
    ObjectNode trackers = (ObjectNode) tracking.get("trackers");
    ObjectNode piwik = (ObjectNode) trackers.get("piwik");
    assertEquals(123, piwik.get("piwikSiteId").asInt());
    assertEquals("http://piwik", piwik.get("piwikUrl").asText());
  }

  @Test
  public void testLogSlowQueryNoException() throws Exception {
    Http.Request request =
        Helpers.fakeRequest()
            .bodyJson(Json.parse("{\"query\":\"select * from test\", \"other\":\"value\"}"))
            .build();
    Method m =
        Application.class.getDeclaredMethod(
            "logSlowQuery", Http.Request.class, String.class, float.class);
    m.setAccessible(true);
    m.invoke(unitApp, request, "/api/test", 1234f);
    // No exception expected
  }
}
