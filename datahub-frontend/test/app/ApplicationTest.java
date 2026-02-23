package app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;
import static play.mvc.Http.Status.MOVED_PERMANENTLY;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.fakeRequest;
import static play.test.Helpers.route;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import controllers.routes;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.http.OAuth2HttpRequest;
import no.nav.security.mock.oauth2.http.OAuth2HttpResponse;
import no.nav.security.mock.oauth2.http.Route;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;
import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.Application;
import play.Environment;
import play.Mode;
import play.inject.guice.GuiceApplicationBuilder;
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
@SetEnvironmentVariable(key = "AUTH_OIDC_JIT_PROVISIONING_ENABLED", value = "true")
@SetEnvironmentVariable(key = "AUTH_OIDC_EXTRACT_GROUPS_ENABLED", value = "true")
@SetEnvironmentVariable(key = "AUTH_OIDC_GROUPS_CLAIM_NAME", value = "groups")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_ID", value = "testclient")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_SECRET", value = "testsecret")
@SetEnvironmentVariable(key = "AUTH_OIDC_CONNECT_TIMEOUT", value = "2000")
@SetEnvironmentVariable(key = "AUTH_OIDC_READ_TIMEOUT", value = "2000")
@SetEnvironmentVariable(key = "AUTH_OIDC_HTTP_RETRY_ATTEMPTS", value = "5")
@SetEnvironmentVariable(key = "AUTH_OIDC_HTTP_RETRY_DELAY", value = "500")
@SetEnvironmentVariable(key = "AUTH_VERBOSE_LOGGING", value = "true")
@SetEnvironmentVariable(key = "MFE_CONFIG_FILE_PATH", value = "mfe.config.local.yaml")
public class ApplicationTest extends WithBrowser {
  private static final Logger logger = LoggerFactory.getLogger(ApplicationTest.class);
  private static final String ISSUER_ID = "testIssuer";

  @Override
  protected Application provideApplication() {
    return new GuiceApplicationBuilder()
        .configure("metadataService.port", String.valueOf(actualGmsServerPort))
        .configure("metadataService.host", "localhost")
        .configure("datahub.basePath", "")
        .configure("auth.baseUrl", "http://localhost:" + providePort())
        .configure(
            "auth.oidc.discoveryUri",
            "http://localhost:"
                + actualOauthServerPort
                + "/testIssuer/.well-known/openid-configuration")
        .overrides(new TestModule())
        .in(new Environment(Mode.TEST))
        .build();
  }

  @Override
  protected TestBrowser provideBrowser(int port) {
    HtmlUnitDriver webClient = new HtmlUnitDriver();
    webClient.setJavascriptEnabled(false);
    return Helpers.testBrowser(webClient, providePort());
  }

  /** Find an available port for testing to avoid port conflicts */
  private int findAvailablePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (Exception e) {
      throw new RuntimeException("Failed to find available port", e);
    }
  }

  private MockOAuth2Server oauthServer;
  private Thread oauthServerThread;
  private CompletableFuture<Void> oauthServerStarted;

  private MockWebServer gmsServer;

  private String wellKnownUrl;
  private int actualOauthServerPort;
  private int actualGmsServerPort;
  private String actualGmsServerHost;

  private static final String TEST_USER = "urn:li:corpuser:testUser@myCompany.com";
  private static final String TEST_TOKEN = "faketoken_YCpYIrjQH4sD3_rAc3VPPFg4";

  @BeforeAll
  public void init() throws IOException {
    // Store actual ports to avoid dynamic allocation issues
    actualOauthServerPort = findAvailablePort();
    actualGmsServerPort = findAvailablePort();

    // Start Mock GMS
    gmsServer = new MockWebServer();

    // Set up dispatcher to handle requests based on path
    gmsServer.setDispatcher(
        new okhttp3.mockwebserver.Dispatcher() {
          @Override
          public MockResponse dispatch(RecordedRequest request) {
            String path = request.getPath();

            // Handle user info requests
            if (path.contains("/users/") && path.contains("/info")) {
              return new MockResponse().setBody(String.format("{\"value\":\"%s\"}", TEST_USER));
            }

            // Handle token generation requests
            if (path.contains("/generateSessionTokenForUser")) {
              return new MockResponse()
                  .setBody(String.format("{\"accessToken\":\"%s\"}", TEST_TOKEN));
            }

            // Handle other requests (like /config endpoint requests)
            if (path.contains("/config") || path.contains("/health")) {
              return new MockResponse().setResponseCode(200).setBody("{}");
            }

            // No stored sso settings
            if (path.contains("/auth/getSsoSettings")) {
              return new MockResponse().setResponseCode(404);
            }

            // Log and return 404 for unexpected requests
            logger.warn("GMS Server received unexpected request: {} {}", request.getMethod(), path);
            return new MockResponse().setResponseCode(404).setBody("Not found");
          }
        });

    gmsServer.start(InetAddress.getByName("localhost"), actualGmsServerPort);

    // Start Mock Identity Provider
    startMockOauthServer();
    // Start Play Frontend
    startServer();
    // Start Browser
    createBrowser();

    Awaitility.await().timeout(Durations.TEN_SECONDS).until(() -> app != null);

    // Wait for all servers to be fully ready to handle requests
    Awaitility.await()
        .timeout(Durations.TEN_SECONDS)
        .until(
            () -> {
              try {
                // Check Play application readiness
                browser.goTo("/admin");

                // Check mock OAuth server critical endpoints
                // Test the well-known endpoint (first thing OIDC client hits)
                String wellKnownUrl =
                    String.format(
                        "http://localhost:%d/%s/.well-known/openid-configuration",
                        actualOauthServerPort, ISSUER_ID);
                java.net.URL url = new java.net.URL(wellKnownUrl);
                java.net.HttpURLConnection connection =
                    (java.net.HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setConnectTimeout(5000);
                connection.setReadTimeout(5000);
                int wellKnownResponseCode = connection.getResponseCode();
                connection.disconnect();

                // Test the userinfo endpoint (where groups are retrieved)
                String userInfoUrl =
                    String.format(
                        "http://localhost:%d/%s/userinfo", actualOauthServerPort, ISSUER_ID);
                java.net.URL userInfoUrlObj = new java.net.URL(userInfoUrl);
                java.net.HttpURLConnection userInfoConnection =
                    (java.net.HttpURLConnection) userInfoUrlObj.openConnection();
                userInfoConnection.setRequestMethod("GET");
                userInfoConnection.setConnectTimeout(5000);
                userInfoConnection.setReadTimeout(5000);
                int userInfoResponseCode = userInfoConnection.getResponseCode();
                userInfoConnection.disconnect();

                logger.debug(
                    "All servers readiness check - Play app: OK, well-known: {}, userinfo: {}",
                    wellKnownResponseCode,
                    userInfoResponseCode);

                return wellKnownResponseCode == 200 && userInfoResponseCode == 200;
              } catch (Exception e) {
                logger.debug("Servers not ready yet: {}", e.getMessage());
                return false;
              }
            });
  }

  @BeforeEach
  public void setUpTest() {
    // Clear any captured proposals from previous tests to prevent interference
    TestModule.clearCapturedProposals();

    // Clear browser cookies and state to ensure clean test isolation
    if (browser != null) {
      // Clear cookies using the underlying WebDriver
      browser.getDriver().manage().deleteAllCookies();
    }
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
        oauthServerThread.join(1000); // Wait up to 1 second for thread to finish
      } catch (InterruptedException e) {
        logger.warn("Shutdown MockOAuth2Server thread failed to join.");
        Thread.currentThread().interrupt();
      }
      // Force stop if still alive
      if (oauthServerThread.isAlive()) {
        logger.warn("OAuth server thread still alive after interrupt, forcing stop");
        oauthServerThread.stop(); // Force stop as last resort
      }
    }
  }

  private void startMockOauthServer() {
    // Configure HEAD responses and userInfo endpoint
    Route[] routes =
        new Route[] {
          new Route() {
            @Override
            public boolean match(@NotNull OAuth2HttpRequest oAuth2HttpRequest) {
              return "HEAD".equals(oAuth2HttpRequest.getMethod())
                  && (String.format("/%s/.well-known/openid-configuration", ISSUER_ID)
                          .equals(oAuth2HttpRequest.getUrl().url().getPath())
                      || String.format("/%s/token", ISSUER_ID)
                          .equals(oAuth2HttpRequest.getUrl().url().getPath())
                      || String.format("/%s/userinfo", ISSUER_ID)
                          .equals(oAuth2HttpRequest.getUrl().url().getPath()));
            }

            @Override
            public OAuth2HttpResponse invoke(OAuth2HttpRequest oAuth2HttpRequest) {
              String path = oAuth2HttpRequest.getUrl().url().getPath();
              String responseBody = null;

              if (path.equals(String.format("/%s/.well-known/openid-configuration", ISSUER_ID))) {
                // Return well-known configuration with userinfo endpoint
                int port = oAuth2HttpRequest.getUrl().url().getPort();
                responseBody =
                    String.format(
                        "{\n"
                            + "  \"issuer\": \"http://localhost:%d/%s\",\n"
                            + "  \"authorization_endpoint\": \"http://localhost:%d/%s/authorize\",\n"
                            + "  \"token_endpoint\": \"http://localhost:%d/%s/token\",\n"
                            + "  \"userinfo_endpoint\": \"http://localhost:%d/%s/userinfo\",\n"
                            + "  \"jwks_uri\": \"http://localhost:%d/%s/.well-known/jwks.json\",\n"
                            + "  \"response_types_supported\": [\"code\"],\n"
                            + "  \"subject_types_supported\": [\"public\"],\n"
                            + "  \"id_token_signing_alg_values_supported\": [\"RS256\"]\n"
                            + "}",
                        port, ISSUER_ID, port, ISSUER_ID, port, ISSUER_ID, port, ISSUER_ID, port,
                        ISSUER_ID);
              } else if (path.equals(String.format("/%s/userinfo", ISSUER_ID))) {
                // For HEAD requests to userinfo endpoint, just return 200 with no body
                responseBody = null;
              }

              return new OAuth2HttpResponse(
                  Headers.of(
                      Map.of(
                          "Content-Type", "application/json",
                          "Cache-Control", "no-store",
                          "Pragma", "no-cache")),
                  200,
                  responseBody,
                  null);
            }
          },
          // Add userInfo endpoint route
          new Route() {
            @Override
            public boolean match(@NotNull OAuth2HttpRequest oAuth2HttpRequest) {
              return "GET".equals(oAuth2HttpRequest.getMethod())
                  && String.format("/%s/userinfo", ISSUER_ID)
                      .equals(oAuth2HttpRequest.getUrl().url().getPath());
            }

            @Override
            public OAuth2HttpResponse invoke(OAuth2HttpRequest oAuth2HttpRequest) {
              // Log the request to help debug
              logger.debug(
                  "UserInfo endpoint called with headers: {}", oAuth2HttpRequest.getHeaders());

              // Return userInfo with groups (not in ID token)
              String userInfoResponse =
                  String.format(
                      "{\n"
                          + "  \"sub\": \"testUser\",\n"
                          + "  \"preferred_username\": \"testUser\",\n"
                          + "  \"given_name\": \"Test\",\n"
                          + "  \"family_name\": \"User\",\n"
                          + "  \"email\": \"testUser@myCompany.com\",\n"
                          + "  \"name\": \"Test User\",\n"
                          + "  \"groups\": \"myGroup\",\n"
                          + "  \"email_verified\": true\n"
                          + "}");

              return new OAuth2HttpResponse(
                  Headers.of(Map.of("Content-Type", "application/json")),
                  200,
                  userInfoResponse,
                  null);
            }
          }
        };
    oauthServer = new MockOAuth2Server(routes);
    oauthServerStarted = new CompletableFuture<>();

    // Create and start server in separate thread
    oauthServerThread =
        new Thread(
            () -> {
              try {
                // Configure mock responses - groups are NOT in ID token, only in userInfo endpoint
                // Enqueue enough callbacks for all tests with retries (22 tests * 10 retries = 220)
                for (int i = 0; i < 220; i++) {
                  oauthServer.enqueueCallback(
                      new DefaultOAuth2TokenCallback(
                          ISSUER_ID,
                          "testUser",
                          "JWT",
                          List.of(),
                          Map.of(
                              "email", "testUser@myCompany.com"
                              // Note: groups are intentionally NOT included in ID token
                              // They will only be available from the userInfo endpoint
                              ),
                          600));
                }

                oauthServer.start(InetAddress.getByName("localhost"), actualOauthServerPort);

                oauthServerStarted.complete(null);

                // Keep thread alive - it will be interrupted during shutdown
                while (!Thread.currentThread().isInterrupted()) {
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

    oauthServerThread.setDaemon(true); // Ensure thread doesn't prevent JVM shutdown
    oauthServerThread.start();

    // Wait for server to start with timeout - block until complete
    try {
      oauthServerStarted.get(10, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new RuntimeException("MockOAuth2Server failed to start within timeout", e);
    } catch (Exception e) {
      throw new RuntimeException("MockOAuth2Server failed to start", e);
    }

    // Discovery url to authorization server metadata
    wellKnownUrl =
        "http://localhost:"
            + actualOauthServerPort
            + "/"
            + ISSUER_ID
            + "/.well-known/openid-configuration";

    // Wait for server to return configuration with retries
    // Validate mock server returns data
    int maxRetries = 20;
    int retryCount = 0;
    boolean serverReady = false;

    while (retryCount < maxRetries && !serverReady) {
      try {
        URL url = new URL(wellKnownUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        int responseCode = conn.getResponseCode();
        conn.disconnect();

        if (responseCode == 200) {
          logger.info("Successfully started MockOAuth2Server after {} attempts.", retryCount + 1);
          serverReady = true;
        } else {
          throw new RuntimeException(
              "MockOAuth2Server not accessible. Response code: " + responseCode);
        }
      } catch (Exception e) {
        retryCount++;
        if (retryCount >= maxRetries) {
          throw new RuntimeException(
              "Failed to connect to MockOAuth2Server after " + maxRetries + " attempts", e);
        }
        logger.debug("Waiting for OAuth server to be ready, attempt {}/{}", retryCount, maxRetries);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting for MockOAuth2Server", ie);
        }
      }
    }

    // Additional wait to ensure token endpoint is also fully ready
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during final OAuth server stabilization", e);
    }
    logger.info("MockOAuth2Server fully initialized and stabilized.");
  }

  @Test
  public void testHealth() {
    Http.RequestBuilder request = fakeRequest(routes.Application.healthcheck());

    Result result = route(app, request);
    assertEquals(OK, result.status());
  }

  @Test
  public void testAppConfigWithBasePath() {
    Http.RequestBuilder request = fakeRequest(routes.Application.appConfig());

    Result result = route(app, request);
    assertEquals(OK, result.status());

    // Verify the response contains basePath configuration
    String content = Helpers.contentAsString(result);
    assertTrue(content.contains("\"basePath\""));
    assertTrue(
        content.contains(
            "\"\"")); // Default basePath is "" (empty) as configured in provideApplication()
  }

  @Test
  public void testAppConfigWithCustomBasePath() {
    // Create a new application with custom basePath
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "/custom")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    Http.RequestBuilder request = fakeRequest(routes.Application.appConfig());

    Result result = route(customApp, request);
    assertEquals(OK, result.status());

    // Verify the response contains custom basePath configuration
    String content = Helpers.contentAsString(result);
    assertTrue(content.contains("\"basePath\""));
    assertTrue(content.contains("\"/custom\""));
  }

  @Test
  public void testIndexWithBasePathInjection() {
    Http.RequestBuilder request = fakeRequest(routes.Application.index(""));

    Result result = route(app, request);
    assertEquals(OK, result.status());

    // Verify the response contains basePath injection
    String content = Helpers.contentAsString(result);
    // The HTML should contain the basePath replacement
    assertTrue(content.contains("@basePath") || content.contains("href=\"/\""));
  }

  @Test
  public void testIndexWithCustomBasePathInjection() {
    // Create a new application with custom basePath
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "/datahub")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    // Use "index.html" instead of "" to avoid matching the trailing slash redirect rule
    Http.RequestBuilder request = fakeRequest(routes.Application.index("index.html"));

    Result result = route(customApp, request);
    assertEquals(OK, result.status());

    // Verify the response contains custom basePath injection
    String content = Helpers.contentAsString(result);
    // The HTML should contain the custom basePath
    assertTrue(content.contains("@basePath") || content.contains("href=\"/datahub/\""));
  }

  @Test
  public void testIndexWithBasePathEndingSlash() {
    // Test basePath that doesn't end with slash gets slash added
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "/datahub")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    Http.RequestBuilder request = fakeRequest(routes.Application.index(""));

    Result result = route(customApp, request);
    assertEquals(OK, result.status());

    // Verify the response contains basePath with trailing slash
    String content = Helpers.contentAsString(result);
    assertTrue(content.contains("@basePath") || content.contains("href=\"/datahub/\""));
  }

  // BasePathRedirectFilter coverage (integration; context-mounted app requests often 404 before
  // filter):
  // - effectiveBase = (basePath == "/") ? "" : basePath: default app has basePath "/" ->
  // effectiveBase "" (all trailing-slash tests).
  // - redirectUrl = effectiveBase + "/" + (safePath.isEmpty ? "" : safePath) + querySuffix:
  // trailing branch: testRedirectTrailingSlash* (non-empty safePath),
  // testRedirectTrailingSlashOnlySlashes (safePath empty),
  // testBasePathRedirectFilterTrailingSlashPreservesQueryString (querySuffix); base path branch:
  // testBasePathRedirectFilterPreventsOpenRedirectDoubleSlashPath when basePath "/" and path
  // "//evil.com".
  // - basePath.nonEmpty && !path.startsWith(basePath): true in
  // testBasePathRedirectFilterPreventsOpenRedirectDoubleSlashPath; false branch not asserted
  // (path-with-context requests pass through).

  /** BasePathRedirectFilter handles trailing-slash redirects before routing. */
  @Test
  public void testRedirectTrailingSlash() {
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "test/");
    Result result = route(app, request);
    assertEquals(MOVED_PERMANENTLY, result.status());
    assertEquals("/test", result.redirectLocation().orElse(""));
  }

  @Test
  public void testRedirectTrailingSlashNestedPath() {
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "foo/bar/baz/");
    Result result = route(app, request);
    assertEquals(MOVED_PERMANENTLY, result.status());
    assertEquals("/foo/bar/baz", result.redirectLocation().orElse(""));
  }

  @Test
  public void testRedirectTrailingSlashWithLeadingSlash() {
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "/evil.com/");
    Result result = route(app, request);
    assertEquals(MOVED_PERMANENTLY, result.status());
    assertEquals("/evil.com", result.redirectLocation().orElse(""));
  }

  @Test
  public void testRedirectTrailingSlashWithMultipleLeadingSlashes() {
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "///evil.com/path/");
    Result result = route(app, request);
    assertEquals(MOVED_PERMANENTLY, result.status());
    assertEquals("/evil.com/path", result.redirectLocation().orElse(""));
  }

  @Test
  public void testRedirectTrailingSlashOnlySlashes() {
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "////");
    Result result = route(app, request);
    assertEquals(MOVED_PERMANENTLY, result.status());
    assertEquals("/", result.redirectLocation().orElse(""));
  }

  @Test
  public void testRedirectTrailingSlashNormalPath() {
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "dataset/urn:li:dataset:1/");
    Result result = route(app, request);
    assertEquals(MOVED_PERMANENTLY, result.status());
    assertEquals("/dataset/urn:li:dataset:1", result.redirectLocation().orElse(""));
  }

  /**
   * BasePathRedirectFilter runs before routes; requests to paths like ////google.com/ must redirect
   * to same-origin /google.com, not to scheme-relative //google.com (open redirect).
   */
  @Test
  public void testBasePathRedirectFilterPreventsOpenRedirectSchemeRelative() {
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "////google.com/");
    Result result = route(app, request);
    assertEquals(MOVED_PERMANENTLY, result.status());
    String location = result.redirectLocation().orElse("");
    assertTrue(
        location.equals("/google.com") || location.startsWith("/google.com?"),
        "Redirect must be same-origin path /google.com, not scheme-relative //google.com; got: "
            + location);
  }

  /** Double-slash path without trailing slash (base path branch) must also be safe. */
  @Test
  public void testBasePathRedirectFilterPreventsOpenRedirectDoubleSlashPath() {
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "//evil.com");
    Result result = route(app, request);
    // Filter may redirect to base path or pass through; redirect location must not be
    // scheme-relative
    if (result.status() == MOVED_PERMANENTLY) {
      String location = result.redirectLocation().orElse("");
      assertTrue(
          location.startsWith("/") && !location.startsWith("//"),
          "Redirect must not be scheme-relative; got: " + location);
    }
  }

  /** Trailing-slash redirect preserves query string (redirectUrl + querySuffix). */
  @Test
  public void testBasePathRedirectFilterTrailingSlashPreservesQueryString() {
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "test/?foo=bar&baz=qux");
    Result result = route(app, request);
    assertEquals(MOVED_PERMANENTLY, result.status());
    String location = result.redirectLocation().orElse("");
    assertTrue(location.startsWith("/test"), "Redirect should start with /test; got: " + location);
    assertTrue(location.contains("foo=bar"), "Redirect should preserve query; got: " + location);
    assertTrue(location.contains("baz=qux"), "Redirect should preserve query; got: " + location);
  }

  @Test
  public void testAppConfigWithEmptyBasePath() {
    // Create a new application with empty basePath
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    Http.RequestBuilder request = fakeRequest(routes.Application.appConfig());

    Result result = route(customApp, request);
    assertEquals(OK, result.status());

    // Verify the response contains empty basePath configuration
    String content = Helpers.contentAsString(result);
    assertTrue(content.contains("\"basePath\""));
    assertTrue(content.contains("\"\"")); // Empty basePath
  }

  @Test
  public void testAppConfigWithNullBasePath() {
    // Create a new application without basePath configuration (should use default)
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    Http.RequestBuilder request = fakeRequest(routes.Application.appConfig());

    Result result = route(customApp, request);
    assertEquals(OK, result.status());

    // Verify the response contains default basePath configuration
    String content = Helpers.contentAsString(result);
    assertTrue(content.contains("\"basePath\""));
    // Should contain the default value from application.conf
  }

  @Test
  public void testIndexWithEmptyBasePath() {
    // Test with empty basePath
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    // Use "index.html" instead of "" to avoid matching the trailing slash redirect rule
    Http.RequestBuilder request = fakeRequest(routes.Application.index("index.html"));

    Result result = route(customApp, request);
    assertEquals(OK, result.status());

    // Verify the response handles empty basePath correctly
    String content = Helpers.contentAsString(result);
    // Should still contain the basePath replacement logic
    assertTrue(content.contains("@basePath") || content.contains("href=\""));
  }

  @Test
  public void testIndexWithRootBasePath() {
    // Test with root basePath "/"
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "/")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    Http.RequestBuilder request = fakeRequest(routes.Application.index(""));

    Result result = route(customApp, request);
    assertEquals(OK, result.status());

    // Verify the response contains root basePath
    String content = Helpers.contentAsString(result);
    assertTrue(content.contains("@basePath") || content.contains("href=\"/\""));
  }

  @Test
  public void testIndexWithComplexBasePath() {
    // Test with complex basePath like "/datahub/v2"
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "/datahub/v2")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    Http.RequestBuilder request = fakeRequest(routes.Application.index(""));

    Result result = route(customApp, request);
    assertEquals(OK, result.status());

    // Verify the response contains complex basePath
    String content = Helpers.contentAsString(result);
    assertTrue(content.contains("@basePath") || content.contains("href=\"/datahub/v2/\""));
  }

  @Test
  public void testPlayHttpContextIntegration() {
    // Test integration with play.http.context configuration
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "/datahub")
            .configure("play.http.context", "/datahub")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    // When play.http.context is set, routes are prefixed with that context
    Http.RequestBuilder request =
        fakeRequest(Helpers.GET, "/datahub" + routes.Application.appConfig().url());

    Result result = route(customApp, request);
    assertEquals(OK, result.status());

    // Verify the response contains basePath configuration
    String content = Helpers.contentAsString(result);
    assertTrue(content.contains("\"basePath\""));
    assertTrue(content.contains("\"/datahub\""));
  }

  @Test
  public void testPlayHttpContextWithDifferentBasePath() {
    // Test when play.http.context and datahub.basePath are different
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "/frontend")
            .configure("play.http.context", "/api")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    // When play.http.context is set, routes are prefixed with that context
    Http.RequestBuilder request =
        fakeRequest(Helpers.GET, "/api" + routes.Application.appConfig().url());

    Result result = route(customApp, request);
    assertEquals(OK, result.status());

    // Verify the response contains the datahub.basePath, not play.http.context
    String content = Helpers.contentAsString(result);
    assertTrue(content.contains("\"basePath\""));
    assertTrue(content.contains("\"/frontend\""));
  }

  @Test
  public void testAppConfigWithAllBasePathVariations() {
    // Test various basePath configurations to ensure robustness
    String[] basePaths = {"", "/", "/datahub", "/datahub/", "/custom/path", "/custom/path/"};

    for (String basePath : basePaths) {
      Application customApp =
          new GuiceApplicationBuilder()
              .configure("metadataService.port", String.valueOf(actualGmsServerPort))
              .configure("metadataService.host", "localhost")
              .configure("datahub.basePath", basePath)
              .configure("auth.baseUrl", "http://localhost:" + providePort())
              .configure(
                  "auth.oidc.discoveryUri",
                  "http://localhost:"
                      + actualOauthServerPort
                      + "/testIssuer/.well-known/openid-configuration")
              .overrides(new TestModule())
              .in(new Environment(Mode.TEST))
              .build();

      Http.RequestBuilder request = fakeRequest(routes.Application.appConfig());

      Result result = route(customApp, request);
      assertEquals(OK, result.status());

      // Verify the response contains basePath configuration
      String content = Helpers.contentAsString(result);
      assertTrue(
          content.contains("\"basePath\""),
          "BasePath not found in response for basePath: " + basePath);
    }
  }

  @Test
  public void testIndexWithAllBasePathVariations() {
    // Test various basePath configurations in index method
    String[] basePaths = {"", "/", "/datahub", "/datahub/", "/custom/path", "/custom/path/"};

    for (String basePath : basePaths) {
      Application customApp =
          new GuiceApplicationBuilder()
              .configure("metadataService.port", String.valueOf(actualGmsServerPort))
              .configure("metadataService.host", "localhost")
              .configure("datahub.basePath", basePath)
              .configure("auth.baseUrl", "http://localhost:" + providePort())
              .configure(
                  "auth.oidc.discoveryUri",
                  "http://localhost:"
                      + actualOauthServerPort
                      + "/testIssuer/.well-known/openid-configuration")
              .overrides(new TestModule())
              .in(new Environment(Mode.TEST))
              .build();

      Http.RequestBuilder request = fakeRequest(routes.Application.index(""));

      Result result = route(customApp, request);
      assertEquals(OK, result.status());

      // Verify the response contains basePath injection
      String content = Helpers.contentAsString(result);
      assertTrue(
          content.contains("@basePath") || content.contains("href="),
          "BasePath injection not found in response for basePath: " + basePath);
    }
  }

  @Test
  public void testIndex() {
    Http.RequestBuilder request = fakeRequest(routes.Application.index(""));

    Result result = route(app, request);
    assertEquals(OK, result.status());
  }

  @Test
  public void testMovedPermanently() {
    // We expect now to be redirected instead of returning 404
    Http.RequestBuilder request = fakeRequest(routes.Application.index("/other"));
    Result result = route(app, request);
    assertEquals(MOVED_PERMANENTLY, result.status());
  }

  @Test
  public void testOpenIdConfig() {
    assertEquals(
        "http://localhost:"
            + actualOauthServerPort
            + "/testIssuer/.well-known/openid-configuration",
        wellKnownUrl);
  }

  @Test
  public void testHappyPathOidc() throws ParseException {
    // Verify the list is actually empty (cleared by @BeforeEach)
    assertTrue(TestModule.getCapturedProposals().isEmpty());

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
    // Default expiration is 24h, so should always be less than current time + 1 day since it stamps
    // the time before this executes. Use a more generous tolerance to account for timezone
    // differences, DST transitions, and test execution time variations.
    // Increased tolerance to 22-26 hours to handle DST transitions (which can cause 1-hour shifts)
    Date maxExpectedExpiration =
        new Date(System.currentTimeMillis() + (26 * 60 * 60 * 1000)); // 26 hours
    Date minExpectedExpiration =
        new Date(System.currentTimeMillis() + (22 * 60 * 60 * 1000)); // 22 hours
    Date actualExpiration = claims.getExpirationTime();

    assertTrue(
        actualExpiration.after(minExpectedExpiration)
            && actualExpiration.before(maxExpectedExpiration),
        "JWT expiration time should be within reasonable bounds. Actual: "
            + actualExpiration
            + ", Min expected: "
            + minExpectedExpiration
            + ", Max expected: "
            + maxExpectedExpiration);

    // Verify that ingestProposal was called for group membership and user status updates
    List<MetadataChangeProposal> capturedProposals = TestModule.getCapturedProposals();
    logger.debug("Captured {} ingestProposal calls", capturedProposals.size());

    // We should have at least 2 calls: one for group membership and one for user status
    assertTrue(capturedProposals.size() >= 2);

    // Verify we have a group membership proposal
    boolean hasGroupMembership =
        capturedProposals.stream()
            .anyMatch(proposal -> "groupMembership".equals(proposal.getAspectName()));
    assertTrue(hasGroupMembership);

    // Verify we have a user status proposal
    boolean hasUserStatus =
        capturedProposals.stream()
            .anyMatch(proposal -> "corpUserStatus".equals(proposal.getAspectName()));
    assertTrue(hasUserStatus);

    // Verify the basic OIDC flow worked and the user was authenticated
    assertNotNull(actorCookie);
    assertNotNull(sessionCookie);

    // Find and validate the group membership proposal
    MetadataChangeProposal groupMembershipProposal =
        capturedProposals.stream()
            .filter(proposal -> "groupMembership".equals(proposal.getAspectName()))
            .findFirst()
            .orElse(null);

    assertNotNull(groupMembershipProposal);
    assertEquals(
        "urn:li:corpuser:testUser@myCompany.com",
        groupMembershipProposal.getEntityUrn().toString());
    assertEquals("corpuser", groupMembershipProposal.getEntityType());
    assertEquals("groupMembership", groupMembershipProposal.getAspectName());
    assertEquals("UPSERT", groupMembershipProposal.getChangeType().toString());

    // Validate that the group membership proposal contains "myGroup" from the /userInfo endpoint
    String aspectData =
        groupMembershipProposal.getAspect().getValue().asString(StandardCharsets.UTF_8);
    logger.debug("Group membership aspect data: {}", aspectData);
    assertTrue(aspectData.contains("myGroup"));
  }

  @Test
  public void testOidcRedirectToRequestedUrl() {
    // Wait briefly to ensure browser is ready after @BeforeEach cleanup
    Awaitility.await()
        .timeout(Durations.TEN_SECONDS)
        .pollDelay(Durations.ONE_HUNDRED_MILLISECONDS)
        .until(() -> browser.getDriver() != null);

    browser.goTo("/authenticate?redirect_uri=%2Fcontainer%2Furn%3Ali%3Acontainer%3ADATABASE");
    assertEquals("container/urn:li:container:DATABASE", browser.url());
  }

  /**
   * The Redirect Uri parameter is used to store a previous relative location within the app to be
   * able to take a user back to their expected page. Redirecting to other domains should be
   * blocked.
   */
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

    // Protocol-relative URL (///google.com) must not redirect to external host
    browser.goTo("/authenticate?redirect_uri=%2F%2F%2Fgoogle.com");
    assertEquals("", browser.url());
  }

  /** Test module that provides comprehensive mocks to handle all GMS interactions */
  private static class TestModule extends AbstractModule {
    // Store captured ingestProposal calls for validation
    private static final List<MetadataChangeProposal> capturedProposals = new ArrayList<>();

    @Override
    protected void configure() {
      // This module will override providers for GMS interactions
    }

    public static List<MetadataChangeProposal> getCapturedProposals() {
      return new ArrayList<>(capturedProposals);
    }

    public static void clearCapturedProposals() {
      capturedProposals.clear();
    }

    @Provides
    @Singleton
    protected SystemEntityClient provideMockSystemEntityClient() {
      logger.debug("Creating mock SystemEntityClient");
      SystemEntityClient mockClient = mock(SystemEntityClient.class);

      try {
        // Configure user provisioning mocks (mirrors tryProvisionUser)
        configureUserProvisioningMocks(mockClient);

        // Configure group provisioning mocks (mirrors tryProvisionGroups)
        configureGroupProvisioningMocks(mockClient);

        // Mock ingestProposal to capture calls for validation
        doAnswer(
                invocation -> {
                  MetadataChangeProposal proposal = invocation.getArgument(1);
                  logger.debug(
                      "ingestProposal() called with entityUrn: {}, entityType: {}, aspectName: {}, changeType: {}",
                      proposal.getEntityUrn(),
                      proposal.getEntityType(),
                      proposal.getAspectName(),
                      proposal.getChangeType());
                  // Capture the proposal for validation
                  capturedProposals.add(proposal);
                  return null;
                })
            .when(mockClient)
            .ingestProposal(any(), any());
      } catch (RemoteInvocationException e) {
        // This should not happen with mocks, but handle it just in case
        throw new RuntimeException("Failed to configure mock SystemEntityClient", e);
      } catch (Exception e) {
        // This should not happen with mocks, but handle it just in case
        throw new RuntimeException("Failed to configure mock SystemEntityClient", e);
      }

      return mockClient;
    }

    /**
     * Configures mocks for user provisioning flow (mirrors tryProvisionUser method). - First get()
     * call returns null (user doesn't exist) - update() call stores the actual Entity object -
     * Subsequent get() calls return the stored Entity
     */
    private void configureUserProvisioningMocks(SystemEntityClient mockClient)
        throws RemoteInvocationException {
      final AtomicReference<Entity> storedEntity = new AtomicReference<>();

      // First call to get() returns an Entity with just a key aspect (user doesn't exist),
      // subsequent calls return the stored user with full aspects
      doAnswer(
              invocation -> {
                Entity entity = storedEntity.get();
                if (entity == null) {
                  // Return an Entity with just a key aspect (simulating non-existent user)
                  Entity keyOnlyEntity = new Entity();
                  // Create a minimal CorpUserSnapshot with just the key aspect
                  CorpUserSnapshot keyOnlySnapshot = new CorpUserSnapshot();
                  keyOnlySnapshot.setUrn(invocation.getArgument(1)); // The URN from the get() call
                  keyOnlySnapshot.setAspects(new CorpUserAspectArray());
                  keyOnlyEntity.setValue(Snapshot.create(keyOnlySnapshot));
                  logger.debug("get() called, returning key-only entity for non-existent user");
                  return keyOnlyEntity;
                } else {
                  logger.debug("get() called, returning stored entity with full aspects");
                  return entity;
                }
              })
          .when(mockClient)
          .get(any(), any());

      // Store the entity when update() is called
      doAnswer(
              invocation -> {
                Entity entity = invocation.getArgument(1);
                storedEntity.set(entity);
                logger.debug("update() called, stored entity: {}", entity);
                return null;
              })
          .when(mockClient)
          .update(any(), any());
    }

    /**
     * Configures mocks for group provisioning flow (mirrors tryProvisionGroups method). -
     * batchGet() returns empty map initially (groups don't exist) - batchUpdate() stores the actual
     * Map<Urn, Entity> of groups - Subsequent batchGet() calls return the stored groups
     */
    private void configureGroupProvisioningMocks(SystemEntityClient mockClient)
        throws RemoteInvocationException {
      final AtomicReference<Map<Urn, Entity>> storedGroups =
          new AtomicReference<>(Collections.emptyMap());

      // batchGet() returns the stored groups (empty initially, then populated after batchUpdate)
      doAnswer(
              invocation -> {
                Map<Urn, Entity> groups = storedGroups.get();
                logger.debug("batchGet() called, returning {} groups", groups.size());
                return groups;
              })
          .when(mockClient)
          .batchGet(any(), any());

      // Store the groups when batchUpdate() is called
      doAnswer(
              invocation -> {
                Set<Entity> groupEntities = invocation.getArgument(1);
                // Convert Set<Entity> to Map<Urn, Entity> for storage
                Map<Urn, Entity> groupsMap = new HashMap<>();
                for (Entity entity : groupEntities) {
                  Urn urn = entity.getValue().getCorpGroupSnapshot().getUrn();
                  groupsMap.put(urn, entity);
                }
                storedGroups.set(groupsMap);
                logger.debug("batchUpdate() called, stored {} groups", groupEntities.size());
                return null;
              })
          .when(mockClient)
          .batchUpdate(any(), any());
    }
  }

  @Test
  public void testMapPathGraphQLMappingWithBasePath() {
    // Test that legacy GraphQL path is mapped correctly even with base path
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "/datahub")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    // Test legacy GraphQL path mapping with base path
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "/datahub/api/v2/graphql");
    Result result = route(customApp, request);
    assertEquals(OK, result.status());
  }

  @Test
  public void testMapPathRegularApiPathWithBasePath() {
    // Test that regular API paths with base path are handled correctly
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "/datahub")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    // Test regular API path with base path
    Http.RequestBuilder request = fakeRequest(Helpers.GET, "/datahub/api/entities");
    Result result = route(customApp, request);
    assertEquals(OK, result.status());
  }

  @Test
  public void testSwaggerUiPathWithBasePath() {
    // Test swagger UI path with base path - should preserve full path and not strip base path
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "/datahub")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModule())
            .in(new Environment(Mode.TEST))
            .build();

    Http.RequestBuilder request = fakeRequest(Helpers.GET, "/datahub/openapi/swagger-ui");

    Result result = route(customApp, request);
    assertEquals(OK, result.status());
  }

  @Test
  public void testIndexWhenResourceNotFound() {
    // Create a new application with a custom test module that overrides the Application controller
    // to simulate the case where the index.html resource cannot be loaded
    Application customApp =
        new GuiceApplicationBuilder()
            .configure("metadataService.port", String.valueOf(actualGmsServerPort))
            .configure("metadataService.host", "localhost")
            .configure("datahub.basePath", "")
            .configure("auth.baseUrl", "http://localhost:" + providePort())
            .configure(
                "auth.oidc.discoveryUri",
                "http://localhost:"
                    + actualOauthServerPort
                    + "/testIssuer/.well-known/openid-configuration")
            .overrides(new TestModuleWithFailingResource())
            .in(new Environment(Mode.TEST))
            .build();

    Http.RequestBuilder request = fakeRequest(routes.Application.index(""));

    Result result = route(customApp, request);

    // When the resource cannot be loaded, the controller should return a 404 status
    assertEquals(play.mvc.Http.Status.NOT_FOUND, result.status());

    // Verify the response has the correct content type and cache control header
    assertEquals("text/html", result.contentType().orElse(""));
    assertTrue(result.headers().containsKey("Cache-Control"));
    assertEquals("no-cache", result.headers().get("Cache-Control"));
  }

  /**
   * Test module that provides a mock Application controller that simulates resource loading failure
   */
  private static class TestModuleWithFailingResource extends AbstractModule {
    @Override
    protected void configure() {
      // This module will override the Application controller
    }

    @Provides
    @Singleton
    protected controllers.Application provideFailingApplicationController(
        Environment environment, com.typesafe.config.Config config) {
      // Create a mock HttpClient
      java.net.http.HttpClient mockHttpClient = mock(java.net.http.HttpClient.class);

      // Create a mock Environment that returns null for resourceAsStream
      Environment mockEnvironment = mock(Environment.class);
      when(mockEnvironment.resourceAsStream("public/index.html")).thenReturn(null);

      return new controllers.Application(mockHttpClient, mockEnvironment, config);
    }

    @Provides
    @Singleton
    protected SystemEntityClient provideMockSystemEntityClient() {
      // Reuse the same mock SystemEntityClient as the base TestModule
      return new TestModule().provideMockSystemEntityClient();
    }
  }
}
