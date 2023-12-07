package app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.fakeRequest;
import static play.test.Helpers.route;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import controllers.routes;
import java.io.IOException;
import java.net.InetAddress;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
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
@SetEnvironmentVariable(key = "AUTH_OIDC_JIT_PROVISIONING_ENABLED", value = "false")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_ID", value = "testclient")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_SECRET", value = "testsecret")
public class ApplicationTest extends WithBrowser {
  private static final String ISSUER_ID = "testIssuer";

  @Override
  protected Application provideApplication() {
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

  private MockOAuth2Server _oauthServer;
  private MockWebServer _gmsServer;

  private String _wellKnownUrl;

  private static final String TEST_USER = "urn:li:corpuser:testUser@myCompany.com";
  private static final String TEST_TOKEN = "faketoken_YCpYIrjQH4sD3_rAc3VPPFg4";

  @BeforeAll
  public void init() throws IOException {
    _gmsServer = new MockWebServer();
    _gmsServer.enqueue(new MockResponse().setBody(String.format("{\"value\":\"%s\"}", TEST_USER)));
    _gmsServer.enqueue(
        new MockResponse().setBody(String.format("{\"accessToken\":\"%s\"}", TEST_TOKEN)));
    _gmsServer.start(gmsServerPort());

    _oauthServer = new MockOAuth2Server();
    _oauthServer.enqueueCallback(
        new DefaultOAuth2TokenCallback(
            ISSUER_ID,
            "testUser",
            List.of(),
            Map.of(
                "email", "testUser@myCompany.com",
                "groups", "myGroup"),
            600));
    _oauthServer.start(InetAddress.getByName("localhost"), oauthServerPort());

    // Discovery url to authorization server metadata
    _wellKnownUrl = _oauthServer.wellKnownUrl(ISSUER_ID).toString();

    startServer();
    createBrowser();

    Awaitility.await().timeout(Durations.TEN_SECONDS).until(() -> app != null);
  }

  @AfterAll
  public void shutdown() throws IOException {
    if (_gmsServer != null) {
      _gmsServer.shutdown();
    }
    if (_oauthServer != null) {
      _oauthServer.shutdown();
    }
    stopServer();
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
        _wellKnownUrl);
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
    // Default expiration is 24h, so should always be less than current time + 1 day since it stamps
    // the time before this executes
    assertTrue(
        claims
                .getExpirationTime()
                .compareTo(new Date(System.currentTimeMillis() + (24 * 60 * 60 * 1000)))
            < 0);
  }

  @Test
  public void testAPI() throws ParseException {
    testHappyPathOidc();
    int requestCount = _gmsServer.getRequestCount();

    browser.goTo("/api/v2/graphql/");
    assertEquals(++requestCount, _gmsServer.getRequestCount());
  }

  @Test
  public void testOidcRedirectToRequestedUrl() throws InterruptedException {
    browser.goTo("/authenticate?redirect_uri=%2Fcontainer%2Furn%3Ali%3Acontainer%3ADATABASE");
    assertEquals("container/urn:li:container:DATABASE", browser.url());
  }
}
