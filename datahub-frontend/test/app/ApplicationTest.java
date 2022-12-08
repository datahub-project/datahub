package app;

import controllers.routes;
import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.openqa.selenium.Cookie;
import play.Application;
import play.Environment;
import play.Mode;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;

import play.test.TestBrowser;
import play.test.WithBrowser;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.fakeRequest;
import static play.test.Helpers.route;

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
           .configure("auth.baseUrl", "http://localhost:" +  providePort())
           .configure("auth.oidc.discoveryUri", "http://localhost:" + oauthServerPort()
                   + "/testIssuer/.well-known/openid-configuration")
            .in(new Environment(Mode.TEST)).build();
  }

  public int oauthServerPort() {
    return providePort() + 1;
  }

  public int gmsServerPort() {
    return providePort() + 2;
  }

  @Override
  protected TestBrowser provideBrowser(int port) {
    return Helpers.testBrowser(providePort());
  }

  private MockOAuth2Server _oauthServer;
  private MockWebServer _gmsServer;

  private String _wellKnownUrl;

  @BeforeAll
  public void init() throws IOException, InterruptedException {
    _gmsServer = new MockWebServer();
    _gmsServer.enqueue(new MockResponse().setBody("{\"value\":\"urn:li:corpuser:testUser@myCompany.com\"}"));
    _gmsServer.enqueue(new MockResponse().setBody("{\"accessToken\":\"faketoken_YCpYIrjQH4sD3_rAc3VPPFg4\"}"));
    _gmsServer.start(gmsServerPort());

    _oauthServer = new MockOAuth2Server();
    _oauthServer.enqueueCallback(
            new DefaultOAuth2TokenCallback(ISSUER_ID, "testUser", List.of(), Map.of(
                    "email", "testUser@myCompany.com",
                    "groups", "myGroup"
            ), 600)
    );
    _oauthServer.start(InetAddress.getByName("localhost"), oauthServerPort());

    // Discovery url to authorization server metadata
    _wellKnownUrl = _oauthServer.wellKnownUrl(ISSUER_ID).toString();

    startServer();
    createBrowser();
    Thread.sleep(5000);
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
    assertEquals("http://localhost:" + oauthServerPort()
            + "/testIssuer/.well-known/openid-configuration", _wellKnownUrl);
  }

  @Test
  public void testHappyPathOidc() throws InterruptedException {
    browser.goTo("/authenticate");
    assertEquals("", browser.url());
    Cookie actorCookie = browser.getCookie("actor");
    assertEquals("urn:li:corpuser:testUser@myCompany.com", actorCookie.getValue());
  }

}
