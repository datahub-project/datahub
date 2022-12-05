package app;

import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;
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
import play.test.Helpers;

import play.test.TestBrowser;
import play.test.WithBrowser;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SetEnvironmentVariable(key = "DATAHUB_SECRET", value = "test")
@SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVER", value = "")
@SetEnvironmentVariable(key = "DATAHUB_ANALYTICS_ENABLED", value = "false")
@SetEnvironmentVariable(key = "AUTH_OIDC_ENABLED", value = "true")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_ID", value = "testclient")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_SECRET", value = "testsecret")
@SetEnvironmentVariable(key = "AUTH_OIDC_DISCOVERY_URI", value = "http://localhost:51152/testIssuer/.well-known/openid-configuration")
@SetEnvironmentVariable(key = "AUTH_OIDC_BASE_URL", value = "http://localhost:51153")
public class ApplicationOidcTest extends WithBrowser {
  private static final String ISSUER_ID = "testIssuer";

  @Override
  protected Application provideApplication() {
    return new GuiceApplicationBuilder().in(new Environment(Mode.TEST)).build();
  }

  @Override
  protected int providePort() {
    return 51153;
  }

  @Override
  protected TestBrowser provideBrowser(int port) {
    return Helpers.testBrowser(providePort());
  }

  private MockOAuth2Server _oauthServer;
  private String _wellKnownUrl;

  @BeforeAll
  public void init() throws IOException, InterruptedException {
    _oauthServer = new MockOAuth2Server();
    _oauthServer.start(InetAddress.getByName("localhost"), 51152);
    _oauthServer.enqueueCallback(
            new DefaultOAuth2TokenCallback(ISSUER_ID, "testUser", List.of(), Map.of(
                    "email", "testUser@myCompany.com",
                    "groups", "myGroup"
            ), 600)
    );

    // Discovery url to authorization server metadata
    _wellKnownUrl = _oauthServer.wellKnownUrl(ISSUER_ID).toString();

    createBrowser();
    startServer();

    Thread.sleep(5000);
  }

  @AfterAll
  public void shutdown() throws IOException {
    _oauthServer.shutdown();
    stopServer();
  }

  @Test
  public void testOpenIdConfig() {
    assertEquals("http://localhost:51152/testIssuer/.well-known/openid-configuration", _wellKnownUrl);
  }

  @Test
  public void testHappyPathOidc() {
    browser.goTo("/authenticate");
    assertEquals("", browser.url());
    Cookie actorCookie = browser.getCookie("actor");
    assertEquals("urn:li:corpuser:testUser@myCompany.com", actorCookie.getValue());
  }

}
