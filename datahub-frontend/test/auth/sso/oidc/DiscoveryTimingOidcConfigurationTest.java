package auth.sso.oidc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pac4j.oidc.exceptions.OidcException;
import org.slf4j.LoggerFactory;

public class DiscoveryTimingOidcConfigurationTest {

  private Logger logger;
  private ListAppender<ILoggingEvent> logs;

  @BeforeEach
  void setUp() {
    logger = (Logger) LoggerFactory.getLogger(DiscoveryTimingOidcConfiguration.class);
    logger.setLevel(Level.INFO);
    logs = new ListAppender<>();
    logs.start();
    logger.addAppender(logs);
  }

  @AfterEach
  void tearDown() {
    logger.detachAppender(logs);
  }

  @Test
  void logsDurationWhenDiscoveryDocumentIsReturned() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      final String document = discoveryDocument(server);
      server.enqueue(new MockResponse().setBody(document));
      // init() reads the document, then asks the same URL for Last-Modified.
      server.enqueue(new MockResponse().setBody(document));

      assertNotNull(configuration(server).createNewOpMetadataResolver());

      assertFetchLogged("success");
    }
  }

  @Test
  void logsDurationWhenDiscoveryDocumentCannotBeParsed() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      server.enqueue(new MockResponse().setResponseCode(200).setBody("not-json"));

      final DiscoveryTimingOidcConfiguration configuration = configuration(server);
      assertThrows(OidcException.class, configuration::createNewOpMetadataResolver);

      assertFetchLogged("failed");
    }
  }

  @Test
  void logsDurationWhenDiscoveryReadTimesOut() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.start();
      server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));

      final DiscoveryTimingOidcConfiguration configuration = configuration(server);
      configuration.setConnectTimeout(200);
      configuration.setReadTimeout(200);
      assertThrows(RuntimeException.class, configuration::createNewOpMetadataResolver);

      assertFetchLogged("failed");
    }
  }

  private static DiscoveryTimingOidcConfiguration configuration(final MockWebServer server) {
    final DiscoveryTimingOidcConfiguration configuration = new DiscoveryTimingOidcConfiguration();
    configuration.setClientId("test-client");
    configuration.setConnectTimeout(500);
    configuration.setReadTimeout(500);
    configuration.setDiscoveryURI(server.url("/.well-known/openid-configuration").toString());
    return configuration;
  }

  private static String discoveryDocument(final MockWebServer server) {
    final String base = server.url("/").toString().replaceAll("/$", "");
    return """
        {
          "issuer": "%s",
          "authorization_endpoint": "%s/authorize",
          "token_endpoint": "%s/token",
          "jwks_uri": "%s/jwks",
          "response_types_supported": ["code"],
          "subject_types_supported": ["public"],
          "id_token_signing_alg_values_supported": ["RS256"]
        }
        """
        .formatted(base, base, base, base);
  }

  private void assertFetchLogged(final String outcome) {
    assertTrue(
        logs.list.stream()
            .anyMatch(
                event ->
                    event.getLevel() == Level.INFO
                        && event
                            .getFormattedMessage()
                            .matches(
                                "OIDC provider metadata fetch completed in \\d+ ms outcome="
                                    + outcome)),
        () -> "expected outcome=" + outcome + " but saw " + logs.list);
  }
}
