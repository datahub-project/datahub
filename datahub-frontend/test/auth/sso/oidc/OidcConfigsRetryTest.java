package auth.sso.oidc;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OidcConfigsRetryTest {

  private OidcConfigs oidcConfigs;

  @BeforeEach
  void setUp() {
    String configString =
        """
        auth {
          oidc {
            clientId = "test-client-id"
            clientSecret = "test-client-secret"
            discoveryUri = "https://example.com/.well-known/openid_configuration"
          }
          baseUrl = "https://example.com"
          enabled = true
        }
        """;

    Config config = ConfigFactory.parseString(configString);
    OidcConfigs.Builder builder = new OidcConfigs.Builder();
    builder.from(config);
    oidcConfigs = new OidcConfigs(builder);
  }

  @Test
  void testDefaultHttpRetryAttempts() {
    assertEquals("3", oidcConfigs.getHttpRetryAttempts());
  }

  @Test
  void testDefaultHttpRetryDelay() {
    assertEquals("1000", oidcConfigs.getHttpRetryDelay());
  }

  @Test
  void testRetryConfigurationWithCustomValues() {
    String configString =
        """
        auth {
          oidc {
            clientId = "test-client-id"
            clientSecret = "test-client-secret"
            discoveryUri = "https://example.com/.well-known/openid_configuration"
            httpRetryAttempts = "5"
            httpRetryDelay = "2000"
          }
          baseUrl = "https://example.com"
          enabled = true
        }
        """;

    Config config = ConfigFactory.parseString(configString);
    OidcConfigs.Builder builder = new OidcConfigs.Builder();
    builder.from(config);
    OidcConfigs customConfigs = new OidcConfigs(builder);

    assertEquals("5", customConfigs.getHttpRetryAttempts());
    assertEquals("2000", customConfigs.getHttpRetryDelay());
  }

  @Test
  void testRetryConfigurationCombination() {
    String configString =
        """
        auth {
          oidc {
            clientId = "test-client-id"
            clientSecret = "test-client-secret"
            discoveryUri = "https://example.com/.well-known/openid_configuration"
            httpRetryAttempts = "4"
            httpRetryDelay = "1500"
          }
          baseUrl = "https://example.com"
          enabled = true
        }
        """;

    Config config = ConfigFactory.parseString(configString);
    OidcConfigs.Builder builder = new OidcConfigs.Builder();
    builder.from(config);
    OidcConfigs customConfigs = new OidcConfigs(builder);

    assertEquals("4", customConfigs.getHttpRetryAttempts());
    assertEquals("1500", customConfigs.getHttpRetryDelay());
  }

  @Test
  void testRetryConfigurationWithZeroAttempts() {
    String configString =
        """
        auth {
          oidc {
            clientId = "test-client-id"
            clientSecret = "test-client-secret"
            discoveryUri = "https://example.com/.well-known/openid_configuration"
            httpRetryAttempts = "0"
            httpRetryDelay = "500"
          }
          baseUrl = "https://example.com"
          enabled = true
        }
        """;

    Config config = ConfigFactory.parseString(configString);
    OidcConfigs.Builder builder = new OidcConfigs.Builder();
    builder.from(config);
    OidcConfigs customConfigs = new OidcConfigs(builder);

    assertEquals("0", customConfigs.getHttpRetryAttempts());
    assertEquals("500", customConfigs.getHttpRetryDelay());
  }

  @Test
  void testRetryConfigurationWithHighValues() {
    String configString =
        """
        auth {
          oidc {
            clientId = "test-client-id"
            clientSecret = "test-client-secret"
            discoveryUri = "https://example.com/.well-known/openid_configuration"
            httpRetryAttempts = "10"
            httpRetryDelay = "5000"
          }
          baseUrl = "https://example.com"
          enabled = true
        }
        """;

    Config config = ConfigFactory.parseString(configString);
    OidcConfigs.Builder builder = new OidcConfigs.Builder();
    builder.from(config);
    OidcConfigs customConfigs = new OidcConfigs(builder);

    assertEquals("10", customConfigs.getHttpRetryAttempts());
    assertEquals("5000", customConfigs.getHttpRetryDelay());
  }
}
