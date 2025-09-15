package auth.sso.oidc.custom;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import auth.sso.oidc.OidcConfigs;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.credentials.Credentials;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.OidcCredentials;
import org.pac4j.oidc.metadata.OidcOpMetadataResolver;

public class CustomOidcAuthenticatorTest {

  private OidcConfiguration configuration;
  private OidcConfigs oidcConfigs;
  private CustomOidcAuthenticator authenticator;
  private OidcClient client;
  private CallContext callContext;
  private WebContext webContext;
  private SessionStore sessionStore;
  private OidcOpMetadataResolver metadataResolver;
  private OIDCProviderMetadata providerMetadata;

  @BeforeEach
  void setUp() throws Exception {
    configuration = mock(OidcConfiguration.class);
    oidcConfigs = mock(OidcConfigs.class);
    client = mock(OidcClient.class);
    webContext = mock(WebContext.class);
    sessionStore = mock(SessionStore.class);
    callContext = new CallContext(webContext, sessionStore);

    when(client.getConfiguration()).thenReturn(configuration);
    when(oidcConfigs.getHttpRetryAttempts()).thenReturn("3");
    when(oidcConfigs.getHttpRetryDelay()).thenReturn("100");

    // Mock the configuration to return valid values
    when(configuration.getClientId()).thenReturn("test-client-id");
    when(configuration.getSecret()).thenReturn("test-secret");

    // Mock the metadata resolver to avoid initialization issues
    metadataResolver = mock(OidcOpMetadataResolver.class);
    providerMetadata = mock(OIDCProviderMetadata.class);
    when(configuration.getOpMetadataResolver()).thenReturn(metadataResolver);
    when(metadataResolver.load()).thenReturn(providerMetadata);

    authenticator = new CustomOidcAuthenticator(client, oidcConfigs);
  }

  @Test
  void testLoadWithRetrySuccessOnFirstAttempt() throws Exception {
    OIDCProviderMetadata expectedMetadata = mock(OIDCProviderMetadata.class);
    when(metadataResolver.load()).thenReturn(expectedMetadata);

    OIDCProviderMetadata result = authenticator.loadWithRetry();

    assertNotNull(result);
    assertEquals(expectedMetadata, result);
    // Constructor calls loadWithRetry() once, test calls it once more
    verify(metadataResolver, times(2)).load();
  }

  @Test
  void testLoadWithRetryRetriesOnFailure() throws Exception {
    OIDCProviderMetadata expectedMetadata = mock(OIDCProviderMetadata.class);

    // Reset the mock to start fresh for this test
    reset(metadataResolver);

    // First two calls fail, third succeeds
    when(metadataResolver.load())
        .thenThrow(new RuntimeException("Network error"))
        .thenThrow(new RuntimeException("Network error"))
        .thenReturn(expectedMetadata);

    OIDCProviderMetadata result = authenticator.loadWithRetry();

    assertNotNull(result);
    assertEquals(expectedMetadata, result);
    // Test calls it 3 times (2 failures + 1 success)
    verify(metadataResolver, times(3)).load();
  }

  @Test
  void testLoadWithRetryFailsAfterMaxAttempts() throws Exception {
    // Reset the mock to start fresh for this test
    reset(metadataResolver);

    when(metadataResolver.load()).thenThrow(new RuntimeException("Persistent network error"));

    assertThrows(
        RuntimeException.class,
        () -> {
          authenticator.loadWithRetry();
        });

    // Test calls it 3 times (all failures)
    verify(metadataResolver, times(3)).load();
  }

  @Test
  void testValidateWithInvalidCredentials() throws Exception {
    OidcCredentials credentials = mock(OidcCredentials.class);
    when(credentials.getCode()).thenReturn(null); // Invalid - no auth code

    Optional<Credentials> result = authenticator.validate(callContext, credentials);

    // When code is null, the method returns the credentials object (not empty)
    assertTrue(result.isPresent());
    assertEquals(credentials, result.get());
  }

  @Test
  void testValidateWithNullCredentials() throws Exception {
    // The validate method doesn't handle null credentials gracefully, so we expect an exception
    assertThrows(
        NullPointerException.class,
        () -> {
          authenticator.validate(callContext, null);
        });
  }

  @Test
  void testConstructorInitializesCorrectly() {
    assertNotNull(authenticator);
  }
}
