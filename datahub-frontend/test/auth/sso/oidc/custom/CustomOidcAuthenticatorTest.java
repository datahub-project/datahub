package auth.sso.oidc.custom;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import auth.sso.oidc.OidcConfigs;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.PrivateKeyJWT;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.credentials.Credentials;
import org.pac4j.core.exception.TechnicalException;
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

    // Reset so we're not counting the invocation the constructor already made.
    reset(metadataResolver);
    when(metadataResolver.load()).thenReturn(expectedMetadata);

    OIDCProviderMetadata result = authenticator.loadWithRetry();

    assertNotNull(result);
    assertEquals(expectedMetadata, result);
    verify(metadataResolver, times(1)).load();
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
  void testConstructor_PrivateKeyJwtMethod() throws Exception {
    // Setup for private_key_jwt authentication
    OidcConfiguration pkjConfiguration = mock(OidcConfiguration.class);
    OidcConfigs pkjOidcConfigs = mock(OidcConfigs.class);
    OidcClient pkjClient = mock(OidcClient.class);
    OidcOpMetadataResolver pkjMetadataResolver = mock(OidcOpMetadataResolver.class);
    OIDCProviderMetadata pkjProviderMetadata = mock(OIDCProviderMetadata.class);

    when(pkjClient.getConfiguration()).thenReturn(pkjConfiguration);
    when(pkjOidcConfigs.getHttpRetryAttempts()).thenReturn("3");
    when(pkjOidcConfigs.getHttpRetryDelay()).thenReturn("100");

    when(pkjConfiguration.getClientId()).thenReturn("test-client-id");
    when(pkjConfiguration.getOpMetadataResolver()).thenReturn(pkjMetadataResolver);
    when(pkjMetadataResolver.load()).thenReturn(pkjProviderMetadata);

    // Configure provider metadata to support private_key_jwt
    when(pkjProviderMetadata.getTokenEndpointAuthMethods())
        .thenReturn(List.of(ClientAuthenticationMethod.PRIVATE_KEY_JWT));
    when(pkjProviderMetadata.getTokenEndpointURI())
        .thenReturn(new URI("https://example.com/token"));

    // Configure private_key_jwt method with test key files
    when(pkjConfiguration.getClientAuthenticationMethod())
        .thenReturn(ClientAuthenticationMethod.PRIVATE_KEY_JWT);
    when(pkjOidcConfigs.getPrivateKeyFilePath())
        .thenReturn(Optional.of(auth.sso.oidc.TestKeyMaterial.PRIVATE_KEY_PATH));
    when(pkjOidcConfigs.getCertificateFilePath())
        .thenReturn(Optional.of(auth.sso.oidc.TestKeyMaterial.CERTIFICATE_PATH));
    when(pkjOidcConfigs.getPrivateKeyPassword()).thenReturn(Optional.empty());
    when(pkjOidcConfigs.getPrivateKeyJwtKid()).thenReturn(Optional.empty());
    when(pkjOidcConfigs.getPrivateKeyJwtAlgorithm()).thenReturn("RS256");

    // Create authenticator with private_key_jwt config
    CustomOidcAuthenticator pkjAuthenticator =
        new CustomOidcAuthenticator(pkjClient, pkjOidcConfigs);

    assertNotNull(pkjAuthenticator);
  }

  @Test
  void testPrivateKeyJwt_FreshAssertionPerTokenRequest() throws Exception {
    // Regression test: the private_key_jwt assertion must be re-signed on every token request
    // so its `exp` claim is never stale. Building it once at startup and caching the
    // ClientAuthentication would let it expire ~5 minutes into server uptime.

    OidcConfiguration pkjConfiguration = mock(OidcConfiguration.class);
    OidcConfigs pkjOidcConfigs = mock(OidcConfigs.class);
    OidcClient pkjClient = mock(OidcClient.class);
    OidcOpMetadataResolver pkjMetadataResolver = mock(OidcOpMetadataResolver.class);
    OIDCProviderMetadata pkjProviderMetadata = mock(OIDCProviderMetadata.class);

    when(pkjClient.getConfiguration()).thenReturn(pkjConfiguration);
    when(pkjOidcConfigs.getHttpRetryAttempts()).thenReturn("3");
    when(pkjOidcConfigs.getHttpRetryDelay()).thenReturn("100");
    when(pkjConfiguration.getClientId()).thenReturn("test-client-id");
    when(pkjConfiguration.getScope()).thenReturn("openid profile email");
    when(pkjConfiguration.getOpMetadataResolver()).thenReturn(pkjMetadataResolver);
    when(pkjMetadataResolver.load()).thenReturn(pkjProviderMetadata);
    when(pkjProviderMetadata.getTokenEndpointAuthMethods())
        .thenReturn(List.of(ClientAuthenticationMethod.PRIVATE_KEY_JWT));
    when(pkjProviderMetadata.getTokenEndpointURI())
        .thenReturn(new URI("https://example.com/token"));
    when(pkjConfiguration.getClientAuthenticationMethod())
        .thenReturn(ClientAuthenticationMethod.PRIVATE_KEY_JWT);
    when(pkjOidcConfigs.getPrivateKeyFilePath())
        .thenReturn(Optional.of(auth.sso.oidc.TestKeyMaterial.PRIVATE_KEY_PATH));
    when(pkjOidcConfigs.getCertificateFilePath())
        .thenReturn(Optional.of(auth.sso.oidc.TestKeyMaterial.CERTIFICATE_PATH));
    when(pkjOidcConfigs.getPrivateKeyPassword()).thenReturn(Optional.empty());
    when(pkjOidcConfigs.getPrivateKeyJwtKid()).thenReturn(Optional.empty());
    when(pkjOidcConfigs.getPrivateKeyJwtAlgorithm()).thenReturn("RS256");

    // Subclass exposes protected createTokenRequest so the test can invoke it directly.
    class ExposedAuthenticator extends CustomOidcAuthenticator {
      ExposedAuthenticator(OidcClient c, OidcConfigs o) {
        super(c, o);
      }

      TokenRequest callCreateTokenRequest(AuthorizationCodeGrant grant) {
        return createTokenRequest(grant);
      }
    }

    ExposedAuthenticator auth = new ExposedAuthenticator(pkjClient, pkjOidcConfigs);

    AuthorizationCodeGrant grant =
        new AuthorizationCodeGrant(new AuthorizationCode("abc"), new URI("https://dh/callback"));

    TokenRequest first = auth.callCreateTokenRequest(grant);
    // Sleep ≥1ms to ensure iat/jti differ deterministically across the two signings.
    Thread.sleep(5);
    TokenRequest second = auth.callCreateTokenRequest(grant);

    PrivateKeyJWT firstAuth = (PrivateKeyJWT) first.getClientAuthentication();
    PrivateKeyJWT secondAuth = (PrivateKeyJWT) second.getClientAuthentication();

    SignedJWT firstJwt = firstAuth.getClientAssertion();
    SignedJWT secondJwt = secondAuth.getClientAssertion();

    // Distinct signings => distinct serialized JWTs, distinct jti, and distinct signatures.
    assertNotEquals(firstJwt.serialize(), secondJwt.serialize());
    assertNotEquals(firstJwt.getJWTClaimsSet().getJWTID(), secondJwt.getJWTClaimsSet().getJWTID());
  }

  @Test
  void testPrivateKeyJwt_KidOverrideFlowsIntoSignedJwtHeader() throws Exception {
    // Operators configure AUTH_OIDC_PRIVATE_KEY_JWT_KID to match the kid registered in their
    // IdP's JWK set (Keycloak, Okta, Auth0, ...). Verify the override flows into the signed JWT
    // header; without it the whole "works for any OIDC vendor" claim is aspirational.

    OidcConfiguration pkjConfiguration = mock(OidcConfiguration.class);
    OidcConfigs pkjOidcConfigs = mock(OidcConfigs.class);
    OidcClient pkjClient = mock(OidcClient.class);
    OidcOpMetadataResolver pkjMetadataResolver = mock(OidcOpMetadataResolver.class);
    OIDCProviderMetadata pkjProviderMetadata = mock(OIDCProviderMetadata.class);

    when(pkjClient.getConfiguration()).thenReturn(pkjConfiguration);
    when(pkjOidcConfigs.getHttpRetryAttempts()).thenReturn("3");
    when(pkjOidcConfigs.getHttpRetryDelay()).thenReturn("100");
    when(pkjConfiguration.getClientId()).thenReturn("test-client-id");
    when(pkjConfiguration.getScope()).thenReturn("openid profile email");
    when(pkjConfiguration.getOpMetadataResolver()).thenReturn(pkjMetadataResolver);
    when(pkjMetadataResolver.load()).thenReturn(pkjProviderMetadata);
    when(pkjProviderMetadata.getTokenEndpointAuthMethods())
        .thenReturn(List.of(ClientAuthenticationMethod.PRIVATE_KEY_JWT));
    when(pkjProviderMetadata.getTokenEndpointURI())
        .thenReturn(new URI("https://example.com/token"));
    when(pkjConfiguration.getClientAuthenticationMethod())
        .thenReturn(ClientAuthenticationMethod.PRIVATE_KEY_JWT);
    when(pkjOidcConfigs.getPrivateKeyFilePath())
        .thenReturn(Optional.of(auth.sso.oidc.TestKeyMaterial.PRIVATE_KEY_PATH));
    when(pkjOidcConfigs.getCertificateFilePath())
        .thenReturn(Optional.of(auth.sso.oidc.TestKeyMaterial.CERTIFICATE_PATH));
    when(pkjOidcConfigs.getPrivateKeyPassword()).thenReturn(Optional.empty());
    when(pkjOidcConfigs.getPrivateKeyJwtKid()).thenReturn(Optional.of("keycloak-client-kid-42"));
    when(pkjOidcConfigs.getPrivateKeyJwtAlgorithm()).thenReturn("RS256");

    class ExposedAuthenticator extends CustomOidcAuthenticator {
      ExposedAuthenticator(OidcClient c, OidcConfigs o) {
        super(c, o);
      }

      TokenRequest callCreateTokenRequest(AuthorizationCodeGrant grant) {
        return createTokenRequest(grant);
      }
    }

    ExposedAuthenticator auth = new ExposedAuthenticator(pkjClient, pkjOidcConfigs);
    TokenRequest request =
        auth.callCreateTokenRequest(
            new AuthorizationCodeGrant(
                new AuthorizationCode("abc"), new URI("https://dh/callback")));

    PrivateKeyJWT clientAuth = (PrivateKeyJWT) request.getClientAuthentication();
    SignedJWT jwt = clientAuth.getClientAssertion();

    assertEquals("keycloak-client-kid-42", jwt.getHeader().getKeyID());
    // x5t#S256 must still be populated so IdPs that match on thumbprint also work alongside
    // the overridden kid.
    assertNotNull(jwt.getHeader().getX509CertSHA256Thumbprint());
  }

  @Test
  void testPrivateKeyJwt_UnsupportedAlgorithmFailsAtStartup() throws Exception {
    // Nimbus's JWSAlgorithm.parse silently accepts any string. Verify we reject unsupported
    // algorithms eagerly at construction time rather than letting them explode later inside the
    // signer with a confusing error.

    OidcConfiguration pkjConfiguration = mock(OidcConfiguration.class);
    OidcConfigs pkjOidcConfigs = mock(OidcConfigs.class);
    OidcClient pkjClient = mock(OidcClient.class);
    OidcOpMetadataResolver pkjMetadataResolver = mock(OidcOpMetadataResolver.class);
    OIDCProviderMetadata pkjProviderMetadata = mock(OIDCProviderMetadata.class);

    when(pkjClient.getConfiguration()).thenReturn(pkjConfiguration);
    when(pkjOidcConfigs.getHttpRetryAttempts()).thenReturn("3");
    when(pkjOidcConfigs.getHttpRetryDelay()).thenReturn("100");
    when(pkjConfiguration.getClientId()).thenReturn("test-client-id");
    when(pkjConfiguration.getOpMetadataResolver()).thenReturn(pkjMetadataResolver);
    when(pkjMetadataResolver.load()).thenReturn(pkjProviderMetadata);
    when(pkjProviderMetadata.getTokenEndpointAuthMethods())
        .thenReturn(List.of(ClientAuthenticationMethod.PRIVATE_KEY_JWT));
    when(pkjProviderMetadata.getTokenEndpointURI())
        .thenReturn(new URI("https://example.com/token"));
    when(pkjConfiguration.getClientAuthenticationMethod())
        .thenReturn(ClientAuthenticationMethod.PRIVATE_KEY_JWT);
    when(pkjOidcConfigs.getPrivateKeyFilePath())
        .thenReturn(Optional.of(auth.sso.oidc.TestKeyMaterial.PRIVATE_KEY_PATH));
    when(pkjOidcConfigs.getCertificateFilePath())
        .thenReturn(Optional.of(auth.sso.oidc.TestKeyMaterial.CERTIFICATE_PATH));
    when(pkjOidcConfigs.getPrivateKeyPassword()).thenReturn(Optional.empty());
    when(pkjOidcConfigs.getPrivateKeyJwtKid()).thenReturn(Optional.empty());
    when(pkjOidcConfigs.getPrivateKeyJwtAlgorithm()).thenReturn("NOPE");

    assertThrows(
        TechnicalException.class, () -> new CustomOidcAuthenticator(pkjClient, pkjOidcConfigs));
  }

  @Test
  void testConstructor_PrivateKeyJwtMethod_MissingPrivateKeyPath() throws Exception {
    // Setup for private_key_jwt with missing key path
    OidcConfiguration pkjConfiguration = mock(OidcConfiguration.class);
    OidcConfigs pkjOidcConfigs = mock(OidcConfigs.class);
    OidcClient pkjClient = mock(OidcClient.class);
    OidcOpMetadataResolver pkjMetadataResolver = mock(OidcOpMetadataResolver.class);
    OIDCProviderMetadata pkjProviderMetadata = mock(OIDCProviderMetadata.class);

    when(pkjClient.getConfiguration()).thenReturn(pkjConfiguration);
    when(pkjOidcConfigs.getHttpRetryAttempts()).thenReturn("3");
    when(pkjOidcConfigs.getHttpRetryDelay()).thenReturn("100");

    when(pkjConfiguration.getClientId()).thenReturn("test-client-id");
    when(pkjConfiguration.getOpMetadataResolver()).thenReturn(pkjMetadataResolver);
    when(pkjMetadataResolver.load()).thenReturn(pkjProviderMetadata);

    when(pkjProviderMetadata.getTokenEndpointAuthMethods())
        .thenReturn(List.of(ClientAuthenticationMethod.PRIVATE_KEY_JWT));
    when(pkjProviderMetadata.getTokenEndpointURI())
        .thenReturn(new URI("https://example.com/token"));

    when(pkjConfiguration.getClientAuthenticationMethod())
        .thenReturn(ClientAuthenticationMethod.PRIVATE_KEY_JWT);
    // Missing private key path
    when(pkjOidcConfigs.getPrivateKeyFilePath()).thenReturn(Optional.empty());
    when(pkjOidcConfigs.getCertificateFilePath())
        .thenReturn(Optional.of(auth.sso.oidc.TestKeyMaterial.CERTIFICATE_PATH));

    // Should throw TechnicalException because privateKeyFilePath is missing
    assertThrows(
        TechnicalException.class, () -> new CustomOidcAuthenticator(pkjClient, pkjOidcConfigs));
  }
}
