package auth.sso.oidc.custom;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import auth.sso.oidc.OidcConfigs;
import auth.sso.oidc.TestKeyMaterial;
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
import org.junit.jupiter.api.Test;
import org.pac4j.core.exception.TechnicalException;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.metadata.OidcOpMetadataResolver;

/** Tests for the {@code private_key_jwt} client authentication path. */
public class CustomOidcAuthenticatorTest {

  private static final URI TOKEN_ENDPOINT = URI.create("https://example.com/token");
  private static final AuthorizationCodeGrant GRANT =
      new AuthorizationCodeGrant(new AuthorizationCode("abc"), URI.create("https://dh/cb"));

  @Test
  void freshAssertionPerTokenRequest() throws Exception {
    // Regression: the JWT must be re-signed on every token request so its `exp` is never stale.
    CustomOidcAuthenticator auth = newPkjAuthenticator(Optional.empty(), "RS256");

    SignedJWT first = signedAssertion(auth.createTokenRequest(GRANT));
    Thread.sleep(5); // ensure iat/jti differ deterministically
    SignedJWT second = signedAssertion(auth.createTokenRequest(GRANT));

    assertNotEquals(first.serialize(), second.serialize());
    assertNotEquals(first.getJWTClaimsSet().getJWTID(), second.getJWTClaimsSet().getJWTID());
  }

  @Test
  void kidOverrideFlowsIntoSignedJwtHeader() throws Exception {
    CustomOidcAuthenticator auth =
        newPkjAuthenticator(Optional.of("keycloak-client-kid-42"), "RS256");

    SignedJWT jwt = signedAssertion(auth.createTokenRequest(GRANT));

    assertEquals("keycloak-client-kid-42", jwt.getHeader().getKeyID());
    // x5t#S256 must still be populated alongside the override so thumbprint-matching IdPs still
    // work.
    assertNotNull(jwt.getHeader().getX509CertSHA256Thumbprint());
  }

  @Test
  void unsupportedAlgorithmFailsAtStartup() {
    // Nimbus's JWSAlgorithm.parse silently accepts any string; we validate eagerly.
    assertThrows(TechnicalException.class, () -> newPkjAuthenticator(Optional.empty(), "NOPE"));
  }

  @Test
  void missingPrivateKeyPathFailsAtStartup() {
    OidcClient client = mock(OidcClient.class);
    OidcConfigs configs = mock(OidcConfigs.class);
    stubMetadataAndRetries(client, configs);
    when(configs.getPrivateKeyFilePath()).thenReturn(Optional.empty());
    when(configs.getCertificateFilePath())
        .thenReturn(Optional.of(TestKeyMaterial.CERTIFICATE_PATH));

    assertThrows(TechnicalException.class, () -> new CustomOidcAuthenticator(client, configs));
  }

  private static SignedJWT signedAssertion(TokenRequest request) {
    return ((PrivateKeyJWT) request.getClientAuthentication()).getClientAssertion();
  }

  /** Builds a fully-configured private_key_jwt authenticator using ephemeral test key material. */
  private static CustomOidcAuthenticator newPkjAuthenticator(
      Optional<String> kidOverride, String algorithm) {
    OidcClient client = mock(OidcClient.class);
    OidcConfigs configs = mock(OidcConfigs.class);
    stubMetadataAndRetries(client, configs);
    when(configs.getPrivateKeyFilePath()).thenReturn(Optional.of(TestKeyMaterial.PRIVATE_KEY_PATH));
    when(configs.getCertificateFilePath())
        .thenReturn(Optional.of(TestKeyMaterial.CERTIFICATE_PATH));
    when(configs.getPrivateKeyPassword()).thenReturn(Optional.empty());
    when(configs.getPrivateKeyJwtKid()).thenReturn(kidOverride);
    when(configs.getPrivateKeyJwtAlgorithm()).thenReturn(algorithm);
    return new CustomOidcAuthenticator(client, configs);
  }

  private static void stubMetadataAndRetries(OidcClient client, OidcConfigs configs) {
    OidcConfiguration configuration = mock(OidcConfiguration.class);
    OidcOpMetadataResolver resolver = mock(OidcOpMetadataResolver.class);
    OIDCProviderMetadata metadata = mock(OIDCProviderMetadata.class);

    when(client.getConfiguration()).thenReturn(configuration);
    when(configuration.getClientId()).thenReturn("test-client-id");
    when(configuration.getScope()).thenReturn("openid profile email");
    when(configuration.getClientAuthenticationMethod())
        .thenReturn(ClientAuthenticationMethod.PRIVATE_KEY_JWT);
    when(configuration.getOpMetadataResolver()).thenReturn(resolver);
    when(resolver.load()).thenReturn(metadata);
    when(metadata.getTokenEndpointAuthMethods())
        .thenReturn(List.of(ClientAuthenticationMethod.PRIVATE_KEY_JWT));
    when(metadata.getTokenEndpointURI()).thenReturn(TOKEN_ENDPOINT);
    when(configs.getHttpRetryAttempts()).thenReturn("3");
    when(configs.getHttpRetryDelay()).thenReturn("100");
  }
}
