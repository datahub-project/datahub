package auth.sso.oidc.custom;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import auth.sso.oidc.OidcConfigs;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.extractor.OidcCredentialsExtractor;
import org.pac4j.oidc.logout.OidcLogoutActionBuilder;
import org.pac4j.oidc.logout.processor.OidcLogoutProcessor;
import org.pac4j.oidc.metadata.OidcOpMetadataResolver;

public class CustomOidcClientTest {

  private OidcConfiguration configuration;
  private OidcConfigs oidcConfigs;
  private CustomOidcClient customOidcClient;
  private OIDCProviderMetadata providerMetadata;
  private OidcOpMetadataResolver metadataResolver;

  @BeforeEach
  void setUp() throws Exception {
    configuration = mock(OidcConfiguration.class);
    oidcConfigs = mock(OidcConfigs.class);
    providerMetadata = mock(OIDCProviderMetadata.class);
    metadataResolver = mock(OidcOpMetadataResolver.class);

    // Mock the configuration to return valid values
    when(configuration.getClientId()).thenReturn("test-client-id");
    when(configuration.getSecret()).thenReturn("test-secret");

    // Mock the configuration dependencies
    when(configuration.getOpMetadataResolver()).thenReturn(metadataResolver);
    when(metadataResolver.load()).thenReturn(providerMetadata);

    // Mock OidcConfigs retry settings
    when(oidcConfigs.getHttpRetryAttempts()).thenReturn("3");
    when(oidcConfigs.getHttpRetryDelay()).thenReturn("100");

    customOidcClient = new CustomOidcClient(configuration, oidcConfigs);
  }

  @Test
  void testInternalInitSetsUpCustomComponents() {
    // Call internalInit to set up all components
    customOidcClient.internalInit(false);

    // Verify that custom components are set up
    assertNotNull(customOidcClient.getProfileCreator());
    assertTrue(customOidcClient.getProfileCreator() instanceof CustomOidcProfileCreator);

    assertNotNull(customOidcClient.getAuthenticator());
    assertTrue(customOidcClient.getAuthenticator() instanceof CustomOidcAuthenticator);

    assertNotNull(customOidcClient.getRedirectionActionBuilder());
    assertTrue(
        customOidcClient.getRedirectionActionBuilder()
            instanceof CustomOidcRedirectionActionBuilder);

    assertNotNull(customOidcClient.getCredentialsExtractor());
    assertTrue(customOidcClient.getCredentialsExtractor() instanceof OidcCredentialsExtractor);

    assertNotNull(customOidcClient.getLogoutProcessor());
    assertTrue(customOidcClient.getLogoutProcessor() instanceof OidcLogoutProcessor);

    assertNotNull(customOidcClient.getLogoutActionBuilder());
    assertTrue(customOidcClient.getLogoutActionBuilder() instanceof OidcLogoutActionBuilder);
  }

  @Test
  void testInternalInitWithForceReinit() {
    // Set up initial authenticator
    CustomOidcAuthenticator initialAuthenticator = mock(CustomOidcAuthenticator.class);
    customOidcClient.setAuthenticator(initialAuthenticator);

    // Call internalInit with forceReinit=true
    customOidcClient.internalInit(true);

    // Verify that authenticator was replaced with new CustomOidcAuthenticator
    assertNotNull(customOidcClient.getAuthenticator());
    assertTrue(customOidcClient.getAuthenticator() instanceof CustomOidcAuthenticator);
    assertNotEquals(initialAuthenticator, customOidcClient.getAuthenticator());
  }

  @Test
  void testInternalInitWithoutForceReinit() {
    // Set up initial authenticator
    CustomOidcAuthenticator initialAuthenticator = mock(CustomOidcAuthenticator.class);
    customOidcClient.setAuthenticator(initialAuthenticator);

    // Call internalInit with forceReinit=false
    customOidcClient.internalInit(false);

    // Verify that authenticator was NOT replaced
    assertEquals(initialAuthenticator, customOidcClient.getAuthenticator());
  }

  @Test
  void testInternalInitCallsConfigurationInit() {
    // Call internalInit
    customOidcClient.internalInit(false);

    // Verify that configuration.init() was called
    verify(configuration).init(false);
  }
}
