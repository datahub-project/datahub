package auth.sso.oidc.custom;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import auth.sso.oidc.OidcConfigs;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.oidc.client.OidcClient;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.metadata.OidcOpMetadataResolver;

public class CustomOidcProfileCreatorTest {

  private OidcConfiguration configuration;
  private OidcClient client;
  private OidcConfigs oidcConfigs;
  private CustomOidcProfileCreator profileCreator;
  private URI userInfoEndpointUri;
  private UserProfile profile;
  private AccessToken accessToken;

  @BeforeEach
  void setUp() {
    userInfoEndpointUri = URI.create("https://nonexistent-endpoint-12345.com/userinfo");
    configuration = mock(OidcConfiguration.class);
    client = mock(OidcClient.class);
    oidcConfigs = mock(OidcConfigs.class);
    profile = mock(UserProfile.class);
    accessToken = mock(AccessToken.class);

    // Mock the OidcOpMetadataResolver to avoid NullPointerException
    OidcOpMetadataResolver metadataResolver = mock(OidcOpMetadataResolver.class);
    when(configuration.getOpMetadataResolver()).thenReturn(metadataResolver);

    // Mock the OIDCProviderMetadata to avoid NullPointerException
    OIDCProviderMetadata providerMetadata = mock(OIDCProviderMetadata.class);
    when(metadataResolver.load()).thenReturn(providerMetadata);

    // Set up the provider metadata to return a valid URI (the actual network call will fail)
    when(providerMetadata.getUserInfoEndpointURI()).thenReturn(userInfoEndpointUri);

    profileCreator = new CustomOidcProfileCreator(configuration, client, oidcConfigs);
  }

  @Test
  void testRetryMechanismWithValidConfig() throws Exception {
    when(oidcConfigs.getHttpRetryAttempts()).thenReturn("3");
    when(oidcConfigs.getHttpRetryDelay()).thenReturn("100");

    // Should throw IOException after retries are exhausted (network error from non-existent
    // endpoint)
    IOException exception =
        assertThrows(
            IOException.class,
            () -> {
              profileCreator.callUserInfoEndpoint(userInfoEndpointUri, accessToken, profile);
            });

    // Verify the exception is a network-related exception
    assertTrue(exception instanceof java.net.UnknownHostException);
  }

  @Test
  void testRetryMechanismWithInvalidConfig() throws Exception {
    when(oidcConfigs.getHttpRetryAttempts()).thenReturn("invalid");
    when(oidcConfigs.getHttpRetryDelay()).thenReturn("invalid");

    // Should handle invalid config gracefully and still attempt once (defaults to 1)
    IOException exception =
        assertThrows(
            IOException.class,
            () -> {
              profileCreator.callUserInfoEndpoint(userInfoEndpointUri, accessToken, profile);
            });

    // Verify the exception is a network-related exception
    assertTrue(exception instanceof java.net.UnknownHostException);
  }

  @Test
  void testRetryMechanismWithZeroAttempts() throws Exception {
    when(oidcConfigs.getHttpRetryAttempts()).thenReturn("0");
    when(oidcConfigs.getHttpRetryDelay()).thenReturn("100");

    // Should default to 1 attempt and throw IOException
    IOException exception =
        assertThrows(
            IOException.class,
            () -> {
              profileCreator.callUserInfoEndpoint(userInfoEndpointUri, accessToken, profile);
            });

    // Verify the exception is a network-related exception
    assertTrue(exception instanceof java.net.UnknownHostException);
  }

  @Test
  void testRetryMechanismWithNegativeAttempts() throws Exception {
    when(oidcConfigs.getHttpRetryAttempts()).thenReturn("-1");
    when(oidcConfigs.getHttpRetryDelay()).thenReturn("100");

    // Should default to 1 attempt and throw IOException
    IOException exception =
        assertThrows(
            IOException.class,
            () -> {
              profileCreator.callUserInfoEndpoint(userInfoEndpointUri, accessToken, profile);
            });

    // Verify the exception is a network-related exception
    assertTrue(exception instanceof java.net.UnknownHostException);
  }

  @Test
  void testRetryMechanismStackTraceDebug() throws Exception {
    when(oidcConfigs.getHttpRetryAttempts()).thenReturn("2");
    when(oidcConfigs.getHttpRetryDelay()).thenReturn("50");

    try {
      profileCreator.callUserInfoEndpoint(userInfoEndpointUri, accessToken, profile);
      fail("Expected IOException to be thrown");
    } catch (IOException e) {
      // Print stack trace for debugging
      System.out.println("=== Stack Trace Debug ===");
      e.printStackTrace();
      System.out.println("=== End Stack Trace ===");

      // Verify the stack trace contains our retry method
      StackTraceElement[] stackTrace = e.getStackTrace();
      boolean foundRetryMethod = false;

      for (StackTraceElement element : stackTrace) {
        String className = element.getClassName();
        String methodName = element.getMethodName();

        if (className.contains("CustomOidcProfileCreator")
            && methodName.contains("callUserInfoEndpointWithRetry")) {
          foundRetryMethod = true;
          System.out.println("Found retry method: " + className + "." + methodName);
        }
      }

      assertTrue(
          foundRetryMethod, "Stack trace should contain callUserInfoEndpointWithRetry method");
    }
  }
}
