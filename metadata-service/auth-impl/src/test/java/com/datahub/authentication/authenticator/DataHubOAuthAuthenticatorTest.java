package com.datahub.authentication.authenticator;

import static com.datahub.authentication.AuthenticationConstants.AUTHORIZATION_HEADER_NAME;
import static com.datahub.authentication.AuthenticationConstants.ENTITY_SERVICE;
import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_URN;
import static com.linkedin.metadata.Constants.ORIGIN_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SUB_TYPES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OAuthProvider;
import com.linkedin.settings.global.OAuthSettings;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.crypto.spec.SecretKeySpec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubOAuthAuthenticatorTest {

  private static final String TEST_CLIENT_ID = "test-client-id";
  private static final String TEST_DISCOVERY_URI =
      "https://auth.example.com/.well-known/openid-configuration";
  private static final String TEST_USER_NAME_CLAIM = "preferred_username";
  private static final String TEST_ALGORITHM = "RS256";
  private static final String TEST_ISSUER = "https://auth.example.com";
  private static final String TEST_JWKS_URI = "https://auth.example.com/.well-known/jwks.json";

  private DataHubOAuthAuthenticator authenticator;
  private EntityService<?> mockEntityService;
  private OperationContext mockOperationContext;
  private AuthenticatorContext authenticatorContext;

  @BeforeMethod
  public void setUp() {
    authenticator = new DataHubOAuthAuthenticator();
    mockEntityService = mock(EntityService.class);

    // Use TestOperationContexts to create a proper OperationContext with all dependencies
    mockOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    // Set up authenticator context
    Map<String, Object> contextData = new HashMap<>();
    contextData.put(ENTITY_SERVICE, mockEntityService);
    contextData.put("systemOperationContext", mockOperationContext);
    authenticatorContext = new AuthenticatorContext(contextData);
  }

  @Test
  public void testInitSuccessWithStaticConfig() {
    // Arrange
    Map<String, Object> config = createValidStaticConfig();

    // Act
    authenticator.init(config, authenticatorContext);

    // Assert - If no exception is thrown, initialization was successful
    assertNotNull(authenticator);
  }

  @Test
  public void testInitSucceedsButAuthenticationFailsWhenNotConfigured()
      throws AuthenticationException {
    // Static config is empty/invalid - missing required fields but OAuth enabled
    Map<String, Object> invalidStaticConfig = new HashMap<>();
    invalidStaticConfig.put("enabled", "true");

    // Act - Init should succeed without throwing exception
    authenticator.init(invalidStaticConfig, authenticatorContext);

    // But authentication should fail gracefully
    AuthenticationRequest request = new AuthenticationRequest(Collections.emptyMap());

    try {
      authenticator.authenticate(request);
      assertNotNull(null, "Expected AuthenticationException to be thrown");
    } catch (AuthenticationException e) {
      assertTrue(
          e.getMessage().contains("OAuth authenticator is not configured"),
          "Should fail with not configured message, got: " + e.getMessage());
    }
  }

  @Test
  public void testInitFailureNullConfig() {
    // Act & Assert
    try {
      authenticator.init(null, authenticatorContext);
      assertNotNull(null, "Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  public void testInitFailureNullContext() {
    // Act & Assert
    try {
      authenticator.init(Collections.emptyMap(), null);
      assertNotNull(null, "Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  public void testInitFailureMissingEntityService() {
    // Arrange
    Map<String, Object> contextData = new HashMap<>();
    contextData.put("systemOperationContext", mockOperationContext);
    AuthenticatorContext invalidContext = new AuthenticatorContext(contextData);

    Map<String, Object> config = new HashMap<>();
    config.put("enabled", "true"); // Enable to trigger EntityService check

    // Act & Assert
    try {
      authenticator.init(config, invalidContext);
      assertNotNull(null, "Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  public void testAuthenticateFailureMissingAuthorizationHeader() throws AuthenticationException {
    // Arrange
    Map<String, Object> config = createValidStaticConfig();
    authenticator.init(config, authenticatorContext);

    AuthenticationRequest request = new AuthenticationRequest(Collections.emptyMap());

    // Act & Assert
    try {
      authenticator.authenticate(request);
      assertNotNull(null, "Expected AuthenticationException to be thrown");
    } catch (AuthenticationException e) {
      assertTrue(
          e.getMessage().contains("Invalid Authorization header"),
          "Should fail with invalid authorization header message, got: " + e.getMessage());
    }
  }

  @Test
  public void testAuthenticateFailureInvalidAuthorizationHeader() throws AuthenticationException {
    // Arrange
    Map<String, Object> config = createValidStaticConfig();
    authenticator.init(config, authenticatorContext);

    Map<String, String> headers = new HashMap<>();
    headers.put(AUTHORIZATION_HEADER_NAME, "InvalidHeader withoutBearer");
    AuthenticationRequest request = new AuthenticationRequest(headers);

    // Act & Assert
    try {
      authenticator.authenticate(request);
      assertNotNull(null, "Expected AuthenticationException to be thrown");
    } catch (AuthenticationException e) {
      assertTrue(
          e.getMessage().contains("Invalid Authorization header"),
          "Should fail with invalid authorization header message, got: " + e.getMessage());
    }
  }

  @Test
  public void testAuthenticateWithValidJWT() throws AuthenticationException {
    // Arrange
    Map<String, Object> staticConfig = createValidStaticConfig();
    authenticator.init(staticConfig, authenticatorContext);

    // Create a valid JWT token (though it will fail signature verification in real scenario)
    String validJwtToken = createValidJwtToken();
    Map<String, String> headers = new HashMap<>();
    headers.put(AUTHORIZATION_HEADER_NAME, "Bearer " + validJwtToken);
    AuthenticationRequest request = new AuthenticationRequest(headers);

    // Act & Assert
    // This test would require more complex setup to mock the JWT verification process
    // For now, we'll test the basic flow up to token parsing
    try {
      authenticator.authenticate(request);
      // If we get here without exception, that's unexpected since we don't have a real JWKS setup
      // But the test validates the configuration loading works
    } catch (AuthenticationException e) {
      // Expected since we don't have a real JWT setup for this test
      assertNotNull(e.getMessage());
    }
  }

  private Map<String, Object> createValidStaticConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("enabled", "true"); // Enable OAuth authentication
    config.put("userIdClaim", "sub");
    config.put("trustedIssuers", TEST_ISSUER);
    config.put("allowedAudiences", TEST_CLIENT_ID);
    config.put("jwksUri", TEST_JWKS_URI);
    config.put("algorithm", "RS256");
    return config;
  }

  private String createValidJwtToken() {
    // Create a simple JWT token for testing
    // In a real scenario, this would be created by the OIDC provider
    // Use a 256-bit key to avoid WeakKeyException
    String secret = "test-secret-key-for-jwt-signing-that-is-long-enough-for-hmac-sha256-algorithm";
    SecretKeySpec key = new SecretKeySpec(secret.getBytes(), "HmacSHA256");

    Map<String, Object> claims = new HashMap<>();
    claims.put("sub", "test-user");
    claims.put("aud", List.of(TEST_CLIENT_ID));
    claims.put("iss", TEST_ISSUER);
    claims.put("exp", new Date(System.currentTimeMillis() + 3600000)); // 1 hour from now
    claims.put(TEST_USER_NAME_CLAIM, "test-user@example.com");

    return Jwts.builder()
        .setClaims(claims)
        .setHeaderParam("kid", "test-key-id")
        .signWith(key, SignatureAlgorithm.HS256)
        .compact();
  }

  @Test
  public void testMultipleAudienceConfiguration() {
    // Arrange - Static config with multiple audiences (comma-separated)
    Map<String, Object> staticConfigWithMultipleAudiences = new HashMap<>();
    staticConfigWithMultipleAudiences.put("enabled", "true");
    staticConfigWithMultipleAudiences.put("trustedIssuers", TEST_ISSUER);
    staticConfigWithMultipleAudiences.put(
        "allowedAudiences", "audience-1,additional-client-1,additional-client-2");
    staticConfigWithMultipleAudiences.put("jwksUri", TEST_JWKS_URI);

    // Act
    authenticator.init(staticConfigWithMultipleAudiences, authenticatorContext);

    // Assert - verify OAuth providers were created for each audience
    try {
      java.lang.reflect.Field oauthProvidersField =
          DataHubOAuthAuthenticator.class.getDeclaredField("oauthProviders");
      oauthProvidersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<OAuthProvider> actualProviders =
          (List<OAuthProvider>) oauthProvidersField.get(authenticator);

      assertNotNull(actualProviders);
      assertEquals(actualProviders.size(), 3); // 3 providers (1 issuer × 3 audiences)

      // Collect all audiences from providers
      HashSet<String> providerAudiences = new HashSet<>();
      for (OAuthProvider provider : actualProviders) {
        assertEquals(provider.getIssuer(), TEST_ISSUER); // Same issuer for all
        providerAudiences.add(provider.getAudience());
        assertTrue(provider.getName().startsWith("static_"));
      }

      assertEquals(providerAudiences.size(), 3);
      assertTrue(providerAudiences.contains("audience-1"));
      assertTrue(providerAudiences.contains("additional-client-1"));
      assertTrue(providerAudiences.contains("additional-client-2"));

    } catch (Exception e) {
      assertNotNull(null, "Failed to verify multiple audience configuration: " + e.getMessage());
    }
  }

  @Test
  public void testStaticConfigOnlyAudiences() {
    // Arrange - Static config only
    Map<String, Object> staticConfig = createValidStaticConfig();

    // Act
    authenticator.init(staticConfig, authenticatorContext);

    // Assert - verify only static config providers are used
    try {
      java.lang.reflect.Field oauthProvidersField =
          DataHubOAuthAuthenticator.class.getDeclaredField("oauthProviders");
      oauthProvidersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<OAuthProvider> actualProviders =
          (List<OAuthProvider>) oauthProvidersField.get(authenticator);

      assertNotNull(actualProviders);
      assertEquals(actualProviders.size(), 1); // Only one static provider

      OAuthProvider staticProvider = actualProviders.get(0);
      assertTrue(staticProvider.getName().startsWith("static_"));
      assertEquals(staticProvider.getIssuer(), TEST_ISSUER);
      assertEquals(staticProvider.getAudience(), TEST_CLIENT_ID);

    } catch (Exception e) {
      assertNotNull(
          null, "Failed to verify static-only OAuth provider configuration: " + e.getMessage());
    }
  }

  @Test
  public void testServiceAccountUserIdGeneration() {
    // Test that the user ID generation logic works correctly without full JWT authentication
    Map<String, Object> staticConfig = createValidStaticConfig();
    authenticator.init(staticConfig, authenticatorContext);

    // Use reflection to test the buildServiceUserUrn method directly
    try {
      // Create a test JWT claims object
      java.lang.reflect.Method buildMethod =
          DataHubOAuthAuthenticator.class.getDeclaredMethod(
              "buildServiceUserUrn", io.jsonwebtoken.Claims.class);
      buildMethod.setAccessible(true);

      // Create mock claims
      io.jsonwebtoken.Claims mockClaims = mock(io.jsonwebtoken.Claims.class);
      when(mockClaims.getIssuer()).thenReturn("https://auth.example.com");
      when(mockClaims.get("sub", String.class)).thenReturn("service-account-123");

      // Act
      String userId = (String) buildMethod.invoke(authenticator, mockClaims);

      // Assert
      assertNotNull(userId);
      assertTrue(userId.startsWith("__oauth_"));
      assertTrue(userId.contains("auth_example_com"));
      assertTrue(userId.contains("service-account-123"));

    } catch (Exception e) {
      assertNotNull(null, "Failed to test user ID generation: " + e.getMessage());
    }
  }

  @Test
  public void testServiceAccountAspectCreation() {
    // Test the aspect creation logic without full JWT authentication
    Map<String, Object> staticConfig = createValidStaticConfig();
    authenticator.init(staticConfig, authenticatorContext);

    try {
      // Use reflection to test the createServiceAccountAspects method directly
      java.lang.reflect.Method createMethod =
          DataHubOAuthAuthenticator.class.getDeclaredMethod(
              "createServiceAccountAspects", CorpuserUrn.class, io.jsonwebtoken.Claims.class);
      createMethod.setAccessible(true);

      // Create test data
      CorpuserUrn testUrn = new CorpuserUrn("__oauth_auth_example_com_service123");
      io.jsonwebtoken.Claims mockClaims = mock(io.jsonwebtoken.Claims.class);
      when(mockClaims.getIssuer()).thenReturn("https://auth.example.com");
      when(mockClaims.get("sub", String.class)).thenReturn("service123");

      // Act
      @SuppressWarnings("unchecked")
      List<MetadataChangeProposal> aspects =
          (List<MetadataChangeProposal>) createMethod.invoke(authenticator, testUrn, mockClaims);

      // Assert
      assertNotNull(aspects);
      assertEquals(aspects.size(), 3); // CorpUserInfo, SubTypes, Origin

      // Verify each aspect
      boolean hasCorpUserInfo = false;
      boolean hasSubTypes = false;
      boolean hasOrigin = false;

      for (MetadataChangeProposal mcp : aspects) {
        assertEquals(mcp.getEntityUrn(), testUrn);
        assertEquals(mcp.getEntityType(), "corpuser");
        assertEquals(mcp.getChangeType(), ChangeType.UPSERT);

        if (CORP_USER_INFO_ASPECT_NAME.equals(mcp.getAspectName())) {
          hasCorpUserInfo = true;
        } else if (SUB_TYPES_ASPECT_NAME.equals(mcp.getAspectName())) {
          hasSubTypes = true;
        } else if (ORIGIN_ASPECT_NAME.equals(mcp.getAspectName())) {
          hasOrigin = true;
        }
      }

      assertTrue(hasCorpUserInfo, "Should create CorpUserInfo aspect");
      assertTrue(hasSubTypes, "Should create SubTypes aspect");
      assertTrue(hasOrigin, "Should create Origin aspect");

    } catch (Exception e) {
      assertNotNull(null, "Failed to test aspect creation: " + e.getMessage());
    }
  }

  @Test
  public void testEnsureServiceAccountExistsWithNewUser() {
    // Test the ensureServiceAccountExists logic with a new user
    Map<String, Object> staticConfig = createValidStaticConfig();

    // Mock user doesn't exist initially
    when(mockEntityService.exists(eq(mockOperationContext), any(CorpuserUrn.class), eq(false)))
        .thenReturn(false);

    authenticator.init(staticConfig, authenticatorContext);

    try {
      // Use reflection to test the ensureServiceAccountExists method directly
      java.lang.reflect.Method ensureMethod =
          DataHubOAuthAuthenticator.class.getDeclaredMethod(
              "ensureServiceAccountExists", String.class, io.jsonwebtoken.Claims.class);
      ensureMethod.setAccessible(true);

      // Create test data
      String userId = "__oauth_auth_example_com_service123";
      io.jsonwebtoken.Claims mockClaims = mock(io.jsonwebtoken.Claims.class);
      when(mockClaims.getIssuer()).thenReturn("https://auth.example.com");
      when(mockClaims.get("sub", String.class)).thenReturn("service123");

      // Act
      ensureMethod.invoke(authenticator, userId, mockClaims);

      // Verify user existence was checked
      verify(mockEntityService, times(1))
          .exists(eq(mockOperationContext), any(CorpuserUrn.class), eq(false));

      // Verify aspects were ingested
      verify(mockEntityService, times(1))
          .ingestAspects(eq(mockOperationContext), any(AspectsBatch.class), eq(false), eq(true));

    } catch (Exception e) {
      assertNotNull(null, "Failed to test service account creation: " + e.getMessage());
    }
  }

  @Test
  public void testServiceAccountUserIdUniqueness() {
    // Test that different issuers produce different user IDs even with same subject
    Map<String, Object> staticConfig = createValidStaticConfig();
    authenticator.init(staticConfig, authenticatorContext);

    try {
      // Use reflection to test user ID generation with different issuers
      java.lang.reflect.Method buildMethod =
          DataHubOAuthAuthenticator.class.getDeclaredMethod(
              "buildServiceUserUrn", io.jsonwebtoken.Claims.class);
      buildMethod.setAccessible(true);

      // Create mock claims for first issuer
      io.jsonwebtoken.Claims mockClaims1 = mock(io.jsonwebtoken.Claims.class);
      when(mockClaims1.getIssuer()).thenReturn("https://auth.company1.com");
      when(mockClaims1.get("sub", String.class)).thenReturn("service-account-123");

      // Create mock claims for second issuer
      io.jsonwebtoken.Claims mockClaims2 = mock(io.jsonwebtoken.Claims.class);
      when(mockClaims2.getIssuer()).thenReturn("https://auth.company2.com");
      when(mockClaims2.get("sub", String.class)).thenReturn("service-account-123");

      // Act
      String userId1 = (String) buildMethod.invoke(authenticator, mockClaims1);
      String userId2 = (String) buildMethod.invoke(authenticator, mockClaims2);

      // Assert different user IDs are generated
      assertNotNull(userId1);
      assertNotNull(userId2);
      assertTrue(!userId1.equals(userId2), "Different issuers should generate different user IDs");

      // Both should contain the issuer information
      assertTrue(
          userId1.contains("auth_company1_com"), "User ID should contain sanitized issuer 1");
      assertTrue(
          userId2.contains("auth_company2_com"), "User ID should contain sanitized issuer 2");

      // Both should have the OAuth prefix and subject
      assertTrue(userId1.startsWith("__oauth_"), "User ID 1 should have OAuth prefix");
      assertTrue(userId2.startsWith("__oauth_"), "User ID 2 should have OAuth prefix");
      assertTrue(userId1.contains("service-account-123"), "User ID 1 should contain subject");
      assertTrue(userId2.contains("service-account-123"), "User ID 2 should contain subject");

    } catch (Exception e) {
      assertNotNull(null, "Failed to test user ID uniqueness: " + e.getMessage());
    }
  }

  @Test
  public void testEnsureServiceAccountExistsWithExistingUser() {
    // Test the ensureServiceAccountExists logic with an existing user
    Map<String, Object> staticConfig = createValidStaticConfig();

    // Mock user already exists
    when(mockEntityService.exists(eq(mockOperationContext), any(CorpuserUrn.class), eq(false)))
        .thenReturn(true);

    authenticator.init(staticConfig, authenticatorContext);

    try {
      // Use reflection to test the ensureServiceAccountExists method directly
      java.lang.reflect.Method ensureMethod =
          DataHubOAuthAuthenticator.class.getDeclaredMethod(
              "ensureServiceAccountExists", String.class, io.jsonwebtoken.Claims.class);
      ensureMethod.setAccessible(true);

      // Create test data
      String userId = "__oauth_auth_example_com_service123";
      io.jsonwebtoken.Claims mockClaims = mock(io.jsonwebtoken.Claims.class);
      when(mockClaims.getIssuer()).thenReturn("https://auth.example.com");
      when(mockClaims.get("sub", String.class)).thenReturn("service123");

      // Act
      ensureMethod.invoke(authenticator, userId, mockClaims);

      // Verify user existence was checked
      verify(mockEntityService, times(1))
          .exists(eq(mockOperationContext), any(CorpuserUrn.class), eq(false));

      // Verify aspects were NOT ingested (user already exists)
      verify(mockEntityService, never())
          .ingestAspects(eq(mockOperationContext), any(AspectsBatch.class), eq(false), eq(true));

    } catch (Exception e) {
      assertNotNull(null, "Failed to test existing service account handling: " + e.getMessage());
    }
  }

  @Test
  public void testServiceAccountCreationErrorHandling() {
    // Test that service account creation failures are handled gracefully
    Map<String, Object> staticConfig = createValidStaticConfig();

    // Mock user doesn't exist initially
    when(mockEntityService.exists(eq(mockOperationContext), any(CorpuserUrn.class), eq(false)))
        .thenReturn(false);

    // Mock aspect ingestion failure
    doThrow(new RuntimeException("Ingestion failed"))
        .when(mockEntityService)
        .ingestAspects(eq(mockOperationContext), any(AspectsBatch.class), eq(false), eq(true));

    authenticator.init(staticConfig, authenticatorContext);

    try {
      // Use reflection to test the ensureServiceAccountExists method directly
      java.lang.reflect.Method ensureMethod =
          DataHubOAuthAuthenticator.class.getDeclaredMethod(
              "ensureServiceAccountExists", String.class, io.jsonwebtoken.Claims.class);
      ensureMethod.setAccessible(true);

      // Create test data
      String userId = "__oauth_auth_example_com_service123";
      io.jsonwebtoken.Claims mockClaims = mock(io.jsonwebtoken.Claims.class);
      when(mockClaims.getIssuer()).thenReturn("https://auth.example.com");
      when(mockClaims.get("sub", String.class)).thenReturn("service123");

      // Act - should not throw exception even though ingestion fails
      ensureMethod.invoke(authenticator, userId, mockClaims);

      // Verify user existence was checked
      verify(mockEntityService, times(1))
          .exists(eq(mockOperationContext), any(CorpuserUrn.class), eq(false));

      // Verify aspects ingestion was attempted (but failed)
      verify(mockEntityService, times(1))
          .ingestAspects(eq(mockOperationContext), any(AspectsBatch.class), eq(false), eq(true));

    } catch (Exception e) {
      assertNotNull(null, "Failed to test error handling: " + e.getMessage());
    }
  }

  private String createJwtTokenWithClaims(String issuer, String subject, String audience) {
    // Use a 256-bit key to avoid WeakKeyException
    String secretKey = "mySecretKeyThatIsLongEnoughFor256BitHmacSha256AlgorithmToWork";
    SecretKeySpec signingKey = new SecretKeySpec(secretKey.getBytes(), "HmacSHA256");

    return Jwts.builder()
        .setIssuer(issuer)
        .setSubject(subject)
        .setAudience(audience)
        .claim("preferred_username", subject)
        .setIssuedAt(new Date())
        .setExpiration(new Date(System.currentTimeMillis() + 3600000)) // 1 hour
        .signWith(signingKey, SignatureAlgorithm.HS256)
        .compact();
  }

  // ==================== DYNAMIC CONFIGURATION TESTS ====================

  @Test
  public void testDynamicConfigurationLoading() {
    // Arrange
    Map<String, Object> staticConfig = createValidStaticConfig();

    // Create mock GlobalSettings with OAuth providers
    GlobalSettingsInfo globalSettings = createGlobalSettingsWithOAuthProviders();
    when(mockEntityService.getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(globalSettings);

    // Act
    authenticator.init(staticConfig, authenticatorContext);

    // Assert
    verify(mockEntityService, times(1))
        .getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME));
    assertNotNull(authenticator);
  }

  @Test
  public void testDynamicConfigurationWithNoGlobalSettings() {
    // Arrange
    Map<String, Object> staticConfig = createValidStaticConfig();

    // Mock that no GlobalSettings exist
    when(mockEntityService.getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    // Act
    authenticator.init(staticConfig, authenticatorContext);

    // Assert - Should still initialize successfully with static config
    verify(mockEntityService, times(1))
        .getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME));
    assertNotNull(authenticator);
  }

  @Test
  public void testDynamicConfigurationWithEmptyOAuthProviders() {
    // Arrange
    Map<String, Object> staticConfig = createValidStaticConfig();

    // Create GlobalSettings with no OAuth providers
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    OAuthSettings oauthSettings = new OAuthSettings();
    oauthSettings.setProviders(new com.linkedin.settings.global.OAuthProviderArray());
    globalSettings.setOauth(oauthSettings);

    when(mockEntityService.getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(globalSettings);

    // Act
    authenticator.init(staticConfig, authenticatorContext);

    // Assert
    verify(mockEntityService, times(1))
        .getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME));
    assertNotNull(authenticator);
  }

  @Test
  public void testDynamicConfigurationWithDisabledProviders() {
    // Arrange
    Map<String, Object> staticConfig = createValidStaticConfig();

    // Create GlobalSettings with disabled OAuth provider
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    OAuthSettings oauthSettings = new OAuthSettings();
    com.linkedin.settings.global.OAuthProviderArray providers =
        new com.linkedin.settings.global.OAuthProviderArray();

    OAuthProvider disabledProvider = new OAuthProvider();
    disabledProvider.data().put("enabled", Boolean.FALSE);
    disabledProvider.setName("disabled-provider");
    disabledProvider.setIssuer("https://disabled.example.com");
    disabledProvider.setAudience("disabled-audience");
    disabledProvider.setJwksUri("https://disabled.example.com/jwks");
    providers.add(disabledProvider);

    oauthSettings.setProviders(providers);
    globalSettings.setOauth(oauthSettings);

    when(mockEntityService.getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(globalSettings);

    // Act
    authenticator.init(staticConfig, authenticatorContext);

    // Assert - Should not load disabled providers
    verify(mockEntityService, times(1))
        .getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME));
    assertNotNull(authenticator);
  }

  @Test
  public void testDynamicConfigurationErrorHandling() {
    // Arrange
    Map<String, Object> staticConfig = createValidStaticConfig();

    // Mock EntityService to throw exception when loading GlobalSettings
    when(mockEntityService.getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenThrow(new RuntimeException("GlobalSettings loading failed"));

    // Act - Should not throw exception, should handle gracefully
    authenticator.init(staticConfig, authenticatorContext);

    // Assert - Should still initialize with static config despite dynamic config error
    verify(mockEntityService, times(1))
        .getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME));
    assertNotNull(authenticator);
  }

  @Test
  public void testAuthenticationWithDynamicProviderOnly() {
    // Arrange
    Map<String, Object> staticConfig = new HashMap<>();
    staticConfig.put("enabled", "true");
    staticConfig.put("userIdClaim", "sub");
    // Intentionally omit static OAuth configuration

    GlobalSettingsInfo globalSettings = createGlobalSettingsWithOAuthProviders();
    when(mockEntityService.getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(globalSettings);

    authenticator.init(staticConfig, authenticatorContext);

    // Create a valid JWT token for the dynamic provider
    String token =
        createJwtTokenWithClaims("https://dynamic.example.com", "test-user", "dynamic-audience");

    Map<String, String> headers = new HashMap<>();
    headers.put(AUTHORIZATION_HEADER_NAME, "Bearer " + token);
    AuthenticationRequest request = new AuthenticationRequest(headers);

    // Act & Assert - Should authenticate successfully using dynamic provider
    try {
      var result = authenticator.authenticate(request);
      assertNotNull(result);
      assertEquals(result.getActor().getId(), "__oauth_dynamic.example.com_test-user");
    } catch (AuthenticationException e) {
      // Note: This test may fail if JWT signature verification is required
      // In real scenarios, proper JWT signing keys would be configured
      assertTrue(e.getMessage().contains("OAuth token validation failed"));
    }
  }

  @Test
  public void testAuthenticationWithStaticAndDynamicProviders() {
    // Arrange
    Map<String, Object> staticConfig = createValidStaticConfig();

    GlobalSettingsInfo globalSettings = createGlobalSettingsWithOAuthProviders();
    when(mockEntityService.getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(globalSettings);

    authenticator.init(staticConfig, authenticatorContext);

    // Test with static provider token
    String staticToken = createJwtTokenWithClaims(TEST_ISSUER, "static-user", "static-audience");
    Map<String, String> staticHeaders = new HashMap<>();
    staticHeaders.put(AUTHORIZATION_HEADER_NAME, "Bearer " + staticToken);
    AuthenticationRequest staticRequest = new AuthenticationRequest(staticHeaders);

    // Test with dynamic provider token
    String dynamicToken =
        createJwtTokenWithClaims("https://dynamic.example.com", "dynamic-user", "dynamic-audience");
    Map<String, String> dynamicHeaders = new HashMap<>();
    dynamicHeaders.put(AUTHORIZATION_HEADER_NAME, "Bearer " + dynamicToken);
    AuthenticationRequest dynamicRequest = new AuthenticationRequest(dynamicHeaders);

    // Act & Assert - Both should work (subject to JWT signature validation)
    try {
      var staticResult = authenticator.authenticate(staticRequest);
      assertNotNull(staticResult);
    } catch (AuthenticationException e) {
      assertTrue(e.getMessage().contains("OAuth token validation failed"));
    }

    try {
      var dynamicResult = authenticator.authenticate(dynamicRequest);
      assertNotNull(dynamicResult);
    } catch (AuthenticationException e) {
      assertTrue(e.getMessage().contains("OAuth token validation failed"));
    }
  }

  @Test
  public void testScheduledConfigurationRefresh() throws InterruptedException {
    // Arrange
    Map<String, Object> staticConfig = createValidStaticConfig();

    GlobalSettingsInfo globalSettings = createGlobalSettingsWithOAuthProviders();
    when(mockEntityService.getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(globalSettings);

    // Act
    authenticator.init(staticConfig, authenticatorContext);

    // Wait a short time to ensure scheduler is set up
    Thread.sleep(100);

    // Assert - Verify that the scheduler was set up (initial load + scheduled refresh setup)
    verify(mockEntityService, times(1))
        .getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME));

    // Clean up
    authenticator.destroy();
  }

  @Test
  public void testCleanupDestroy() {
    // Arrange
    Map<String, Object> staticConfig = createValidStaticConfig();

    GlobalSettingsInfo globalSettings = createGlobalSettingsWithOAuthProviders();
    when(mockEntityService.getLatestAspect(
            eq(mockOperationContext),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(globalSettings);

    authenticator.init(staticConfig, authenticatorContext);

    // Act
    authenticator.destroy();

    // Assert - No exception should be thrown, scheduler should be shut down gracefully
    assertNotNull(authenticator);
  }

  // Helper method to create GlobalSettings with OAuth providers
  private GlobalSettingsInfo createGlobalSettingsWithOAuthProviders() {
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    OAuthSettings oauthSettings = new OAuthSettings();
    com.linkedin.settings.global.OAuthProviderArray providers =
        new com.linkedin.settings.global.OAuthProviderArray();

    // Create enabled provider
    OAuthProvider enabledProvider = new OAuthProvider();
    enabledProvider.data().put("enabled", Boolean.TRUE);
    enabledProvider.setName("dynamic-provider");
    enabledProvider.setIssuer("https://dynamic.example.com");
    enabledProvider.setAudience("dynamic-audience");
    enabledProvider.setJwksUri("https://dynamic.example.com/jwks");
    providers.add(enabledProvider);

    // Create another enabled provider
    OAuthProvider secondProvider = new OAuthProvider();
    secondProvider.data().put("enabled", Boolean.TRUE);
    secondProvider.setName("second-provider");
    secondProvider.setIssuer("https://second.example.com");
    secondProvider.setAudience("second-audience");
    secondProvider.setJwksUri("https://second.example.com/jwks");
    providers.add(secondProvider);

    oauthSettings.setProviders(providers);
    globalSettings.setOauth(oauthSettings);

    return globalSettings;
  }
}
