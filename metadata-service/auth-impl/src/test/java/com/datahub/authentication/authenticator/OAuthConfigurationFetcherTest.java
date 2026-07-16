package com.datahub.authentication.authenticator;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OAuthProvider;
import com.linkedin.settings.global.OAuthProviderArray;
import com.linkedin.settings.global.OAuthSettings;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OAuthConfigurationFetcherTest {

  private static final String TEST_ISSUER = "https://auth.example.com";
  private static final String TEST_AUDIENCE = "test-client-id";
  private static final String TEST_JWKS_URI = "https://auth.example.com/.well-known/jwks.json";

  @Mock private EntityService<?> mockEntityService;

  private OAuthConfigurationFetcher fetcher;
  private OperationContext operationContext;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    fetcher = new OAuthConfigurationFetcher();
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @AfterMethod
  public void tearDown() {
    if (fetcher != null) {
      fetcher.destroy();
    }
  }

  @Test
  public void testInitializeWithValidStaticConfig() {
    // Arrange
    Map<String, Object> config = createValidStaticConfig();
    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    // Act
    fetcher.initialize(config, mockEntityService, operationContext);

    // Assert
    assertTrue(fetcher.isConfigured());
    List<OAuthProvider> providers = fetcher.getCachedConfiguration();
    assertEquals(providers.size(), 1);

    OAuthProvider provider = providers.get(0);
    assertEquals(provider.getIssuer(), TEST_ISSUER);
    assertEquals(provider.getAudience(), TEST_AUDIENCE);
    assertEquals(provider.getJwksUri(), TEST_JWKS_URI);
    assertTrue(provider.getName().startsWith("static_"));
    assertTrue(Boolean.TRUE.equals(provider.data().get("enabled")));
  }

  @Test
  public void testInitializeWithMultipleStaticProviders() {
    // Arrange
    Map<String, Object> config = new HashMap<>();
    config.put("trustedIssuers", "issuer1,issuer2");
    config.put("allowedAudiences", "aud1,aud2");
    config.put("jwksUri", TEST_JWKS_URI);

    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    // Act
    fetcher.initialize(config, mockEntityService, operationContext);

    // Assert
    assertTrue(fetcher.isConfigured());
    List<OAuthProvider> providers = fetcher.getCachedConfiguration();
    assertEquals(providers.size(), 4); // 2 issuers × 2 audiences

    // Verify all combinations exist
    boolean found11 = false, found12 = false, found21 = false, found22 = false;
    for (OAuthProvider provider : providers) {
      if ("issuer1".equals(provider.getIssuer()) && "aud1".equals(provider.getAudience())) {
        found11 = true;
      } else if ("issuer1".equals(provider.getIssuer()) && "aud2".equals(provider.getAudience())) {
        found12 = true;
      } else if ("issuer2".equals(provider.getIssuer()) && "aud1".equals(provider.getAudience())) {
        found21 = true;
      } else if ("issuer2".equals(provider.getIssuer()) && "aud2".equals(provider.getAudience())) {
        found22 = true;
      }
    }
    assertTrue(found11 && found12 && found21 && found22);
  }

  @Test
  public void testInitializeWithInvalidStaticConfig() {
    // Arrange - Missing required fields
    Map<String, Object> config = new HashMap<>();
    config.put("trustedIssuers", TEST_ISSUER);
    // Missing allowedAudiences and jwksUri

    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    // Act
    fetcher.initialize(config, mockEntityService, operationContext);

    // Assert
    assertFalse(fetcher.isConfigured());
    List<OAuthProvider> providers = fetcher.getCachedConfiguration();
    assertEquals(providers.size(), 0);
  }

  @Test
  public void testInitializeWithDynamicConfigOnly() {
    // Arrange
    Map<String, Object> config = new HashMap<>(); // No static config
    GlobalSettingsInfo globalSettings = createGlobalSettingsWithOAuthProviders();

    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(globalSettings);

    // Act
    fetcher.initialize(config, mockEntityService, operationContext);

    // Assert
    assertTrue(fetcher.isConfigured());
    List<OAuthProvider> providers = fetcher.getCachedConfiguration();
    assertEquals(providers.size(), 2); // Two dynamic providers

    // Verify dynamic providers are present
    boolean foundDynamic1 = false, foundDynamic2 = false;
    for (OAuthProvider provider : providers) {
      if ("dynamic-provider".equals(provider.getName())) {
        foundDynamic1 = true;
        assertEquals(provider.getIssuer(), "https://dynamic.example.com");
      } else if ("second-provider".equals(provider.getName())) {
        foundDynamic2 = true;
        assertEquals(provider.getIssuer(), "https://second.example.com");
      }
    }
    assertTrue(foundDynamic1 && foundDynamic2);
  }

  @Test
  public void testInitializeWithStaticAndDynamicConfig() {
    // Arrange
    Map<String, Object> config = createValidStaticConfig();
    GlobalSettingsInfo globalSettings = createGlobalSettingsWithOAuthProviders();

    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(globalSettings);

    // Act
    fetcher.initialize(config, mockEntityService, operationContext);

    // Assert
    assertTrue(fetcher.isConfigured());
    List<OAuthProvider> providers = fetcher.getCachedConfiguration();
    assertEquals(providers.size(), 3); // 1 static + 2 dynamic

    // Verify both static and dynamic providers are present
    boolean foundStatic = false, foundDynamic1 = false, foundDynamic2 = false;
    for (OAuthProvider provider : providers) {
      if (provider.getName().startsWith("static_")) {
        foundStatic = true;
      } else if ("dynamic-provider".equals(provider.getName())) {
        foundDynamic1 = true;
      } else if ("second-provider".equals(provider.getName())) {
        foundDynamic2 = true;
      }
    }
    assertTrue(foundStatic && foundDynamic1 && foundDynamic2);
  }

  @Test
  public void testInitializeWithDisabledDynamicProviders() {
    // Arrange
    Map<String, Object> config = new HashMap<>();
    GlobalSettingsInfo globalSettings = createGlobalSettingsWithDisabledProviders();

    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(globalSettings);

    // Act
    fetcher.initialize(config, mockEntityService, operationContext);

    // Assert
    assertFalse(fetcher.isConfigured()); // Should be false since all providers are disabled
    List<OAuthProvider> providers = fetcher.getCachedConfiguration();
    assertEquals(providers.size(), 0);
  }

  @Test
  public void testFindMatchingProvider() {
    // Arrange
    Map<String, Object> config = createValidStaticConfig();
    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    fetcher.initialize(config, mockEntityService, operationContext);

    // Act & Assert - Matching provider
    OAuthProvider matchingProvider =
        fetcher.findMatchingProvider(TEST_ISSUER, Arrays.asList(TEST_AUDIENCE));
    assertNotNull(matchingProvider);
    assertEquals(matchingProvider.getIssuer(), TEST_ISSUER);
    assertEquals(matchingProvider.getAudience(), TEST_AUDIENCE);

    // Act & Assert - Non-matching issuer
    OAuthProvider nonMatchingIssuer =
        fetcher.findMatchingProvider("https://wrong.issuer.com", Arrays.asList(TEST_AUDIENCE));
    assertNull(nonMatchingIssuer);

    // Act & Assert - Non-matching audience
    OAuthProvider nonMatchingAudience =
        fetcher.findMatchingProvider(TEST_ISSUER, Arrays.asList("wrong-audience"));
    assertNull(nonMatchingAudience);

    // Act & Assert - Multiple audiences with one match
    OAuthProvider multipleAudiences =
        fetcher.findMatchingProvider(TEST_ISSUER, Arrays.asList("wrong-audience", TEST_AUDIENCE));
    assertNotNull(multipleAudiences);
    assertEquals(multipleAudiences.getAudience(), TEST_AUDIENCE);
  }

  @Test
  public void testForceRefreshConfiguration() {
    // Arrange
    Map<String, Object> config = createValidStaticConfig();
    GlobalSettingsInfo initialSettings = null;
    GlobalSettingsInfo updatedSettings = createGlobalSettingsWithOAuthProviders();

    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(initialSettings)
        .thenReturn(updatedSettings);

    fetcher.initialize(config, mockEntityService, operationContext);
    assertEquals(fetcher.getCachedConfiguration().size(), 1); // Only static

    // Act
    List<OAuthProvider> refreshedProviders = fetcher.forceRefreshConfiguration();

    // Assert
    assertEquals(refreshedProviders.size(), 3); // Static + 2 dynamic
    verify(mockEntityService, times(2))
        .getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME));
  }

  @Test
  public void testDynamicConfigurationErrorHandling() {
    // Arrange
    Map<String, Object> config = createValidStaticConfig();

    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenThrow(new RuntimeException("GlobalSettings fetch failed"));

    // Act
    fetcher.initialize(config, mockEntityService, operationContext);

    // Assert - Should still be configured with static config despite dynamic error
    assertTrue(fetcher.isConfigured());
    List<OAuthProvider> providers = fetcher.getCachedConfiguration();
    assertEquals(providers.size(), 1); // Only static provider
  }

  @Test
  public void testIsConfiguredReturnsFalseWhenNoProviders() {
    // Arrange
    Map<String, Object> config = new HashMap<>(); // Empty config

    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    // Act
    fetcher.initialize(config, mockEntityService, operationContext);

    // Assert
    assertFalse(fetcher.isConfigured());
  }

  @Test
  public void testGetCachedConfigurationReturnsImmutableCopy() {
    // Arrange
    Map<String, Object> config = createValidStaticConfig();
    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    fetcher.initialize(config, mockEntityService, operationContext);

    // Act
    List<OAuthProvider> providers1 = fetcher.getCachedConfiguration();
    List<OAuthProvider> providers2 = fetcher.getCachedConfiguration();

    // Assert - Should be different instances (defensive copies)
    assertNotSame(providers1, providers2);
    assertEquals(providers1.size(), providers2.size());
  }

  @Test
  public void testDestroy() {
    // Arrange
    Map<String, Object> config = createValidStaticConfig();
    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    fetcher.initialize(config, mockEntityService, operationContext);

    // Act
    fetcher.destroy();

    // Assert - Should not throw exception, scheduler should be cleaned up
    // Multiple destroy calls should be safe
    fetcher.destroy();
  }

  @Test
  public void testInitializeWithDiscoveryUri() {
    // Arrange
    Map<String, Object> config = new HashMap<>();
    config.put("trustedIssuers", TEST_ISSUER);
    config.put("allowedAudiences", TEST_AUDIENCE);
    config.put("discoveryUri", "https://auth.example.com/.well-known/openid-configuration");
    // No jwksUri - should be derived from discoveryUri

    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    // Act
    fetcher.initialize(config, mockEntityService, operationContext);

    // Assert
    assertTrue(fetcher.isConfigured());
    List<OAuthProvider> providers = fetcher.getCachedConfiguration();
    assertEquals(providers.size(), 1);

    OAuthProvider provider = providers.get(0);
    assertNotNull(provider.getJwksUri());
    // JWKS URI should be derived by JwksUriResolver (actual value depends on network call or
    // fallback)
  }

  @Test
  public void testStaticProviderNaming() {
    // Arrange
    Map<String, Object> config = new HashMap<>();
    config.put("trustedIssuers", "https://auth-server.example.com:443/oauth2");
    config.put("allowedAudiences", TEST_AUDIENCE);
    config.put("jwksUri", TEST_JWKS_URI);

    when(mockEntityService.getLatestAspect(
            eq(operationContext), eq(GLOBAL_SETTINGS_URN), eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(null);

    // Act
    fetcher.initialize(config, mockEntityService, operationContext);

    // Assert
    List<OAuthProvider> providers = fetcher.getCachedConfiguration();
    assertEquals(providers.size(), 1);

    OAuthProvider provider = providers.get(0);
    assertTrue(provider.getName().startsWith("static_"));
    // Name should have special characters replaced with underscores
    assertTrue(provider.getName().contains("auth_server_example_com"));
  }

  // Helper methods
  private Map<String, Object> createValidStaticConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("trustedIssuers", TEST_ISSUER);
    config.put("allowedAudiences", TEST_AUDIENCE);
    config.put("jwksUri", TEST_JWKS_URI);
    return config;
  }

  private GlobalSettingsInfo createGlobalSettingsWithOAuthProviders() {
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    OAuthSettings oauthSettings = new OAuthSettings();
    OAuthProviderArray providers = new OAuthProviderArray();

    // Create first enabled provider
    OAuthProvider provider1 = new OAuthProvider();
    provider1.data().put("enabled", Boolean.TRUE);
    provider1.setName("dynamic-provider");
    provider1.setIssuer("https://dynamic.example.com");
    provider1.setAudience("dynamic-audience");
    provider1.setJwksUri("https://dynamic.example.com/jwks");
    providers.add(provider1);

    // Create second enabled provider
    OAuthProvider provider2 = new OAuthProvider();
    provider2.data().put("enabled", Boolean.TRUE);
    provider2.setName("second-provider");
    provider2.setIssuer("https://second.example.com");
    provider2.setAudience("second-audience");
    provider2.setJwksUri("https://second.example.com/jwks");
    providers.add(provider2);

    oauthSettings.setProviders(providers);
    globalSettings.setOauth(oauthSettings);

    return globalSettings;
  }

  private GlobalSettingsInfo createGlobalSettingsWithDisabledProviders() {
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    OAuthSettings oauthSettings = new OAuthSettings();
    OAuthProviderArray providers = new OAuthProviderArray();

    // Create disabled provider
    OAuthProvider disabledProvider = new OAuthProvider();
    disabledProvider.data().put("enabled", Boolean.FALSE);
    disabledProvider.setName("disabled-provider");
    disabledProvider.setIssuer("https://disabled.example.com");
    disabledProvider.setAudience("disabled-audience");
    disabledProvider.setJwksUri("https://disabled.example.com/jwks");
    providers.add(disabledProvider);

    oauthSettings.setProviders(providers);
    globalSettings.setOauth(oauthSettings);

    return globalSettings;
  }
}
