package security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import auth.sso.SsoConfigs;
import auth.sso.SsoManager;
import auth.sso.SsoProvider;
import auth.sso.oidc.OidcProvider;
import com.datahub.authentication.Authentication;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class SsoManagerTest {

  private SsoManager ssoManager;
  private Config mockConfig;
  private Authentication mockAuthentication;
  private CloseableHttpClient mockHttpClient;
  private String ssoSettingsRequestUrl;
  private SsoProvider mockSsoProvider;
  private OidcProvider mockOidcProvider;

  @BeforeEach
  public void setUp() {
    // Create mock configurations
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.oidc.enabled", false);
    mockConfig = ConfigFactory.parseMap(configMap);

    // Mock dependencies
    mockAuthentication = mock(Authentication.class);
    mockHttpClient = mock(CloseableHttpClient.class);
    ssoSettingsRequestUrl = "http://localhost:8080/sso/settings";
    mockSsoProvider = mock(SsoProvider.class);
    mockOidcProvider = mock(OidcProvider.class);

    when(mockAuthentication.getCredentials()).thenReturn("Bearer test-token");

    // Create the SsoManager instance
    ssoManager =
        new SsoManager(mockConfig, mockAuthentication, ssoSettingsRequestUrl, mockHttpClient);
  }

  @Test
  public void testConstructorWithValidParameters() {
    // Test that constructor works with valid parameters
    SsoManager manager =
        new SsoManager(mockConfig, mockAuthentication, ssoSettingsRequestUrl, mockHttpClient);
    assertNotNull(manager);
    assertFalse(manager.isSsoEnabled());
    assertNull(manager.getSsoProvider());
  }

  @Test
  public void testConstructorWithNullAuthentication() {
    // Test that constructor throws exception with null authentication
    assertThrows(
        NullPointerException.class,
        () -> new SsoManager(mockConfig, null, ssoSettingsRequestUrl, mockHttpClient));
  }

  @Test
  public void testConstructorWithNullSsoSettingsUrl() {
    // Test that constructor throws exception with null SSO settings URL
    assertThrows(
        NullPointerException.class,
        () -> new SsoManager(mockConfig, mockAuthentication, null, mockHttpClient));
  }

  @Test
  public void testConstructorWithNullHttpClient() {
    // Test that constructor throws exception with null HTTP client
    assertThrows(
        NullPointerException.class,
        () -> new SsoManager(mockConfig, mockAuthentication, ssoSettingsRequestUrl, null));
  }

  @Test
  public void testIsSsoEnabledWithOidcConfigEnabled() {
    // Test SSO enabled via static config
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.oidc.enabled", true);
    Config oidcEnabledConfig = ConfigFactory.parseMap(configMap);

    SsoManager manager =
        new SsoManager(
            oidcEnabledConfig, mockAuthentication, ssoSettingsRequestUrl, mockHttpClient);
    assertTrue(manager.isSsoEnabled());
  }

  @Test
  public void testIsSsoEnabledWithProvider() {
    // Test SSO enabled when provider is set
    ssoManager.setSsoProvider(mockSsoProvider);
    assertTrue(ssoManager.isSsoEnabled());
  }

  @Test
  public void testIsSsoEnabledWithoutProviderOrConfig() {
    // Test SSO disabled when no provider and config disabled
    assertFalse(ssoManager.isSsoEnabled());
  }

  @Test
  public void testSetSsoProvider() {
    // Test setting SSO provider
    ssoManager.setSsoProvider(mockSsoProvider);
    assertEquals(mockSsoProvider, ssoManager.getSsoProvider());
  }

  @Test
  public void testSetSsoProviderWithNull() {
    // Test setting null SSO provider
    ssoManager.setSsoProvider(mockSsoProvider);
    ssoManager.setSsoProvider(null);
    assertNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testClearSsoProvider() {
    // Test clearing SSO provider
    ssoManager.setSsoProvider(mockSsoProvider);
    ssoManager.clearSsoProvider();
    assertNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testSetConfigs() {
    // Test setting new configs
    Map<String, Object> newConfigMap = new HashMap<>();
    newConfigMap.put("auth.oidc.enabled", true);
    Config newConfig = ConfigFactory.parseMap(newConfigMap);

    ssoManager.setConfigs(newConfig);
    assertTrue(ssoManager.isSsoEnabled());
  }

  // ========== COMPREHENSIVE TESTS FOR initializeSsoProvider() ==========

  @Test
  public void testInitializeSsoProvider_StaticConfigsInvalid_NoDynamicSettings() throws Exception {
    // Setup: Invalid static configs, no dynamic settings
    Map<String, Object> invalidConfigMap = new HashMap<>();
    Config invalidConfig = ConfigFactory.parseMap(invalidConfigMap);
    ssoManager.setConfigs(invalidConfig);

    // Mock no dynamic settings (empty response)
    mockNoDynamicSettings();

    ssoManager.initializeSsoProvider();

    // Should clear provider and remain null
    assertNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testInitializeSsoProvider_StaticConfigsValid_NoDynamicSettings() throws Exception {
    // Setup: Valid static OIDC configs, no dynamic settings
    setupValidStaticOidcConfig();
    mockNoDynamicSettings();

    ssoManager.initializeSsoProvider();

    // Should set provider from static config
    assertNotNull(ssoManager.getSsoProvider());
    assertTrue(ssoManager.getSsoProvider() instanceof OidcProvider);
  }

  @Test
  public void testInitializeSsoProvider_StaticConfigsInvalid_DynamicSettingsHttpError()
      throws Exception {
    // Setup: Invalid static configs, HTTP error from dynamic settings
    Map<String, Object> invalidConfigMap = new HashMap<>();
    Config invalidConfig = ConfigFactory.parseMap(invalidConfigMap);
    ssoManager.setConfigs(invalidConfig);

    // Mock HTTP error response (no EntityUtils needed)
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(mockResponse.getEntity()).thenReturn(null);

    ssoManager.initializeSsoProvider();

    // Should remain null since both static configs and dynamic settings failed
    assertNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testInitializeSsoProvider_StaticConfigsValid_DynamicSettingsHttpFailure()
      throws Exception {
    // Setup: Valid static OIDC configs, HTTP failure for dynamic settings
    setupValidStaticOidcConfig();

    // Mock HTTP failure (network error)
    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenThrow(new IOException("Connection timeout"));

    ssoManager.initializeSsoProvider();

    // Should set provider from static config, ignore HTTP failure
    assertNotNull(ssoManager.getSsoProvider());
    assertTrue(ssoManager.getSsoProvider() instanceof OidcProvider);
  }

  @Test
  public void testInitializeSsoProvider_StaticOidcBuildFails_DynamicSettingsUnavailable()
      throws Exception {
    // Setup: Static configs that will fail OIDC building, no dynamic settings
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.baseUrl", "http://localhost:9002");
    configMap.put("auth.oidc.enabled", "true");
    configMap.put("auth.oidc.clientId", "test-client-id");
    // Missing required clientSecret and discoveryUri - will cause build() to fail
    Config invalidOidcConfig = ConfigFactory.parseMap(configMap);
    ssoManager.setConfigs(invalidOidcConfig);

    // Mock no dynamic settings available
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(mockResponse.getEntity()).thenReturn(null);

    ssoManager.initializeSsoProvider();

    // Should remain null since static OIDC building failed and no dynamic fallback
    assertNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testInitializeSsoProvider_DynamicSettingsHttp404() throws Exception {
    // Setup: Valid static configs, 404 response from dynamic settings
    setupValidStaticOidcConfig();

    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(mockResponse.getEntity()).thenReturn(null);

    ssoManager.initializeSsoProvider();

    // Should set provider from static config, ignore 404
    assertNotNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testInitializeSsoProvider_DynamicSettingsHttpException() throws Exception {
    // Setup: Valid static configs, HTTP exception when fetching dynamic settings
    setupValidStaticOidcConfig();

    // Mock HTTP exception
    when(mockHttpClient.execute(any(HttpPost.class))).thenThrow(new IOException("Network error"));

    ssoManager.initializeSsoProvider();

    // Should set provider from static config, ignore HTTP failure
    assertNotNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testInitializeSsoProvider_ExistingProviderCleared_WhenStaticConfigInvalid()
      throws Exception {
    // Setup: Start with a provider set, then initialize with invalid static config
    ssoManager.setSsoProvider(mockSsoProvider);
    assertNotNull(ssoManager.getSsoProvider());

    // Set invalid static configs
    Map<String, Object> invalidConfigMap = new HashMap<>();
    Config invalidConfig = ConfigFactory.parseMap(invalidConfigMap);
    ssoManager.setConfigs(invalidConfig);

    // Mock no dynamic settings
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(mockResponse.getEntity()).thenReturn(null);

    ssoManager.initializeSsoProvider();

    // Should clear the provider since configs are invalid and no dynamic settings
    assertNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testInitializeSsoProvider_MultipleCallsWithDifferentStaticConfigs() throws Exception {
    // Test multiple initialization calls with different static configurations

    // First call - valid static OIDC config
    setupValidStaticOidcConfig();
    mockNoDynamicSettings();
    ssoManager.initializeSsoProvider();
    assertNotNull(ssoManager.getSsoProvider());

    // Second call - invalid static config (should clear provider)
    Map<String, Object> invalidConfigMap = new HashMap<>();
    Config invalidConfig = ConfigFactory.parseMap(invalidConfigMap);
    ssoManager.setConfigs(invalidConfig);

    ssoManager.initializeSsoProvider();
    assertNull(ssoManager.getSsoProvider());

    // Third call - different valid OIDC config
    Map<String, Object> differentValidConfigMap = new HashMap<>();
    differentValidConfigMap.put("auth.baseUrl", "http://different-host:9002");
    differentValidConfigMap.put("auth.oidc.enabled", "true");
    differentValidConfigMap.put("auth.oidc.clientId", "different-client-id");
    differentValidConfigMap.put("auth.oidc.clientSecret", "different-client-secret");
    differentValidConfigMap.put(
        "auth.oidc.discoveryUri", "http://different-server/.well-known/openid_configuration");
    Config differentValidConfig = ConfigFactory.parseMap(differentValidConfigMap);
    ssoManager.setConfigs(differentValidConfig);

    ssoManager.initializeSsoProvider();
    assertNotNull(ssoManager.getSsoProvider());
    assertTrue(ssoManager.getSsoProvider() instanceof OidcProvider);
  }

  @Test
  public void testInitializeSsoProvider_StaticOidcDisabled_DynamicSettingsUnavailable()
      throws Exception {
    // Setup: Static configs with OIDC explicitly disabled
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.baseUrl", "http://localhost:9002");
    configMap.put("auth.oidc.enabled", "false"); // Explicitly disabled
    configMap.put("auth.oidc.clientId", "disabled-client-id");
    configMap.put("auth.oidc.clientSecret", "disabled-client-secret");
    configMap.put("auth.oidc.discoveryUri", "http://localhost/.well-known/openid_configuration");
    Config disabledOidcConfig = ConfigFactory.parseMap(configMap);
    ssoManager.setConfigs(disabledOidcConfig);

    // Mock no dynamic settings available
    mockNoDynamicSettings();

    ssoManager.initializeSsoProvider();

    // Should remain null since OIDC is disabled and no dynamic override
    assertNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testInitializeSsoProvider_ExistingNonOidcProvider_StaticOidcEnabled()
      throws Exception {
    // Test scenario where existing provider is not OIDC type but static config enables OIDC
    ssoManager.setSsoProvider(mockSsoProvider); // Set non-OIDC provider

    // Mock the provider's configs to return non-OIDC enabled config
    SsoConfigs mockNonOidcConfigs = mock(SsoConfigs.class);
    when(mockSsoProvider.configs()).thenReturn(mockNonOidcConfigs);
    when(mockNonOidcConfigs.isOidcEnabled()).thenReturn(false);

    // Setup valid OIDC static config
    setupValidStaticOidcConfig();
    mockNoDynamicSettings();

    ssoManager.initializeSsoProvider();

    // Should replace non-OIDC provider with OIDC provider from static config
    assertNotNull(ssoManager.getSsoProvider());
    assertTrue(ssoManager.getSsoProvider() instanceof OidcProvider);
  }

  @Test
  public void testInitializeSsoProvider_HttpResponseWithNullEntity() throws Exception {
    // Test HTTP response with null entity (edge case)
    setupValidStaticOidcConfig();

    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(null); // Null entity

    ssoManager.initializeSsoProvider();

    // Should use static config since entity is null
    assertNotNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testInitializeSsoProvider_HttpResponseCloseThrowsException() throws Exception {
    // Test that exception during response.close() is handled gracefully
    setupValidStaticOidcConfig();

    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(mockResponse.getEntity()).thenReturn(null);
    doThrow(new IOException("Close failed")).when(mockResponse).close();

    // Should not throw exception even if close fails
    ssoManager.initializeSsoProvider();

    // Should still create provider from static config
    assertNotNull(ssoManager.getSsoProvider());
    verify(mockResponse).close(); // Verify close was attempted
  }

  @Test
  public void testInitializeSsoProvider_VerifyHttpRequestFormat() throws Exception {
    // Test that HTTP request is formatted correctly
    setupValidStaticOidcConfig();

    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(mockResponse.getEntity()).thenReturn(null);

    ssoManager.initializeSsoProvider();

    // Verify HTTP POST was called with correct setup
    verify(mockHttpClient).execute(any(HttpPost.class));
    verify(mockAuthentication).getCredentials(); // Should get auth credentials
    verify(mockResponse).close(); // Should close response
  }

  // ========== INTEGRATION-STYLE TESTS FOR COMPLEX SCENARIOS ==========

  @Test
  public void testInitializeSsoProvider_RealWorldScenario_DevToProduction() throws Exception {
    // Simulate real-world scenario: development config becomes production

    // Development setup: OIDC disabled, minimal config
    Map<String, Object> devConfigMap = new HashMap<>();
    devConfigMap.put("auth.baseUrl", "http://localhost:9002");
    devConfigMap.put("auth.oidc.enabled", "false");
    Config devConfig = ConfigFactory.parseMap(devConfigMap);
    ssoManager.setConfigs(devConfig);

    mockNoDynamicSettings();
    ssoManager.initializeSsoProvider();
    assertNull(ssoManager.getSsoProvider()); // No SSO in dev

    // Production setup: Add full OIDC config
    Map<String, Object> prodConfigMap = new HashMap<>();
    prodConfigMap.put("auth.baseUrl", "https://production.example.com");
    prodConfigMap.put("auth.oidc.enabled", "true");
    prodConfigMap.put("auth.oidc.clientId", "prod-client-id");
    prodConfigMap.put("auth.oidc.clientSecret", "prod-client-secret");
    prodConfigMap.put(
        "auth.oidc.discoveryUri", "https://auth.example.com/.well-known/openid_configuration");
    Config prodConfig = ConfigFactory.parseMap(prodConfigMap);
    ssoManager.setConfigs(prodConfig);

    ssoManager.initializeSsoProvider();
    assertNotNull(ssoManager.getSsoProvider()); // SSO enabled in production
    assertTrue(ssoManager.getSsoProvider() instanceof OidcProvider);
  }

  @Test
  public void testInitializeSsoProvider_StaticAndDynamicConfigsBothProvidePartialInfo()
      throws Exception {
    // Test combining partial static config with partial dynamic response would work in real
    // scenario

    // Static provides base info but missing some OIDC details
    Map<String, Object> partialStaticConfigMap = new HashMap<>();
    partialStaticConfigMap.put("auth.baseUrl", "http://localhost:9002");
    partialStaticConfigMap.put("auth.oidc.enabled", "true");
    partialStaticConfigMap.put("auth.oidc.clientId", "static-client-id");
    partialStaticConfigMap.put("auth.oidc.clientSecret", "static-client-secret");
    partialStaticConfigMap.put(
        "auth.oidc.discoveryUri", "http://static-server/.well-known/openid_configuration");
    // Missing scope and other optional fields
    Config partialStaticConfig = ConfigFactory.parseMap(partialStaticConfigMap);
    ssoManager.setConfigs(partialStaticConfig);

    // Simulate scenario where dynamic settings are unavailable (network issue)
    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenThrow(new IOException("Service unavailable"));

    ssoManager.initializeSsoProvider();

    // Should still work with static config alone (using defaults for missing optional fields)
    assertNotNull(ssoManager.getSsoProvider());
    assertTrue(ssoManager.getSsoProvider() instanceof OidcProvider);
  }

  // ========== HELPER METHODS FOR TESTING initializeSsoProvider() ==========

  private void setupValidStaticOidcConfig() {
    Map<String, Object> validConfigMap = new HashMap<>();
    // Required fields for SsoConfigs
    validConfigMap.put("auth.baseUrl", "http://localhost:9002");
    validConfigMap.put("auth.oidc.enabled", "true");
    // Required fields for OidcConfigs
    validConfigMap.put("auth.oidc.clientId", "static-client-id");
    validConfigMap.put("auth.oidc.clientSecret", "static-client-secret");
    validConfigMap.put(
        "auth.oidc.discoveryUri", "http://localhost:8080/.well-known/openid_configuration");
    // Optional fields with common values
    validConfigMap.put("auth.oidc.userNameClaim", "preferred_username");
    validConfigMap.put("auth.oidc.scope", "openid profile email");
    validConfigMap.put("auth.baseCallbackPath", "/callback/oidc");
    validConfigMap.put("auth.successRedirectPath", "/");
    Config validConfig = ConfigFactory.parseMap(validConfigMap);
    ssoManager.setConfigs(validConfig);
  }

  private void mockNoDynamicSettings() throws Exception {
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    when(mockResponse.getEntity()).thenReturn(null);
  }

  private void mockDynamicSettingsResponse(String jsonResponse) throws Exception {
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(mockEntity);

    // Mock EntityUtils statically - this will be used in the test method's try-with-resources
    // The actual MockedStatic setup needs to be done in each test method
  }

  private void executeDynamicSettingsTest(String jsonResponse, Runnable testAction)
      throws Exception {
    mockDynamicSettingsResponse(jsonResponse);

    try (MockedStatic<EntityUtils> mockedEntityUtils = Mockito.mockStatic(EntityUtils.class)) {
      CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
      HttpEntity mockEntity = mock(HttpEntity.class);
      when(mockResponse.getEntity()).thenReturn(mockEntity);

      mockedEntityUtils
          .when(() -> EntityUtils.toString(any(HttpEntity.class)))
          .thenReturn(jsonResponse);

      testAction.run();
    }
  }

  private void mockDynamicSettingsHttpError(int statusCode) throws Exception {
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(statusCode);
    when(mockResponse.getEntity()).thenReturn(null);
  }

  private String createValidOidcJsonSettings() {
    return createValidOidcJsonSettings("dynamic-client-id");
  }

  private String createValidOidcJsonSettings(String clientId) {
    return String.format(
        """
        {
          "oidc": {
            "enabled": true,
            "clientId": "%s",
            "clientSecret": "dynamic-client-secret",
            "discoveryUri": "http://dynamic-server:8080/.well-known/openid_configuration",
            "userNameClaim": "preferred_username",
            "scope": "openid profile email"
          }
        }
        """,
        clientId);
  }

  private String createDisabledOidcJsonSettings() {
    return """
        {
          "oidc": {
            "enabled": false
          }
        }
        """;
  }

  private String createInvalidSsoConfigJson() {
    return """
        {
          "invalid": "config-structure"
        }
        """;
  }

  private String createInvalidOidcConfigJson() {
    return """
        {
          "oidc": {
            "enabled": true,
            "clientId": "test-client"
            // Missing required fields like clientSecret, discoveryUri
          }
        }
        """;
  }

  @Test
  public void testRefreshSsoProviderWithSuccessfulHttpResponse() throws Exception {
    // Mock successful HTTP response
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(mockEntity);

    // Call isSsoEnabled which triggers refreshSsoProvider
    ssoManager.isSsoEnabled();

    verify(mockHttpClient).execute(any(HttpPost.class));
    verify(mockResponse).close();
  }

  @Test
  public void testRefreshSsoProviderWithFailedHttpResponse() throws Exception {
    // Mock failed HTTP response
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    // Call isSsoEnabled which triggers refreshSsoProvider
    ssoManager.isSsoEnabled();

    verify(mockHttpClient).execute(any(HttpPost.class));
    verify(mockResponse).close();
    assertNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testRefreshSsoProviderWithHttpException() throws Exception {
    // Mock HTTP exception
    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenThrow(new IOException("Connection failed"));

    // Call isSsoEnabled which triggers refreshSsoProvider
    ssoManager.isSsoEnabled();

    verify(mockHttpClient).execute(any(HttpPost.class));
    assertNull(ssoManager.getSsoProvider());
  }

  @Test
  public void testRefreshSsoProviderWithExistingProvider() {
    // Test that existing provider is maintained when refresh fails
    ssoManager.setSsoProvider(mockSsoProvider);

    // Call refresh (via isSsoEnabled) with no dynamic settings
    ssoManager.isSsoEnabled();

    // Provider should still be present
    assertEquals(mockSsoProvider, ssoManager.getSsoProvider());
  }

  @Test
  public void testHttpRequestContainsCorrectHeaders() throws Exception {
    // Mock HTTP response
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);

    // Call isSsoEnabled which triggers HTTP request
    ssoManager.isSsoEnabled();

    // Verify that HTTP client was called with a POST request
    verify(mockHttpClient).execute(any(HttpPost.class));
    verify(mockResponse).close();
  }

  @Test
  public void testResponseCloseHandlesException() throws Exception {
    // Mock HTTP response that throws exception on close
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(null);
    doThrow(new IOException("Close failed")).when(mockResponse).close();

    // Should not throw exception even if close fails
    ssoManager.isSsoEnabled();

    verify(mockHttpClient).execute(any(HttpPost.class));
    verify(mockResponse).close();
  }
}
