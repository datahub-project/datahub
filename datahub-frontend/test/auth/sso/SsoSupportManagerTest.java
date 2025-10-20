package auth.sso;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import auth.sso.oidc.support.OidcSupportConfigs;
import auth.sso.oidc.support.OidcSupportProvider;
import com.datahub.authentication.Authentication;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SsoSupportManagerTest {

  @Mock private Authentication mockAuthentication;

  @Mock private CloseableHttpClient mockHttpClient;

  @Mock private CloseableHttpResponse mockHttpResponse;

  @Mock private HttpEntity mockHttpEntity;

  @Mock private StatusLine mockStatusLine;

  private SsoSupportManager supportManager;
  private Config mockConfig;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Setup HTTP response mocks
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
    try {
      when(mockHttpEntity.getContent()).thenReturn(null);
    } catch (IOException e) {
      // This won't happen in the mock
    }
    try {
      when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);
    } catch (IOException e) {
      // This won't happen in the mock
    }

    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    mockConfig = ConfigFactory.parseMap(configMap);

    supportManager =
        new SsoSupportManager(
            mockConfig,
            mockAuthentication,
            "https://test.example.com/sso-settings",
            mockHttpClient);
  }

  @Test
  public void testConstructor() {
    assertNotNull(supportManager);
    assertNotNull(supportManager.configs);
    assertNotNull(supportManager.authentication);
    assertNotNull(supportManager.ssoSettingsRequestUrl);
    assertNotNull(supportManager.httpClient);
  }

  @Test
  public void testIsSupportSsoEnabledWhenEnabled() {
    assertTrue(supportManager.isSupportSsoEnabled());
  }

  @Test
  public void testIsSupportSsoEnabledWhenDisabled() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "false");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config disabledConfig = ConfigFactory.parseMap(configMap);
    supportManager.setConfigs(disabledConfig);

    assertFalse(supportManager.isSupportSsoEnabled());
  }

  @Test
  public void testIsSupportSsoEnabledWhenNotConfigured() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config noSupportConfig = ConfigFactory.parseMap(configMap);
    supportManager.setConfigs(noSupportConfig);

    assertFalse(supportManager.isSupportSsoEnabled());
  }

  @Test
  public void testSetAndGetSupportSsoProvider() {
    OidcSupportConfigs configs = new OidcSupportConfigs.Builder().from(mockConfig).build();
    OidcSupportProvider provider = new OidcSupportProvider(configs);

    supportManager.setSupportSsoProvider(provider);
    SsoProvider<?> retrievedProvider = supportManager.getSupportSsoProvider();

    assertNotNull(retrievedProvider);
    assertEquals(provider, retrievedProvider);
  }

  @Test
  public void testClearSupportSsoProvider() {
    OidcSupportConfigs configs = new OidcSupportConfigs.Builder().from(mockConfig).build();
    OidcSupportProvider provider = new OidcSupportProvider(configs);

    supportManager.setSupportSsoProvider(provider);
    assertNotNull(supportManager.getSupportSsoProvider());

    supportManager.clearSupportSsoProvider();
    assertNull(supportManager.getSupportSsoProvider());
  }

  @Test
  public void testInitializeSupportSsoProviderWhenEnabled() {
    // This test verifies that initialization doesn't throw exceptions
    // The actual provider creation depends on external dependencies
    assertDoesNotThrow(
        () -> {
          supportManager.initializeSupportSsoProvider();
        });
  }

  @Test
  public void testInitializeSupportSsoProviderWhenDisabled() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "false");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config disabledConfig = ConfigFactory.parseMap(configMap);
    supportManager.setConfigs(disabledConfig);

    assertDoesNotThrow(
        () -> {
          supportManager.initializeSupportSsoProvider();
        });

    assertNull(supportManager.getSupportSsoProvider());
  }

  @Test
  public void testInheritanceFromSsoManager() {
    // Test that SsoSupportManager properly inherits from SsoManager
    assertTrue(supportManager instanceof SsoManager);

    // Test inherited methods work correctly
    assertNotNull(supportManager.configs);
    assertNotNull(supportManager.authentication);
    assertNotNull(supportManager.ssoSettingsRequestUrl);
    assertNotNull(supportManager.httpClient);
  }

  @Test
  public void testIsSsoEnabledOverride() {
    // Test that isSsoEnabled() is properly overridden to delegate to isSupportSsoEnabled()
    assertTrue(supportManager.isSsoEnabled());

    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "false");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config disabledConfig = ConfigFactory.parseMap(configMap);
    supportManager.setConfigs(disabledConfig);

    assertFalse(supportManager.isSsoEnabled());
  }

  @Test
  public void testSetConfigs() {
    Map<String, String> newConfigMap = new HashMap<>();
    newConfigMap.put("auth.oidc.support.enabled", "true");
    newConfigMap.put("auth.oidc.support.clientId", "new-client-id");
    newConfigMap.put("auth.oidc.support.clientSecret", "new-client-secret");
    newConfigMap.put(
        "auth.oidc.support.discoveryUri",
        "https://new.example.com/.well-known/openid_configuration");
    newConfigMap.put("auth.baseUrl", "https://new-datahub.example.com");

    Config newConfig = ConfigFactory.parseMap(newConfigMap);
    supportManager.setConfigs(newConfig);

    assertEquals(newConfig, supportManager.configs);
  }

  @Test
  public void testCustomGroupAndAdminRole() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.oidc.support.group", "custom-support-group");
    configMap.put("auth.oidc.support.roleClaim", "custom-role");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config customConfig = ConfigFactory.parseMap(configMap);
    supportManager.setConfigs(customConfig);

    assertDoesNotThrow(
        () -> {
          supportManager.initializeSupportSsoProvider();
        });
  }
}
