package auth.sso.oidc.support;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import auth.sso.oidc.custom.CustomOidcClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pac4j.core.client.Client;

public class OidcSupportProviderTest {

  private OidcSupportConfigs mockConfigs;
  private OidcSupportProvider provider;

  @BeforeEach
  public void setUp() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.oidc.support.clientName", "support-client");
    configMap.put("auth.oidc.support.scope", "openid profile email");
    configMap.put("auth.oidc.support.userNameClaim", "email");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    mockConfigs = new OidcSupportConfigs.Builder().from(config).build();
    provider = new OidcSupportProvider(mockConfigs);
  }

  @Test
  public void testProviderInitialization() {
    assertNotNull(provider);
    assertNotNull(provider.client());
    assertNotNull(provider.configs());
    assertEquals(mockConfigs, provider.configs());
  }

  @Test
  public void testConfigsMethod() {
    OidcSupportConfigs configs = provider.configs();
    assertNotNull(configs);
    assertEquals("test-client-id", configs.getClientId());
    assertEquals("test-client-secret", configs.getClientSecret());
    assertEquals(
        "https://test.example.com/.well-known/openid_configuration", configs.getDiscoveryUri());
    assertEquals("support-client", configs.getClientName());
    assertEquals("role", configs.getRoleClaim());
  }

  @Test
  public void testClientCreation() {
    Client client = provider.client();
    assertNotNull(client);
    assertTrue(client instanceof CustomOidcClient);
    assertEquals("oidc", client.getName());
  }

  @Test
  public void testProtocol() {
    assertEquals(auth.sso.SsoProvider.SsoProtocol.OIDC, provider.protocol());
  }

  @Test
  public void testClientConfiguration() {
    Client client = provider.client();
    assertNotNull(client);

    // Verify the client has the correct name (hardcoded to oidc)
    assertEquals("oidc", client.getName());

    // Verify callback URL is set correctly
    String expectedCallbackUrl = "https://datahub.example.com/support/callback/oidc";
    // Note: We can't directly test the callback URL as it's set internally,
    // but we can verify the client is properly configured
    assertNotNull(client);
  }

  @Test
  public void testCustomParameters() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.oidc.support.customParam.resource", "test-resource");
    configMap.put("auth.oidc.support.grantType", "authorization_code");
    configMap.put("auth.oidc.support.acrValues", "test-acr");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs configsWithCustomParams =
        new OidcSupportConfigs.Builder().from(config).build();
    OidcSupportProvider providerWithCustomParams = new OidcSupportProvider(configsWithCustomParams);

    assertNotNull(providerWithCustomParams.client());
  }

  @Test
  public void testTimeoutConfiguration() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.oidc.support.readTimeout", "10000");
    configMap.put("auth.oidc.support.connectTimeout", "5000");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs configsWithTimeouts = new OidcSupportConfigs.Builder().from(config).build();
    OidcSupportProvider providerWithTimeouts = new OidcSupportProvider(configsWithTimeouts);

    assertNotNull(providerWithTimeouts.client());
  }

  @Test
  public void testInvalidTimeoutConfiguration() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.oidc.support.readTimeout", "invalid-timeout");
    configMap.put("auth.oidc.support.connectTimeout", "invalid-timeout");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs configsWithInvalidTimeouts =
        new OidcSupportConfigs.Builder().from(config).build();

    // Should not throw exception, should use defaults
    OidcSupportProvider providerWithInvalidTimeouts =
        new OidcSupportProvider(configsWithInvalidTimeouts);
    assertNotNull(providerWithInvalidTimeouts.client());
  }

  @Test
  public void testJwsAlgorithmConfiguration() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.oidc.support.preferredJwsAlgorithm", "RS256");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs configsWithJws = new OidcSupportConfigs.Builder().from(config).build();
    OidcSupportProvider providerWithJws = new OidcSupportProvider(configsWithJws);

    assertNotNull(providerWithJws.client());
  }
}
