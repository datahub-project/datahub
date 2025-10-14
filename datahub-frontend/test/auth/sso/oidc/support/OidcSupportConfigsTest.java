package auth.sso.oidc.support;

import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class OidcSupportConfigsTest {

  @Test
  public void testDefaultValues() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs supportConfigs = new OidcSupportConfigs.Builder().from(config).build();

    // Test default values
    assertEquals("", supportConfigs.getGroupClaim());
    assertEquals("role", supportConfigs.getRoleClaim());
    assertTrue(supportConfigs.isJitProvisioningEnabled());
    assertFalse(supportConfigs.isPreProvisioningRequired());
    assertFalse(supportConfigs.isExtractGroupsEnabled());
  }

  @Test
  public void testGroupClaimConfiguration() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    // Test default group claim (empty)
    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs supportConfigs = new OidcSupportConfigs.Builder().from(config).build();
    assertEquals("", supportConfigs.getGroupClaim());

    // Test custom group claim
    configMap.put("auth.oidc.support.groupClaim", "groups");
    config = ConfigFactory.parseMap(configMap);
    supportConfigs = new OidcSupportConfigs.Builder().from(config).build();
    assertEquals("groups", supportConfigs.getGroupClaim());

    // Test JSON configuration
    String jsonConfig = "{\"groupClaim\":\"custom-groups\"}";
    supportConfigs = new OidcSupportConfigs.Builder().from(config, jsonConfig).build();
    assertEquals("custom-groups", supportConfigs.getGroupClaim());
  }

  @Test
  public void testCustomGroupAndRoleClaim() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.oidc.support.groupClaim", "groups");
    configMap.put("auth.oidc.support.roleClaim", "custom-admin");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs supportConfigs = new OidcSupportConfigs.Builder().from(config).build();

    assertEquals("groups", supportConfigs.getGroupClaim());
    assertEquals("custom-admin", supportConfigs.getRoleClaim());
  }

  @Test
  public void testInheritedOidcConfigs() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.oidc.support.userNameClaim", "email");
    configMap.put("auth.oidc.support.scope", "openid profile email");
    configMap.put("auth.oidc.support.clientName", "support-client");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs supportConfigs = new OidcSupportConfigs.Builder().from(config).build();

    // Test inherited OIDC configs
    assertEquals("test-client-id", supportConfigs.getClientId());
    assertEquals("test-client-secret", supportConfigs.getClientSecret());
    assertEquals(
        "https://test.example.com/.well-known/openid_configuration",
        supportConfigs.getDiscoveryUri());
    assertEquals("email", supportConfigs.getUserNameClaim());
    assertEquals("openid profile email", supportConfigs.getScope());
    assertEquals("support-client", supportConfigs.getClientName());
  }

  @Test
  public void testHardcodedSupportBehavior() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs supportConfigs = new OidcSupportConfigs.Builder().from(config).build();

    // Test hardcoded support behavior
    assertTrue(
        supportConfigs.isJitProvisioningEnabled(),
        "JIT provisioning should always be enabled for support");
    assertFalse(
        supportConfigs.isPreProvisioningRequired(),
        "Pre-provisioning should always be disabled for support");
    assertFalse(
        supportConfigs.isExtractGroupsEnabled(),
        "Group extraction should always be disabled for support");
  }

  @Test
  public void testJsonConfigParsing() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);

    String jsonConfig = "{\"groupClaim\":\"json-groups\",\"roleClaim\":\"json-admin\"}";
    OidcSupportConfigs supportConfigs =
        new OidcSupportConfigs.Builder().from(config, jsonConfig).build();

    assertEquals("json-groups", supportConfigs.getGroupClaim());
    assertEquals("json-admin", supportConfigs.getRoleClaim());
  }

  @Test
  public void testValidation() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    // Missing required fields

    Config config = ConfigFactory.parseMap(configMap);

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new OidcSupportConfigs.Builder().from(config).build();
        });
  }

  @Test
  public void testEqualsAndHashCode() {
    Map<String, String> configMap1 = new HashMap<>();
    configMap1.put("auth.oidc.support.enabled", "true");
    configMap1.put("auth.oidc.support.clientId", "test-client-id");
    configMap1.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap1.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap1.put("auth.baseUrl", "https://datahub.example.com");

    Map<String, String> configMap2 = new HashMap<>(configMap1);

    Config config1 = ConfigFactory.parseMap(configMap1);
    Config config2 = ConfigFactory.parseMap(configMap2);

    OidcSupportConfigs supportConfigs1 = new OidcSupportConfigs.Builder().from(config1).build();
    OidcSupportConfigs supportConfigs2 = new OidcSupportConfigs.Builder().from(config2).build();

    // Since OidcSupportConfigs doesn't override equals() and hashCode(),
    // different instances are not equal (reference equality)
    assertNotEquals(supportConfigs1, supportConfigs2);
    assertNotEquals(supportConfigs1.hashCode(), supportConfigs2.hashCode());

    // But they should have the same field values
    assertEquals(supportConfigs1.getClientId(), supportConfigs2.getClientId());
    assertEquals(supportConfigs1.getClientSecret(), supportConfigs2.getClientSecret());
    assertEquals(supportConfigs1.getDiscoveryUri(), supportConfigs2.getDiscoveryUri());
    assertEquals(supportConfigs1.getGroupClaim(), supportConfigs2.getGroupClaim());
    assertEquals(supportConfigs1.getRoleClaim(), supportConfigs2.getRoleClaim());
  }
}
