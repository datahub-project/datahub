package auth.sso.oidc.support;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import auth.sso.SsoSupportManager;
import client.AuthServiceClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class OidcSupportCallbackLogicTest {

  @Mock private SsoSupportManager mockSsoSupportManager;

  @Mock private SystemEntityClient mockEntityClient;

  @Mock private AuthServiceClient mockAuthClient;

  @Mock private OperationContext mockOperationContext;

  private OidcSupportCallbackLogic callbackLogic;
  private OidcSupportConfigs mockConfigs;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    mockConfigs = new OidcSupportConfigs.Builder().from(config).build();

    callbackLogic =
        new OidcSupportCallbackLogic(
            mockSsoSupportManager,
            mockOperationContext,
            mockEntityClient,
            mockAuthClient,
            null // CookieConfigs can be null for testing
            );
  }

  @Test
  public void testConstructor() {
    assertNotNull(callbackLogic);
  }

  @Test
  public void testHardcodedSupportBehavior() {
    // Test that hardcoded support behavior is enforced
    assertTrue(
        mockConfigs.isJitProvisioningEnabled(),
        "JIT provisioning should always be enabled for support");
    assertFalse(
        mockConfigs.isPreProvisioningRequired(),
        "Pre-provisioning should always be disabled for support");
    assertFalse(
        mockConfigs.isExtractGroupsEnabled(),
        "Group extraction should always be disabled for support");
  }

  @Test
  public void testDefaultGroupAndRoleClaim() {
    assertEquals("", mockConfigs.getGroupClaim());
    assertEquals("role", mockConfigs.getRoleClaim());
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
    configMap.put("auth.oidc.support.groupClaim", "custom-support-group");
    configMap.put("auth.oidc.support.roleClaim", "custom-role");
    configMap.put("auth.baseUrl", "https://datahub.example.com");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs customConfigs = new OidcSupportConfigs.Builder().from(config).build();

    assertEquals("custom-support-group", customConfigs.getGroupClaim());
    assertEquals("custom-role", customConfigs.getRoleClaim());
  }

  @Test
  public void testInheritedOidcConfigs() {
    assertEquals("test-client-id", mockConfigs.getClientId());
    assertEquals("test-client-secret", mockConfigs.getClientSecret());
    assertEquals(
        "https://test.example.com/.well-known/openid_configuration", mockConfigs.getDiscoveryUri());
  }

  @Test
  public void testConfigValidation() {
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

    // Since OidcSupportConfigs doesn't override equals/hashCode, different instances are not equal
    assertNotEquals(supportConfigs1, supportConfigs2);
    assertNotEquals(supportConfigs1.hashCode(), supportConfigs2.hashCode());
  }
}
