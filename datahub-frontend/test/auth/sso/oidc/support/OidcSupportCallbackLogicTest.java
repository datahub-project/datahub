package auth.sso.oidc.support;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import auth.sso.SsoSupportManager;
import client.AuthServiceClient;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pac4j.core.profile.CommonProfile;

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

  @Test
  public void testExtractSupportUserWithCompleteProfile() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("test.user@example.com");
    CommonProfile profile = mock(CommonProfile.class);

    when(profile.getFirstName()).thenReturn("John");
    when(profile.getFamilyName()).thenReturn("Doe");
    when(profile.getEmail()).thenReturn("john.doe@example.com");
    when(profile.getPictureUrl()).thenReturn(URI.create("https://example.com/picture.jpg"));
    when(profile.getDisplayName()).thenReturn("John Doe");
    when(profile.getAttribute("name")).thenReturn("John Doe");

    // Act
    CorpUserSnapshot result = callbackLogic.extractSupportUser(userUrn, profile, mockConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(userUrn, result.getUrn());

    CorpUserInfo userInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserInfo())
            .map(aspect -> aspect.getCorpUserInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(userInfo);
    assertTrue(userInfo.isActive());
    assertEquals("John", userInfo.getFirstName());
    assertEquals("Doe", userInfo.getLastName());
    assertEquals("John Doe", userInfo.getFullName());
    assertEquals("john.doe@example.com", userInfo.getEmail());
    assertEquals("John Doe", userInfo.getDisplayName());

    CorpUserEditableInfo editableInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserEditableInfo())
            .map(aspect -> aspect.getCorpUserEditableInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(editableInfo);
    assertNotNull(editableInfo.getPictureLink());
    assertEquals(
        OidcSupportConfigs.DEFAULT_SUPPORT_USER_FALLBACK_PICTURE,
        editableInfo.getPictureLink().toString());
  }

  @Test
  public void testExtractSupportUserWithMinimalProfile() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("minimal.user@example.com");
    CommonProfile profile = mock(CommonProfile.class);

    when(profile.getFirstName()).thenReturn(null);
    when(profile.getFamilyName()).thenReturn(null);
    when(profile.getEmail()).thenReturn("minimal.user@example.com");
    when(profile.getPictureUrl()).thenReturn(null);
    when(profile.getDisplayName()).thenReturn(null);
    when(profile.getAttribute("name")).thenReturn(null);

    // Act
    CorpUserSnapshot result = callbackLogic.extractSupportUser(userUrn, profile, mockConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(userUrn, result.getUrn());

    CorpUserInfo userInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserInfo())
            .map(aspect -> aspect.getCorpUserInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(userInfo);
    assertTrue(userInfo.isActive());
    assertEquals("minimal.user@example.com", userInfo.getEmail());
    assertNull(userInfo.getFirstName());
    assertNull(userInfo.getLastName());
    assertNull(userInfo.getFullName());
    assertNull(userInfo.getDisplayName());
  }

  @Test
  public void testExtractSupportUserWithConfiguredPictureLink() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("test.user@example.com");
    CommonProfile profile = mock(CommonProfile.class);

    when(profile.getFirstName()).thenReturn("John");
    when(profile.getFamilyName()).thenReturn("Doe");
    when(profile.getEmail()).thenReturn("john.doe@example.com");
    when(profile.getPictureUrl()).thenReturn(URI.create("https://example.com/picture.jpg"));
    when(profile.getDisplayName()).thenReturn("John Doe");

    // Create config with custom picture link
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");
    configMap.put("auth.oidc.support.userPictureLink", "https://custom.com/support-logo.png");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs customConfigs = new OidcSupportConfigs.Builder().from(config).build();

    // Act
    CorpUserSnapshot result = callbackLogic.extractSupportUser(userUrn, profile, customConfigs);

    // Assert
    CorpUserEditableInfo editableInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserEditableInfo())
            .map(aspect -> aspect.getCorpUserEditableInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(editableInfo);
    assertNotNull(editableInfo.getPictureLink());
    assertEquals("https://custom.com/support-logo.png", editableInfo.getPictureLink().toString());
  }

  @Test
  public void testExtractSupportUserWithFullNameFromNameAttribute() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("test.user@example.com");
    CommonProfile profile = mock(CommonProfile.class);

    when(profile.getFirstName()).thenReturn("John");
    when(profile.getFamilyName()).thenReturn("Doe");
    when(profile.getEmail()).thenReturn("john.doe@example.com");
    when(profile.getDisplayName()).thenReturn("Johnny");
    when(profile.getAttribute("name")).thenReturn("Johnny Doe");

    // Act
    CorpUserSnapshot result = callbackLogic.extractSupportUser(userUrn, profile, mockConfigs);

    // Assert
    CorpUserInfo userInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserInfo())
            .map(aspect -> aspect.getCorpUserInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(userInfo);
    assertEquals("John", userInfo.getFirstName());
    assertEquals("Doe", userInfo.getLastName());
    assertEquals("Johnny Doe", userInfo.getFullName()); // Uses name attribute
    assertEquals("Johnny", userInfo.getDisplayName()); // Uses displayName
  }

  @Test
  public void testExtractSupportUserWithFullNameFromFirstNameLastName() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("test.user@example.com");
    CommonProfile profile = mock(CommonProfile.class);

    when(profile.getFirstName()).thenReturn("Jane");
    when(profile.getFamilyName()).thenReturn("Smith");
    when(profile.getEmail()).thenReturn("jane.smith@example.com");
    when(profile.getDisplayName()).thenReturn(null);
    when(profile.getAttribute("name")).thenReturn(null);

    // Act
    CorpUserSnapshot result = callbackLogic.extractSupportUser(userUrn, profile, mockConfigs);

    // Assert
    CorpUserInfo userInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserInfo())
            .map(aspect -> aspect.getCorpUserInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(userInfo);
    assertEquals("Jane", userInfo.getFirstName());
    assertEquals("Smith", userInfo.getLastName());
    assertEquals("Jane Smith", userInfo.getFullName()); // Constructed from first + last
    assertEquals("Jane Smith", userInfo.getDisplayName()); // Falls back to fullName
  }

  @Test
  public void testExtractSupportUserWithEmptyPictureLink() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("test.user@example.com");
    CommonProfile profile = mock(CommonProfile.class);

    when(profile.getFirstName()).thenReturn("John");
    when(profile.getFamilyName()).thenReturn("Doe");
    when(profile.getEmail()).thenReturn("john.doe@example.com");
    when(profile.getPictureUrl()).thenReturn(null);
    when(profile.getDisplayName()).thenReturn("John Doe");

    // Create config with empty picture link
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");
    configMap.put("auth.oidc.support.userPictureLink", "");

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs customConfigs = new OidcSupportConfigs.Builder().from(config).build();

    // Act
    CorpUserSnapshot result = callbackLogic.extractSupportUser(userUrn, profile, customConfigs);

    // Assert
    CorpUserEditableInfo editableInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserEditableInfo())
            .map(aspect -> aspect.getCorpUserEditableInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(editableInfo);
    assertNotNull(editableInfo.getPictureLink());
    assertEquals(
        OidcSupportConfigs.DEFAULT_SUPPORT_USER_FALLBACK_PICTURE,
        editableInfo.getPictureLink().toString());
  }

  @Test
  public void testExtractSupportUserWithNullPictureLink() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("test.user@example.com");
    CommonProfile profile = mock(CommonProfile.class);

    when(profile.getFirstName()).thenReturn("John");
    when(profile.getFamilyName()).thenReturn("Doe");
    when(profile.getEmail()).thenReturn("john.doe@example.com");
    when(profile.getPictureUrl()).thenReturn(null);
    when(profile.getDisplayName()).thenReturn("John Doe");

    // Create config with null picture link
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");
    // Don't set userPictureLink, so it will be null

    Config config = ConfigFactory.parseMap(configMap);
    OidcSupportConfigs customConfigs = new OidcSupportConfigs.Builder().from(config).build();

    // Act
    CorpUserSnapshot result = callbackLogic.extractSupportUser(userUrn, profile, customConfigs);

    // Assert
    CorpUserEditableInfo editableInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserEditableInfo())
            .map(aspect -> aspect.getCorpUserEditableInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(editableInfo);
    assertNotNull(editableInfo.getPictureLink());
    assertEquals(
        OidcSupportConfigs.DEFAULT_SUPPORT_USER_FALLBACK_PICTURE,
        editableInfo.getPictureLink().toString());
  }

  @Test
  public void testExtractSupportUserWithOnlyEmail() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("email.only@example.com");
    CommonProfile profile = mock(CommonProfile.class);

    when(profile.getFirstName()).thenReturn(null);
    when(profile.getFamilyName()).thenReturn(null);
    when(profile.getEmail()).thenReturn("email.only@example.com");
    when(profile.getPictureUrl()).thenReturn(null);
    when(profile.getDisplayName()).thenReturn(null);
    when(profile.getAttribute("name")).thenReturn(null);

    // Act
    CorpUserSnapshot result = callbackLogic.extractSupportUser(userUrn, profile, mockConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(userUrn, result.getUrn());

    CorpUserInfo userInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserInfo())
            .map(aspect -> aspect.getCorpUserInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(userInfo);
    assertTrue(userInfo.isActive());
    assertEquals("email.only@example.com", userInfo.getEmail());
    assertNull(userInfo.getFirstName());
    assertNull(userInfo.getLastName());
    assertNull(userInfo.getFullName());
    assertNull(userInfo.getDisplayName());

    CorpUserEditableInfo editableInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserEditableInfo())
            .map(aspect -> aspect.getCorpUserEditableInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(editableInfo);
    assertNotNull(editableInfo.getPictureLink());
    assertEquals(
        OidcSupportConfigs.DEFAULT_SUPPORT_USER_FALLBACK_PICTURE,
        editableInfo.getPictureLink().toString());
  }

  @Test
  public void testExtractSupportUserWithDisplayNameFallback() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("test.user@example.com");
    CommonProfile profile = mock(CommonProfile.class);

    when(profile.getFirstName()).thenReturn("John");
    when(profile.getFamilyName()).thenReturn("Doe");
    when(profile.getEmail()).thenReturn("john.doe@example.com");
    when(profile.getDisplayName()).thenReturn(null);
    when(profile.getAttribute("name")).thenReturn("Johnny Doe");

    // Act
    CorpUserSnapshot result = callbackLogic.extractSupportUser(userUrn, profile, mockConfigs);

    // Assert
    CorpUserInfo userInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserInfo())
            .map(aspect -> aspect.getCorpUserInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(userInfo);
    assertEquals("John", userInfo.getFirstName());
    assertEquals("Doe", userInfo.getLastName());
    assertEquals("Johnny Doe", userInfo.getFullName()); // Uses name attribute
    assertEquals(
        "Johnny Doe", userInfo.getDisplayName()); // Falls back to fullName when displayName is null
  }

  @Test
  public void testExtractSupportUserWithEmptyStringAttributes() {
    // Arrange
    CorpuserUrn userUrn = new CorpuserUrn("test.user@example.com");
    CommonProfile profile = mock(CommonProfile.class);

    when(profile.getFirstName()).thenReturn("");
    when(profile.getFamilyName()).thenReturn("");
    when(profile.getEmail()).thenReturn("test.user@example.com");
    when(profile.getPictureUrl()).thenReturn(null);
    when(profile.getDisplayName()).thenReturn("");
    when(profile.getAttribute("name")).thenReturn("");

    // Act
    CorpUserSnapshot result = callbackLogic.extractSupportUser(userUrn, profile, mockConfigs);

    // Assert
    CorpUserInfo userInfo =
        result.getAspects().stream()
            .filter(aspect -> aspect.isCorpUserInfo())
            .map(aspect -> aspect.getCorpUserInfo())
            .findFirst()
            .orElse(null);

    assertNotNull(userInfo);
    assertTrue(userInfo.isActive());
    assertEquals("test.user@example.com", userInfo.getEmail());
    assertEquals("", userInfo.getFirstName());
    assertEquals("", userInfo.getLastName());
    assertEquals("", userInfo.getFullName());
    assertEquals("", userInfo.getDisplayName());
  }
}
