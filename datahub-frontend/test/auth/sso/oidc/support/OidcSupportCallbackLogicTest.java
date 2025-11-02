package auth.sso.oidc.support;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.pac4j.play.store.PlayCookieSessionStore.*;

import auth.AuthUtils;
import auth.CookieConfigs;
import auth.sso.SsoSupportManager;
import client.AuthServiceClient;
import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.Cookie;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.store.PlayCookieSessionStore;
import play.mvc.Result;

public class OidcSupportCallbackLogicTest {

  @Mock private SsoSupportManager mockSsoSupportManager;
  @Mock private SystemEntityClient mockSystemEntityClient;
  @Mock private Authentication mockAuthentication;
  @Mock private OperationContext mockOperationContext;
  @Mock private AuthServiceClient mockAuthClient;
  @Mock private CookieConfigs mockCookieConfigs;

  private OidcSupportCallbackLogic callbackLogic;
  private OidcSupportConfigs mockConfigs;
  private OidcSupportConfigs realConfigs;

  @BeforeEach
  public void setUp() {
    // Initialize mocks manually since we're not using MockitoExtension
    mockSsoSupportManager = mock(SsoSupportManager.class);
    mockSystemEntityClient = mock(SystemEntityClient.class);
    mockAuthentication = mock(Authentication.class);
    mockOperationContext = mock(OperationContext.class);
    mockAuthClient = mock(AuthServiceClient.class);
    mockCookieConfigs = mock(CookieConfigs.class);

    // Create real configs using a Config object
    java.util.Map<String, String> configMap = new java.util.HashMap<>();
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.oidc.support.clientAuthenticationMethod", "client_secret_basic");
    configMap.put("auth.oidc.support.scope", "openid profile email");
    configMap.put("auth.baseUrl", "https://datahub.example.com");
    configMap.put("auth.oidc.support.userNameClaim", "sub"); // Use "sub" as userNameClaim
    configMap.put("auth.oidc.support.groupClaim", "groups");
    configMap.put("auth.oidc.support.roleClaim", "role");
    configMap.put("auth.oidc.support.userPictureLink", "https://example.com/avatar.png");
    configMap.put("auth.oidc.support.defaultRole", "Admin");
    com.typesafe.config.Config config = com.typesafe.config.ConfigFactory.parseMap(configMap);
    realConfigs = new OidcSupportConfigs.Builder().from(config).build();

    // Create mock configs for testing
    mockConfigs = mock(OidcSupportConfigs.class);

    // Create the callback logic instance
    callbackLogic =
        new OidcSupportCallbackLogic(
            mockSsoSupportManager,
            mockOperationContext,
            mockSystemEntityClient,
            mockAuthClient,
            mockCookieConfigs,
            config);
  }

  @Test
  public void testConstructor() {
    assertNotNull(callbackLogic);
  }

  @Test
  public void testSetContextRedirectUrl() throws Exception {
    // Mock CallContext and WebContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);

    // Mock PlayCookieSessionStore
    PlayCookieSessionStore mockSessionStore = mock(PlayCookieSessionStore.class);
    when(mockCallContext.sessionStore()).thenReturn(mockSessionStore);

    // Mock request cookies
    java.util.List<org.pac4j.core.context.Cookie> requestCookies = new java.util.ArrayList<>();
    org.pac4j.core.context.Cookie mockCookie = mock(org.pac4j.core.context.Cookie.class);
    when(mockCookie.getName()).thenReturn("redirect_url");
    when(mockCookie.getValue()).thenReturn("https://example.com/dashboard");
    requestCookies.add(mockCookie);
    when(mockWebContext.getRequestCookies()).thenReturn(requestCookies);

    // Execute the test
    assertDoesNotThrow(
        () -> {
          callbackLogic.setContextRedirectUrl(mockCallContext);
        });
  }

  @Test
  public void testHandleOidcSupportCallback() throws Exception {
    // Mock the required dependencies
    CallContext mockCallContext = mock(CallContext.class);
    Result mockResult = mock(Result.class);

    // Mock ProfileManager
    ProfileManager mockProfileManager = mock(ProfileManager.class);
    when(mockCallContext.profileManagerFactory())
        .thenReturn((webContext, sessionStore) -> mockProfileManager);
    when(mockProfileManager.isAuthenticated()).thenReturn(true);

    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    when(mockProfileManager.getProfile()).thenReturn(Optional.of(mockProfile));

    // Mock profile attributes
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "test-user");
    attributes.put("email", "test@example.com");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.containsAttribute("sub")).thenReturn(true);
    when(mockProfile.getAttribute("sub")).thenReturn("test-user");

    // Mock SsoSupportManager
    auth.sso.SsoProvider mockSsoProvider = mock(auth.sso.SsoProvider.class);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoProvider.configs()).thenReturn(realConfigs);

    // Mock the configs for the test
    when(mockConfigs.getUserNameClaim()).thenReturn("sub");
    when(mockConfigs.getUserNameClaimRegex()).thenReturn(".*");

    // Execute the test
    Result result =
        callbackLogic.handleOidcSupportCallback(
            mockOperationContext, mockCallContext, mockConfigs, mockResult);

    // Verify the result is not null
    assertNotNull(result);
  }

  @Test
  public void testHandleOidcSupportCallbackWithEmptyProfile() throws Exception {
    // Mock dependencies
    CallContext mockCallContext = mock(CallContext.class);
    Result mockResult = mock(Result.class);

    // Mock ProfileManager to return authenticated but with empty profile
    ProfileManager mockProfileManager = mock(ProfileManager.class);
    when(mockCallContext.profileManagerFactory())
        .thenReturn((webContext, sessionStore) -> mockProfileManager);
    when(mockProfileManager.isAuthenticated()).thenReturn(true);
    when(mockProfileManager.getProfile()).thenReturn(Optional.empty()); // Empty profile

    // Mock configs
    when(mockConfigs.getUserNameClaim()).thenReturn("sub");
    when(mockConfigs.getUserNameClaimRegex()).thenReturn(".*");

    // Execute the test
    Result result =
        callbackLogic.handleOidcSupportCallback(
            mockOperationContext, mockCallContext, mockConfigs, mockResult);

    // Verify the result is an internal server error
    assertNotNull(result);
    assertEquals(500, result.status()); // Internal server error status
  }

  @Test
  public void testExtractUserNameOrThrowWithMissingAttribute() throws Exception {
    // Mock CommonProfile without the required attribute
    CommonProfile mockProfile = mock(CommonProfile.class);
    when(mockProfile.containsAttribute("sub")).thenReturn(false);
    when(mockProfile.getAttributes()).thenReturn(new HashMap<>());

    // Mock configs
    when(mockConfigs.getUserNameClaim()).thenReturn("sub");
    when(mockConfigs.getUserNameClaimRegex()).thenReturn(".*");

    // Execute the test and verify exception is thrown
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> callbackLogic.extractUserNameOrThrow(mockConfigs, mockProfile));

    // Verify the exception message contains expected information
    assertTrue(exception.getMessage().contains("Failed to resolve user name claim"));
    assertTrue(exception.getMessage().contains("Missing attribute"));
    assertTrue(exception.getMessage().contains("sub"));
  }

  @Test
  public void testExtractUserNameOrThrowWithRegexFailure() throws Exception {
    // Mock CommonProfile with attribute but regex that won't match
    CommonProfile mockProfile = mock(CommonProfile.class);
    when(mockProfile.containsAttribute("sub")).thenReturn(true);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");

    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    when(mockProfile.getAttributes()).thenReturn(attributes);

    // Mock configs with regex that won't match the username
    when(mockConfigs.getUserNameClaim()).thenReturn("sub");
    when(mockConfigs.getUserNameClaimRegex())
        .thenReturn("^admin_.*"); // Won't match "testuser@example.com"

    // Execute the test and verify exception is thrown
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> callbackLogic.extractUserNameOrThrow(mockConfigs, mockProfile));

    // Verify the exception message contains expected information
    assertTrue(exception.getMessage().contains("Failed to extract DataHub username"));
    assertTrue(exception.getMessage().contains("testuser@example.com"));
    assertTrue(exception.getMessage().contains("^admin_.*"));
  }

  @Test
  public void testExtractGroupFromClaim() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("groups", "support-staff");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.getAttribute("groups")).thenReturn("support-staff");

    // Mock configs with group claim
    when(mockConfigs.getGroupClaim()).thenReturn("groups");

    // Execute the test
    String result = callbackLogic.extractGroupFromClaim(mockProfile, mockConfigs);

    // Verify the result
    assertNotNull(result);
    assertEquals("support-staff", result);
  }

  @Test
  public void testExtractGroupFromClaimWithEmptyClaim() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    when(mockProfile.getAttributes()).thenReturn(attributes);

    // Mock configs with empty group claim
    when(mockConfigs.getGroupClaim()).thenReturn("");

    // Execute the test
    String result = callbackLogic.extractGroupFromClaim(mockProfile, mockConfigs);

    // Verify the result uses default group
    assertNotNull(result);
    assertEquals(OidcSupportConfigs.DEFAULT_SUPPORT_GROUP_NAME, result);
  }

  @Test
  public void testExtractGroupFromClaimWithNullValue() throws Exception {
    // Mock CommonProfile with null group value
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.getAttribute("groups")).thenReturn(null); // Null value

    // Mock configs with group claim
    when(mockConfigs.getGroupClaim()).thenReturn("groups");

    // Execute the test
    String result = callbackLogic.extractGroupFromClaim(mockProfile, mockConfigs);

    // Verify the result uses default group when groupValue is null
    assertNotNull(result);
    assertEquals(OidcSupportConfigs.DEFAULT_SUPPORT_GROUP_NAME, result);
  }

  @Test
  public void testCreateSupportGroup() throws Exception {
    // Mock configs
    when(mockConfigs.getGroupClaim()).thenReturn("groups");

    // Execute the test
    CorpGroupSnapshot result = callbackLogic.createSupportGroup(mockConfigs, "test-group");

    // Verify the result
    assertNotNull(result);
    assertNotNull(result.getUrn());
    assertEquals("test-group", result.getUrn().getGroupNameEntity());
    assertNotNull(result.getAspects());
    assertEquals(1, result.getAspects().size());
  }

  @Test
  public void testCreateSupportGroupMembership() throws Exception {
    // Create a real CorpGroupSnapshot with a real CorpGroupUrn
    CorpGroupSnapshot mockGroup = mock(CorpGroupSnapshot.class);
    CorpGroupUrn realUrn = new CorpGroupUrn("test-group");
    when(mockGroup.getUrn()).thenReturn(realUrn);

    // Execute the test
    GroupMembership result = callbackLogic.createSupportGroupMembership(mockGroup);

    // Verify the result
    assertNotNull(result);
    assertNotNull(result.getGroups());
    assertEquals(1, result.getGroups().size());
    assertEquals(realUrn, result.getGroups().get(0));
  }

  @Test
  public void testUpdateGroupMembership() throws Exception {
    // Mock dependencies
    Urn mockUrn = mock(Urn.class);
    GroupMembership mockMembership = mock(GroupMembership.class);

    // Mock the systemEntityClient to return a successful response
    when(mockSystemEntityClient.ingestProposal(any(), any())).thenReturn("success");

    // Execute the test
    assertDoesNotThrow(
        () -> {
          callbackLogic.updateGroupMembership(mockOperationContext, mockUrn, mockMembership);
        });

    // Verify the systemEntityClient was called
    verify(mockSystemEntityClient).ingestProposal(any(), any());
  }

  @Test
  public void testUpdateGroupMembershipWithRemoteInvocationException() throws Exception {
    // Mock dependencies
    Urn mockUrn = mock(Urn.class);
    GroupMembership mockMembership = mock(GroupMembership.class);

    // Mock the systemEntityClient to throw RemoteInvocationException
    com.linkedin.r2.RemoteInvocationException remoteException =
        new com.linkedin.r2.RemoteInvocationException("Remote service unavailable");
    when(mockSystemEntityClient.ingestProposal(any(), any())).thenThrow(remoteException);

    // Execute the test and verify exception is thrown
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                callbackLogic.updateGroupMembership(mockOperationContext, mockUrn, mockMembership));

    // Verify the exception message contains expected information
    assertTrue(
        exception
            .getMessage()
            .contains("Failed to update group membership for support user with urn"));
    assertTrue(exception.getCause() instanceof com.linkedin.r2.RemoteInvocationException);
    assertEquals(remoteException, exception.getCause());

    // Verify the systemEntityClient was called
    verify(mockSystemEntityClient).ingestProposal(any(), any());
  }

  @Test
  public void testExtractRegexGroupWithMatch() throws Exception {
    // Test successful regex match
    String pattern = "admin_(.+)";
    String target = "admin_testuser@example.com";

    // Execute the test
    Optional<String> result = callbackLogic.extractRegexGroup(pattern, target);

    // Verify the result
    assertTrue(result.isPresent());
    assertEquals("admin_testuser@example.com", result.get());
  }

  @Test
  public void testExtractRegexGroupWithNoMatch() throws Exception {
    // Test regex that doesn't match
    String pattern = "admin_(.+)";
    String target = "regular_user@example.com";

    // Execute the test
    Optional<String> result = callbackLogic.extractRegexGroup(pattern, target);

    // Verify the result is empty when no match is found
    assertFalse(result.isPresent());
    assertEquals(Optional.empty(), result);
  }

  @Test
  public void testExtractRegexGroupWithEmptyTarget() throws Exception {
    // Test regex with empty target string
    String pattern = "admin_(.+)";
    String target = "";

    // Execute the test
    Optional<String> result = callbackLogic.extractRegexGroup(pattern, target);

    // Verify the result is empty when target is empty
    assertFalse(result.isPresent());
    assertEquals(Optional.empty(), result);
  }

  @Test
  public void testExtractRegexGroupWithComplexPattern() throws Exception {
    // Test complex regex pattern with multiple groups
    String pattern = "user_(.+)_(.+)";
    String target = "user_john_doe";

    // Execute the test
    Optional<String> result = callbackLogic.extractRegexGroup(pattern, target);

    // Verify the result
    assertTrue(result.isPresent());
    assertEquals("user_john_doe", result.get());
  }

  @Test
  public void testExtractRoleFromClaim() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("role", "Admin");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.getAttribute("role")).thenReturn("Admin");

    // Mock configs with role claim
    when(mockConfigs.getRoleClaim()).thenReturn("role");

    // Execute the test
    String result = callbackLogic.extractRoleFromClaim(mockProfile, mockConfigs);

    // Verify the result
    assertNotNull(result);
    assertEquals("Admin", result);
  }

  @Test
  public void testExtractRoleFromClaimWithNullValue() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.getAttribute("role")).thenReturn(null);

    // Mock configs with role claim
    when(mockConfigs.getRoleClaim()).thenReturn("role");

    // Execute the test
    String result = callbackLogic.extractRoleFromClaim(mockProfile, mockConfigs);

    // Verify the result is null
    assertNull(result);
  }

  @Test
  public void testExtractRoleFromClaimWithException() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    when(mockProfile.getAttribute("role")).thenThrow(new RuntimeException("Test exception"));

    // Mock configs with role claim
    when(mockConfigs.getRoleClaim()).thenReturn("role");

    // Execute the test
    String result = callbackLogic.extractRoleFromClaim(mockProfile, mockConfigs);

    // Verify the result is null when exception occurs
    assertNull(result);
  }

  @Test
  public void testAssignRoleToUser() throws Exception {
    // Mock dependencies
    CorpuserUrn mockUserUrn = mock(CorpuserUrn.class);
    String roleName = "Admin";

    // Mock the systemEntityClient - return null for existing aspect (no existing roles)
    when(mockSystemEntityClient.getLatestAspectObject(any(), any(), any(), anyBoolean()))
        .thenReturn(null);
    // Mock successful proposal ingestion
    when(mockSystemEntityClient.ingestProposal(any(), any())).thenReturn("success");

    // Execute the test - use AuthUtils.manageUserRole instead
    assertDoesNotThrow(
        () -> {
          AuthUtils.manageUserRole(
              mockOperationContext, mockUserUrn, roleName, true, mockSystemEntityClient);
        });

    // Verify the systemEntityClient was called for getting existing aspect and ingesting
    verify(mockSystemEntityClient).getLatestAspectObject(any(), any(), any(), anyBoolean());
    verify(mockSystemEntityClient).ingestProposal(any(), any());
  }

  @Test
  public void testAssignRoleToUserWithException() throws Exception {
    // Mock dependencies
    CorpuserUrn mockUserUrn = mock(CorpuserUrn.class);
    String roleName = "Admin";

    // Mock the systemEntityClient - return null for existing aspect (no existing roles)
    when(mockSystemEntityClient.getLatestAspectObject(any(), any(), any(), anyBoolean()))
        .thenReturn(null);
    // Mock ingestProposal to throw an exception (this is the security-critical failure)
    when(mockSystemEntityClient.ingestProposal(any(), any()))
        .thenThrow(new RuntimeException("Test exception"));

    // Execute the test - should throw exception (method now throws on failure for security)
    assertThrows(
        RuntimeException.class,
        () -> {
          AuthUtils.manageUserRole(
              mockOperationContext, mockUserUrn, roleName, true, mockSystemEntityClient);
        });

    // Verify the systemEntityClient was called for getting existing aspect and ingesting
    verify(mockSystemEntityClient).getLatestAspectObject(any(), any(), any(), anyBoolean());
    verify(mockSystemEntityClient).ingestProposal(any(), any());
  }

  @Test
  public void testSetContextRedirectUrlWithPresentCookie() throws Exception {
    // Mock CallContext and its dependencies
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    PlayCookieSessionStore mockSessionStore = mock(PlayCookieSessionStore.class);

    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockCallContext.sessionStore()).thenReturn(mockSessionStore);

    // Create a test redirect URL and encode it properly
    String testRedirectUrl = "/dashboard";
    byte[] compressedBytes = compressBytes(testRedirectUrl.getBytes());
    String encodedCookieValue = Base64.getEncoder().encodeToString(compressedBytes);

    // Mock cookie with the correct name (REDIRECT_URL_COOKIE_NAME)
    Cookie mockCookie = mock(Cookie.class);
    when(mockCookie.getName()).thenReturn("REDIRECT_URL");
    when(mockCookie.getValue()).thenReturn(encodedCookieValue);

    // Mock request cookies list
    List<Cookie> mockCookies = List.of(mockCookie);
    when(mockWebContext.getRequestCookies()).thenReturn(mockCookies);

    // Mock the serializer to avoid null pointer exception
    org.pac4j.core.util.serializer.Serializer mockSerializer =
        mock(org.pac4j.core.util.serializer.Serializer.class);
    when(mockSessionStore.getSerializer()).thenReturn(mockSerializer);
    when(mockSerializer.deserializeFromBytes(any())).thenReturn(testRedirectUrl);

    // Execute the test
    assertDoesNotThrow(
        () -> {
          callbackLogic.setContextRedirectUrl(mockCallContext);
        });

    // Verify that sessionStore.set was called (we can't easily verify the exact parameters due to
    // complex
    // mocking, but we can verify the method was called)
    verify(mockSessionStore).set(any(), any(), any());
  }

  @Test
  public void testSetContextRedirectUrlWithoutCookie() throws Exception {
    // Mock CallContext and its dependencies
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    PlayCookieSessionStore mockSessionStore = mock(PlayCookieSessionStore.class);

    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockCallContext.sessionStore()).thenReturn(mockSessionStore);

    // Mock empty request cookies list (no redirect cookie)
    List<Cookie> mockCookies = List.of();
    when(mockWebContext.getRequestCookies()).thenReturn(mockCookies);

    // Execute the test
    assertDoesNotThrow(
        () -> {
          callbackLogic.setContextRedirectUrl(mockCallContext);
        });

    // Verify that the method was called without throwing an exception
  }

  @Test
  public void testSetContextRedirectUrlWithWrongCookieName() throws Exception {
    // Mock CallContext and its dependencies
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    PlayCookieSessionStore mockSessionStore = mock(PlayCookieSessionStore.class);

    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockCallContext.sessionStore()).thenReturn(mockSessionStore);

    // Mock cookie with wrong name
    Cookie mockCookie = mock(Cookie.class);
    when(mockCookie.getName()).thenReturn("wrong_cookie_name");
    when(mockCookie.getValue()).thenReturn("some_value");

    // Mock request cookies list
    List<Cookie> mockCookies = List.of(mockCookie);
    when(mockWebContext.getRequestCookies()).thenReturn(mockCookies);

    // Execute the test
    assertDoesNotThrow(
        () -> {
          callbackLogic.setContextRedirectUrl(mockCallContext);
        });

    // Verify that the method was called without throwing an exception
  }

  // Helper method to simulate compression (simplified version)
  private byte[] compressBytes(byte[] input) {
    // For testing purposes, we'll just return the input bytes
    // In real implementation, this would use compression
    return input;
  }

  @Test
  public void testUserProvisioningWithRoleClaim() throws Exception {
    // Mock CommonProfile with role claim
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    attributes.put("name", "Test User");
    attributes.put("groups", "support-staff");
    attributes.put("role", "Admin");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");
    when(mockProfile.getAttribute("name")).thenReturn("Test User");
    when(mockProfile.getAttribute("groups")).thenReturn("support-staff");
    when(mockProfile.getAttribute("role")).thenReturn("Admin");
    when(mockProfile.getEmail()).thenReturn("testuser@example.com");
    when(mockProfile.getDisplayName()).thenReturn("Test User");
    when(mockProfile.getFirstName()).thenReturn("Test");
    when(mockProfile.getFamilyName()).thenReturn("User");

    // Mock configs
    when(mockConfigs.getUserNameClaim()).thenReturn("sub");
    when(mockConfigs.getUserNameClaimRegex()).thenReturn(".*");
    when(mockConfigs.getGroupClaim()).thenReturn("groups");
    when(mockConfigs.getRoleClaim()).thenReturn("role");
    when(mockConfigs.getDefaultRole()).thenReturn("User");

    // Mock AuthUtils.tryProvisionUser
    when(mockSystemEntityClient.ingestProposal(any(), any())).thenReturn("success");

    // Execute the test
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");
    CorpUserSnapshot extractedUser =
        callbackLogic.extractSupportUser(userUrn, mockProfile, mockConfigs);

    // Verify the extracted user
    assertNotNull(extractedUser);
    assertEquals(userUrn, extractedUser.getUrn());
    assertNotNull(extractedUser.getAspects());
    assertEquals(2, extractedUser.getAspects().size()); // CorpUserInfo and CorpUserEditableInfo

    // Verify CorpUserInfo
    CorpUserInfo userInfo = extractedUser.getAspects().get(0).getCorpUserInfo();
    assertNotNull(userInfo);
    assertEquals("Test User", userInfo.getDisplayName());
    assertEquals("testuser@example.com", userInfo.getEmail());

    // Verify CorpUserEditableInfo (only has picture link, not display name or email)
    CorpUserEditableInfo editableInfo = extractedUser.getAspects().get(1).getCorpUserEditableInfo();
    assertNotNull(editableInfo);
    assertNotNull(editableInfo.getPictureLink());
  }

  @Test
  public void testUserProvisioningWithDefaultRole() throws Exception {
    // Mock CommonProfile without role claim
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    attributes.put("name", "Test User");
    attributes.put("groups", "support-staff");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");
    when(mockProfile.getAttribute("name")).thenReturn("Test User");
    when(mockProfile.getAttribute("groups")).thenReturn("support-staff");
    when(mockProfile.getAttribute("role")).thenReturn(null);
    when(mockProfile.getEmail()).thenReturn("testuser@example.com");
    when(mockProfile.getDisplayName()).thenReturn("Test User");
    when(mockProfile.getFirstName()).thenReturn("Test");
    when(mockProfile.getFamilyName()).thenReturn("User");

    // Mock configs
    when(mockConfigs.getUserNameClaim()).thenReturn("sub");
    when(mockConfigs.getUserNameClaimRegex()).thenReturn(".*");
    when(mockConfigs.getGroupClaim()).thenReturn("groups");
    when(mockConfigs.getRoleClaim()).thenReturn("role");
    when(mockConfigs.getDefaultRole()).thenReturn("Admin");

    // Mock AuthUtils.tryProvisionUser
    when(mockSystemEntityClient.ingestProposal(any(), any())).thenReturn("success");

    // Execute the test
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");
    CorpUserSnapshot extractedUser =
        callbackLogic.extractSupportUser(userUrn, mockProfile, mockConfigs);

    // Verify the extracted user
    assertNotNull(extractedUser);
    assertEquals(userUrn, extractedUser.getUrn());
    assertNotNull(extractedUser.getAspects());
    assertEquals(2, extractedUser.getAspects().size());
  }

  @Test
  public void testUserProvisioningWithMinimalProfile() throws Exception {
    // Mock CommonProfile with minimal attributes
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");
    when(mockProfile.getAttribute("name")).thenReturn(null);
    when(mockProfile.getEmail()).thenReturn(null);
    when(mockProfile.getDisplayName()).thenReturn(null);
    when(mockProfile.getFirstName()).thenReturn(null);
    when(mockProfile.getFamilyName()).thenReturn(null);

    // Mock configs
    when(mockConfigs.getUserNameClaim()).thenReturn("sub");
    when(mockConfigs.getUserNameClaimRegex()).thenReturn(".*");

    // Mock AuthUtils.tryProvisionUser
    when(mockSystemEntityClient.ingestProposal(any(), any())).thenReturn("success");

    // Execute the test
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");
    CorpUserSnapshot extractedUser =
        callbackLogic.extractSupportUser(userUrn, mockProfile, mockConfigs);

    // Verify the extracted user
    assertNotNull(extractedUser);
    assertEquals(userUrn, extractedUser.getUrn());
    assertNotNull(extractedUser.getAspects());
    assertEquals(2, extractedUser.getAspects().size());

    // Verify CorpUserInfo with fallback values
    CorpUserInfo userInfo = extractedUser.getAspects().get(0).getCorpUserInfo();
    assertNotNull(userInfo);
    // For minimal profile, displayName should be null since both displayName and fullName are null
    assertNull(userInfo.getDisplayName());
    // Email should be null since we mocked it as null
    assertNull(userInfo.getEmail());
  }

  @Test
  public void testCompleteUserProvisioningFlow() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    attributes.put("name", "Test User");
    attributes.put("groups", "support-staff");
    attributes.put("role", "Admin");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");
    when(mockProfile.getAttribute("name")).thenReturn("Test User");
    when(mockProfile.getAttribute("groups")).thenReturn("support-staff");
    when(mockProfile.getAttribute("role")).thenReturn("Admin");
    when(mockProfile.getEmail()).thenReturn("testuser@example.com");
    when(mockProfile.getDisplayName()).thenReturn("Test User");
    when(mockProfile.getFirstName()).thenReturn("Test");
    when(mockProfile.getFamilyName()).thenReturn("User");

    // Mock configs
    when(mockConfigs.getUserNameClaim()).thenReturn("sub");
    when(mockConfigs.getUserNameClaimRegex()).thenReturn(".*");
    when(mockConfigs.getGroupClaim()).thenReturn("groups");
    when(mockConfigs.getRoleClaim()).thenReturn("role");
    when(mockConfigs.getDefaultRole()).thenReturn("User");

    // Mock AuthUtils.tryProvisionUser
    when(mockSystemEntityClient.ingestProposal(any(), any())).thenReturn("success");

    // Execute the complete flow
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");

    // 1. Extract user
    CorpUserSnapshot extractedUser =
        callbackLogic.extractSupportUser(userUrn, mockProfile, mockConfigs);
    assertNotNull(extractedUser);

    // 2. Extract group
    String groupName = callbackLogic.extractGroupFromClaim(mockProfile, mockConfigs);
    assertEquals("support-staff", groupName);

    // 3. Create support group
    CorpGroupSnapshot supportGroup = callbackLogic.createSupportGroup(mockConfigs, groupName);
    assertNotNull(supportGroup);
    assertEquals("support-staff", supportGroup.getUrn().getGroupNameEntity());

    // 4. Create group membership
    GroupMembership membership = callbackLogic.createSupportGroupMembership(supportGroup);
    assertNotNull(membership);
    assertEquals(1, membership.getGroups().size());
    assertEquals(supportGroup.getUrn(), membership.getGroups().get(0));

    // 5. Update group membership
    assertDoesNotThrow(
        () -> {
          callbackLogic.updateGroupMembership(mockOperationContext, userUrn, membership);
        });

    // 6. Extract role
    String roleFromClaim = callbackLogic.extractRoleFromClaim(mockProfile, mockConfigs);
    assertEquals("Admin", roleFromClaim);

    // 7. Assign role - use AuthUtils.manageUserRole instead
    assertDoesNotThrow(
        () -> {
          AuthUtils.manageUserRole(
              mockOperationContext, userUrn, roleFromClaim, true, mockSystemEntityClient);
        });

    // Verify that systemEntityClient was called multiple times
    verify(mockSystemEntityClient, atLeast(2)).ingestProposal(any(), any());
  }

  @Test
  public void testUserProvisioningWithConfiguredPictureLink() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    attributes.put("name", "Test User");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");
    when(mockProfile.getAttribute("name")).thenReturn("Test User");
    when(mockProfile.getEmail()).thenReturn("testuser@example.com");
    when(mockProfile.getDisplayName()).thenReturn("Test User");
    when(mockProfile.getFirstName()).thenReturn("Test");
    when(mockProfile.getFamilyName()).thenReturn("User");

    // Mock configs with configured picture link
    when(mockConfigs.getUserNameClaim()).thenReturn("sub");
    when(mockConfigs.getUserNameClaimRegex()).thenReturn(".*");
    when(mockConfigs.getUserPictureLink()).thenReturn("https://example.com/custom-avatar.png");

    // Mock AuthUtils.tryProvisionUser
    when(mockSystemEntityClient.ingestProposal(any(), any())).thenReturn("success");

    // Execute the test
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");
    CorpUserSnapshot extractedUser =
        callbackLogic.extractSupportUser(userUrn, mockProfile, mockConfigs);

    // Verify the extracted user
    assertNotNull(extractedUser);
    assertEquals(userUrn, extractedUser.getUrn());
    assertNotNull(extractedUser.getAspects());
    assertEquals(2, extractedUser.getAspects().size());

    // Verify CorpUserInfo
    CorpUserInfo userInfo = extractedUser.getAspects().get(0).getCorpUserInfo();
    assertNotNull(userInfo);
    assertEquals("Test User", userInfo.getDisplayName());
    assertEquals("testuser@example.com", userInfo.getEmail());

    // Verify CorpUserEditableInfo has the configured picture link
    CorpUserEditableInfo editableInfo = extractedUser.getAspects().get(1).getCorpUserEditableInfo();
    assertNotNull(editableInfo);
    assertNotNull(editableInfo.getPictureLink());
    assertEquals("https://example.com/custom-avatar.png", editableInfo.getPictureLink().toString());
  }

  @Test
  public void testUserProvisioningWithDefaultGroup() throws Exception {
    // Mock CommonProfile without group claim
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    attributes.put("name", "Test User");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");
    when(mockProfile.getAttribute("name")).thenReturn("Test User");
    when(mockProfile.getAttribute("groups")).thenReturn(null);
    when(mockProfile.getEmail()).thenReturn("testuser@example.com");
    when(mockProfile.getDisplayName()).thenReturn("Test User");
    when(mockProfile.getFirstName()).thenReturn("Test");
    when(mockProfile.getFamilyName()).thenReturn("User");

    // Mock configs with empty group claim
    when(mockConfigs.getUserNameClaim()).thenReturn("sub");
    when(mockConfigs.getUserNameClaimRegex()).thenReturn(".*");
    when(mockConfigs.getGroupClaim()).thenReturn("");

    // Mock AuthUtils.tryProvisionUser
    when(mockSystemEntityClient.ingestProposal(any(), any())).thenReturn("success");

    // Execute the test
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");

    // Extract group should return default
    String groupName = callbackLogic.extractGroupFromClaim(mockProfile, mockConfigs);
    assertEquals(OidcSupportConfigs.DEFAULT_SUPPORT_GROUP_NAME, groupName);

    // Create support group with default name
    CorpGroupSnapshot supportGroup = callbackLogic.createSupportGroup(mockConfigs, groupName);
    assertNotNull(supportGroup);
    assertEquals(
        OidcSupportConfigs.DEFAULT_SUPPORT_GROUP_NAME, supportGroup.getUrn().getGroupNameEntity());
  }

  @Test
  public void testReadTicketIdFromCookie_WithEncodedValue() throws Exception {
    // Use reflection to access private method for testing
    java.lang.reflect.Method method =
        OidcSupportCallbackLogic.class.getDeclaredMethod(
            "readTicketIdFromCookie", CallContext.class);
    method.setAccessible(true);

    // Mock CallContext and WebContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);

    // Create a URL-encoded ticket ID (as it would be stored in the cookie)
    String originalTicketId = "test ticket with spaces & special chars";
    String encodedTicketId =
        java.net.URLEncoder.encode(originalTicketId, java.nio.charset.StandardCharsets.UTF_8);

    // Mock cookie with encoded value
    Cookie mockCookie = mock(Cookie.class);
    when(mockCookie.getName()).thenReturn(AuthUtils.SUPPORT_TICKET_ID_COOKIE_NAME);
    when(mockCookie.getValue()).thenReturn(encodedTicketId);

    List<Cookie> mockCookies = List.of(mockCookie);
    when(mockWebContext.getRequestCookies()).thenReturn(mockCookies);

    // Execute the test
    @SuppressWarnings("unchecked")
    Optional<String> result = (Optional<String>) method.invoke(callbackLogic, mockCallContext);

    // Verify - should return decoded ticket ID
    assertTrue(result.isPresent());
    assertEquals(originalTicketId, result.get());
  }

  @Test
  public void testReadTicketIdFromCookie_NoCookie() throws Exception {
    // Use reflection to access private method for testing
    java.lang.reflect.Method method =
        OidcSupportCallbackLogic.class.getDeclaredMethod(
            "readTicketIdFromCookie", CallContext.class);
    method.setAccessible(true);

    // Mock CallContext and WebContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);

    // Mock empty cookies list
    List<Cookie> mockCookies = List.of();
    when(mockWebContext.getRequestCookies()).thenReturn(mockCookies);

    // Execute the test
    @SuppressWarnings("unchecked")
    Optional<String> result = (Optional<String>) method.invoke(callbackLogic, mockCallContext);

    // Verify - should return empty
    assertFalse(result.isPresent());
  }

  @Test
  public void testReadTicketIdFromCookie_EmptyValue() throws Exception {
    // Use reflection to access private method for testing
    java.lang.reflect.Method method =
        OidcSupportCallbackLogic.class.getDeclaredMethod(
            "readTicketIdFromCookie", CallContext.class);
    method.setAccessible(true);

    // Mock CallContext and WebContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);

    // Mock cookie with empty value
    Cookie mockCookie = mock(Cookie.class);
    when(mockCookie.getName()).thenReturn(AuthUtils.SUPPORT_TICKET_ID_COOKIE_NAME);
    when(mockCookie.getValue()).thenReturn("");

    List<Cookie> mockCookies = List.of(mockCookie);
    when(mockWebContext.getRequestCookies()).thenReturn(mockCookies);

    // Execute the test
    @SuppressWarnings("unchecked")
    Optional<String> result = (Optional<String>) method.invoke(callbackLogic, mockCallContext);

    // Verify - should return empty
    assertFalse(result.isPresent());
  }

  @Test
  public void testReadTicketIdFromCookie_ExceptionHandling() throws Exception {
    // Use reflection to access private method for testing
    java.lang.reflect.Method method =
        OidcSupportCallbackLogic.class.getDeclaredMethod(
            "readTicketIdFromCookie", CallContext.class);
    method.setAccessible(true);

    // Mock CallContext to throw exception
    CallContext mockCallContext = mock(CallContext.class);
    when(mockCallContext.webContext()).thenThrow(new RuntimeException("Test exception"));

    // Execute the test - should handle exception gracefully
    @SuppressWarnings("unchecked")
    Optional<String> result = (Optional<String>) method.invoke(callbackLogic, mockCallContext);

    // Verify - should return empty on exception
    assertFalse(result.isPresent());
  }

  @Test
  public void testEnsureUserSupportFlagCalledDuringLogin() throws Exception {
    // Mock CommonProfile - realConfigs uses "sub" as userNameClaim
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    attributes.put("email", "testuser@example.com");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.containsAttribute("sub")).thenReturn(true);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");
    when(mockProfile.getEmail()).thenReturn("testuser@example.com");

    // Mock ProfileManager
    ProfileManager mockProfileManager = mock(ProfileManager.class);
    when(mockProfileManager.isAuthenticated()).thenReturn(true);
    when(mockProfileManager.getProfile()).thenReturn(Optional.of(mockProfile));

    // Mock CallContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockCallContext.profileManagerFactory())
        .thenReturn((webContext, sessionStore) -> mockProfileManager);

    // Mock empty cookies (no ticket ID)
    when(mockWebContext.getRequestCookies()).thenReturn(List.of());

    // Mock SsoSupportManager
    auth.sso.SsoProvider mockSsoProvider = mock(auth.sso.SsoProvider.class);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoProvider.configs()).thenReturn(realConfigs);

    // Mock user exists
    CorpUserSnapshot existingUserSnapshot = new CorpUserSnapshot();
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");
    existingUserSnapshot.setUrn(userUrn);
    com.linkedin.metadata.aspect.CorpUserAspectArray aspects =
        new com.linkedin.metadata.aspect.CorpUserAspectArray();
    aspects.add(com.linkedin.metadata.aspect.CorpUserAspect.create(new CorpUserInfo()));
    existingUserSnapshot.setAspects(aspects);

    Entity existingUserEntity = new Entity();
    existingUserEntity.setValue(Snapshot.create(existingUserSnapshot));

    when(mockSystemEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(existingUserEntity);

    // Mock AuthUtils.ensureUserSupportFlag to track calls
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);
    when(mockSystemEntityClient.ingestProposal(any(OperationContext.class), any()))
        .thenReturn("success");

    // Mock other required calls
    when(mockCookieConfigs.getAuthCookieSameSite()).thenReturn("LAX");
    when(mockCookieConfigs.getAuthCookieSecure()).thenReturn(false);
    when(mockAuthClient.generateSessionTokenForUser(anyString(), anyString(), any()))
        .thenReturn("test-token");

    // Mock empty group membership
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.GROUP_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);

    Result mockResult = mock(Result.class);
    when(mockResult.withSession(any(Map.class))).thenReturn(mockResult);
    when(mockResult.withCookies(any(play.mvc.Http.Cookie.class))).thenReturn(mockResult);

    // Execute the test
    callbackLogic.handleOidcSupportCallback(
        mockOperationContext, mockCallContext, realConfigs, mockResult);

    // Verify that ensureUserSupportFlag was called (via systemEntityClient.ingestProposal
    // for CorpUserInfo)
    verify(mockSystemEntityClient, atLeastOnce())
        .getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false));
    verify(mockSystemEntityClient, atLeastOnce())
        .ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testManageUserRoleCalledDuringLogin() throws Exception {
    // Mock CommonProfile with role claim
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    attributes.put("email", "testuser@example.com");
    attributes.put("role", "Admin");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.containsAttribute("sub")).thenReturn(true);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");
    when(mockProfile.getAttribute("role")).thenReturn("Admin");
    when(mockProfile.getEmail()).thenReturn("testuser@example.com");

    // Mock ProfileManager
    ProfileManager mockProfileManager = mock(ProfileManager.class);
    when(mockProfileManager.isAuthenticated()).thenReturn(true);
    when(mockProfileManager.getProfile()).thenReturn(Optional.of(mockProfile));

    // Mock CallContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockCallContext.profileManagerFactory())
        .thenReturn((webContext, sessionStore) -> mockProfileManager);

    // Mock empty cookies
    when(mockWebContext.getRequestCookies()).thenReturn(List.of());

    // Mock SsoSupportManager
    auth.sso.SsoProvider mockSsoProvider = mock(auth.sso.SsoProvider.class);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoProvider.configs()).thenReturn(realConfigs);

    // Mock user exists
    CorpUserSnapshot existingUserSnapshot = new CorpUserSnapshot();
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");
    existingUserSnapshot.setUrn(userUrn);
    com.linkedin.metadata.aspect.CorpUserAspectArray aspects =
        new com.linkedin.metadata.aspect.CorpUserAspectArray();
    aspects.add(com.linkedin.metadata.aspect.CorpUserAspect.create(new CorpUserInfo()));
    existingUserSnapshot.setAspects(aspects);

    Entity existingUserEntity = new Entity();
    existingUserEntity.setValue(Snapshot.create(existingUserSnapshot));

    when(mockSystemEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(existingUserEntity);

    // Mock ensureUserSupportFlag
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);

    // Mock manageUserRole (RoleMembership aspect)
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.ROLE_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);

    when(mockSystemEntityClient.ingestProposal(any(OperationContext.class), any()))
        .thenReturn("success");

    // Mock other required calls
    when(mockCookieConfigs.getAuthCookieSameSite()).thenReturn("LAX");
    when(mockCookieConfigs.getAuthCookieSecure()).thenReturn(false);
    when(mockAuthClient.generateSessionTokenForUser(anyString(), anyString(), any()))
        .thenReturn("test-token");

    // Mock empty group membership
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.GROUP_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);

    Result mockResult = mock(Result.class);
    when(mockResult.withSession(any(Map.class))).thenReturn(mockResult);
    when(mockResult.withCookies(any(play.mvc.Http.Cookie.class))).thenReturn(mockResult);

    // Execute the test
    callbackLogic.handleOidcSupportCallback(
        mockOperationContext, mockCallContext, realConfigs, mockResult);

    // Verify that manageUserRole was called (via systemEntityClient.ingestProposal
    // for RoleMembership)
    verify(mockSystemEntityClient, atLeastOnce())
        .getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.ROLE_MEMBERSHIP_ASPECT_NAME),
            eq(false));
    // Verify ingestProposal was called for both CorpUserInfo and RoleMembership
    verify(mockSystemEntityClient, atLeast(2)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testIsSupportUserFlagVerification_UserIsSupportUser() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.containsAttribute("sub")).thenReturn(true);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");

    // Mock ProfileManager
    ProfileManager mockProfileManager = mock(ProfileManager.class);
    when(mockProfileManager.isAuthenticated()).thenReturn(true);
    when(mockProfileManager.getProfile()).thenReturn(Optional.of(mockProfile));

    // Mock CallContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockCallContext.profileManagerFactory())
        .thenReturn((webContext, sessionStore) -> mockProfileManager);

    // Mock empty cookies
    when(mockWebContext.getRequestCookies()).thenReturn(List.of());

    // Mock SsoSupportManager
    auth.sso.SsoProvider mockSsoProvider = mock(auth.sso.SsoProvider.class);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoProvider.configs()).thenReturn(realConfigs);

    // Mock user exists
    CorpUserSnapshot existingUserSnapshot = new CorpUserSnapshot();
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");
    existingUserSnapshot.setUrn(userUrn);
    com.linkedin.metadata.aspect.CorpUserAspectArray aspects =
        new com.linkedin.metadata.aspect.CorpUserAspectArray();
    aspects.add(com.linkedin.metadata.aspect.CorpUserAspect.create(new CorpUserInfo()));
    existingUserSnapshot.setAspects(aspects);

    Entity existingUserEntity = new Entity();
    existingUserEntity.setValue(Snapshot.create(existingUserSnapshot));

    when(mockSystemEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(existingUserEntity);

    // Mock ensureUserSupportFlag
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);
    when(mockSystemEntityClient.ingestProposal(any(OperationContext.class), any()))
        .thenReturn("success");

    // Mock getV2 to return EntityResponse with isSupportUser=true
    CorpUserInfo userInfoWithSupportFlag = new CorpUserInfo();
    userInfoWithSupportFlag.setActive(true);
    userInfoWithSupportFlag.setIsSupportUser(true);

    com.linkedin.entity.EntityResponse entityResponse = new com.linkedin.entity.EntityResponse();
    entityResponse.setUrn(userUrn);
    com.linkedin.entity.EnvelopedAspectMap envelopedAspects =
        new com.linkedin.entity.EnvelopedAspectMap();
    com.linkedin.entity.EnvelopedAspect envelopedAspect = new com.linkedin.entity.EnvelopedAspect();
    // Create Aspect from RecordTemplate data
    Aspect aspect = new Aspect(userInfoWithSupportFlag.data());
    envelopedAspect.setValue(aspect);
    envelopedAspects.put(Constants.CORP_USER_INFO_ASPECT_NAME, envelopedAspect);
    entityResponse.setAspects(envelopedAspects);

    when(mockSystemEntityClient.getV2(
            any(OperationContext.class),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq(userUrn),
            eq(java.util.Collections.singleton(Constants.CORP_USER_INFO_ASPECT_NAME))))
        .thenReturn(entityResponse);

    // Mock other required calls
    when(mockCookieConfigs.getAuthCookieSameSite()).thenReturn("LAX");
    when(mockCookieConfigs.getAuthCookieSecure()).thenReturn(false);
    when(mockAuthClient.generateSessionTokenForUser(anyString(), anyString(), any()))
        .thenReturn("test-token");

    // Mock empty group membership
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.GROUP_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);

    Result mockResult = mock(Result.class);
    when(mockResult.withSession(any(Map.class))).thenReturn(mockResult);
    when(mockResult.withCookies(any(play.mvc.Http.Cookie.class))).thenReturn(mockResult);

    // Execute the test
    callbackLogic.handleOidcSupportCallback(
        mockOperationContext, mockCallContext, realConfigs, mockResult);

    // Verify getV2 was called to verify isSupportUser flag
    verify(mockSystemEntityClient, atLeastOnce())
        .getV2(
            any(OperationContext.class),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq(userUrn),
            eq(java.util.Collections.singleton(Constants.CORP_USER_INFO_ASPECT_NAME)));
  }

  @Test
  public void testIsSupportUserFlagVerification_UserIsNotSupportUser() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.containsAttribute("sub")).thenReturn(true);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");

    // Mock ProfileManager
    ProfileManager mockProfileManager = mock(ProfileManager.class);
    when(mockProfileManager.isAuthenticated()).thenReturn(true);
    when(mockProfileManager.getProfile()).thenReturn(Optional.of(mockProfile));

    // Mock CallContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockCallContext.profileManagerFactory())
        .thenReturn((webContext, sessionStore) -> mockProfileManager);

    // Mock empty cookies
    when(mockWebContext.getRequestCookies()).thenReturn(List.of());

    // Mock SsoSupportManager
    auth.sso.SsoProvider mockSsoProvider = mock(auth.sso.SsoProvider.class);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoProvider.configs()).thenReturn(realConfigs);

    // Mock user exists
    CorpUserSnapshot existingUserSnapshot = new CorpUserSnapshot();
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");
    existingUserSnapshot.setUrn(userUrn);
    com.linkedin.metadata.aspect.CorpUserAspectArray aspects =
        new com.linkedin.metadata.aspect.CorpUserAspectArray();
    aspects.add(com.linkedin.metadata.aspect.CorpUserAspect.create(new CorpUserInfo()));
    existingUserSnapshot.setAspects(aspects);

    Entity existingUserEntity = new Entity();
    existingUserEntity.setValue(Snapshot.create(existingUserSnapshot));

    when(mockSystemEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(existingUserEntity);

    // Mock ensureUserSupportFlag
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);
    when(mockSystemEntityClient.ingestProposal(any(OperationContext.class), any()))
        .thenReturn("success");

    // Mock getV2 to return EntityResponse with isSupportUser=false
    CorpUserInfo userInfoWithoutSupportFlag = new CorpUserInfo();
    userInfoWithoutSupportFlag.setActive(true);
    userInfoWithoutSupportFlag.setIsSupportUser(false);

    com.linkedin.entity.EntityResponse entityResponse = new com.linkedin.entity.EntityResponse();
    entityResponse.setUrn(userUrn);
    com.linkedin.entity.EnvelopedAspectMap envelopedAspects =
        new com.linkedin.entity.EnvelopedAspectMap();
    com.linkedin.entity.EnvelopedAspect envelopedAspect = new com.linkedin.entity.EnvelopedAspect();
    // Create Aspect from RecordTemplate data
    Aspect aspect = new Aspect(userInfoWithoutSupportFlag.data());
    envelopedAspect.setValue(aspect);
    envelopedAspects.put(Constants.CORP_USER_INFO_ASPECT_NAME, envelopedAspect);
    entityResponse.setAspects(envelopedAspects);

    when(mockSystemEntityClient.getV2(
            any(OperationContext.class),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq(userUrn),
            eq(java.util.Collections.singleton(Constants.CORP_USER_INFO_ASPECT_NAME))))
        .thenReturn(entityResponse);

    // Mock other required calls
    when(mockCookieConfigs.getAuthCookieSameSite()).thenReturn("LAX");
    when(mockCookieConfigs.getAuthCookieSecure()).thenReturn(false);
    when(mockAuthClient.generateSessionTokenForUser(anyString(), anyString(), any()))
        .thenReturn("test-token");

    // Mock empty group membership
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.GROUP_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);

    Result mockResult = mock(Result.class);
    when(mockResult.withSession(any(Map.class))).thenReturn(mockResult);
    when(mockResult.withCookies(any(play.mvc.Http.Cookie.class))).thenReturn(mockResult);

    // Execute the test - notification should not be sent since isSupportUser=false
    callbackLogic.handleOidcSupportCallback(
        mockOperationContext, mockCallContext, realConfigs, mockResult);

    // Verify getV2 was called to verify isSupportUser flag
    verify(mockSystemEntityClient, atLeastOnce())
        .getV2(
            any(OperationContext.class),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq(userUrn),
            eq(java.util.Collections.singleton(Constants.CORP_USER_INFO_ASPECT_NAME)));
  }

  @Test
  public void testIsSupportUserFlagVerification_ExceptionDefaultsToTrue() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.containsAttribute("sub")).thenReturn(true);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");

    // Mock ProfileManager
    ProfileManager mockProfileManager = mock(ProfileManager.class);
    when(mockProfileManager.isAuthenticated()).thenReturn(true);
    when(mockProfileManager.getProfile()).thenReturn(Optional.of(mockProfile));

    // Mock CallContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockCallContext.profileManagerFactory())
        .thenReturn((webContext, sessionStore) -> mockProfileManager);

    // Mock empty cookies
    when(mockWebContext.getRequestCookies()).thenReturn(List.of());

    // Mock SsoSupportManager
    auth.sso.SsoProvider mockSsoProvider = mock(auth.sso.SsoProvider.class);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoProvider.configs()).thenReturn(realConfigs);

    // Mock user exists
    CorpUserSnapshot existingUserSnapshot = new CorpUserSnapshot();
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");
    existingUserSnapshot.setUrn(userUrn);
    com.linkedin.metadata.aspect.CorpUserAspectArray aspects =
        new com.linkedin.metadata.aspect.CorpUserAspectArray();
    aspects.add(com.linkedin.metadata.aspect.CorpUserAspect.create(new CorpUserInfo()));
    existingUserSnapshot.setAspects(aspects);

    Entity existingUserEntity = new Entity();
    existingUserEntity.setValue(Snapshot.create(existingUserSnapshot));

    when(mockSystemEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(existingUserEntity);

    // Mock ensureUserSupportFlag
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);
    when(mockSystemEntityClient.ingestProposal(any(OperationContext.class), any()))
        .thenReturn("success");

    // Mock getV2 to throw exception - should default to isSupportUser=true
    when(mockSystemEntityClient.getV2(
            any(OperationContext.class),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq(userUrn),
            eq(java.util.Collections.singleton(Constants.CORP_USER_INFO_ASPECT_NAME))))
        .thenThrow(new RuntimeException("Database error"));

    // Mock other required calls
    when(mockCookieConfigs.getAuthCookieSameSite()).thenReturn("LAX");
    when(mockCookieConfigs.getAuthCookieSecure()).thenReturn(false);
    when(mockAuthClient.generateSessionTokenForUser(anyString(), anyString(), any()))
        .thenReturn("test-token");

    // Mock empty group membership
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.GROUP_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);

    Result mockResult = mock(Result.class);
    when(mockResult.withSession(any(Map.class))).thenReturn(mockResult);
    when(mockResult.withCookies(any(play.mvc.Http.Cookie.class))).thenReturn(mockResult);

    // Execute the test - exception should default to isSupportUser=true, so notification should be
    // attempted
    callbackLogic.handleOidcSupportCallback(
        mockOperationContext, mockCallContext, realConfigs, mockResult);

    // Verify getV2 was called and exception was handled
    verify(mockSystemEntityClient, atLeastOnce())
        .getV2(
            any(OperationContext.class),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq(userUrn),
            eq(java.util.Collections.singleton(Constants.CORP_USER_INFO_ASPECT_NAME)));
  }

  @Test
  public void testTicketIdCookieLogging_WithTicketId() throws Exception {
    // Mock CommonProfile
    CommonProfile mockProfile = mock(CommonProfile.class);
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "testuser@example.com");
    when(mockProfile.getAttributes()).thenReturn(attributes);
    when(mockProfile.containsAttribute("sub")).thenReturn(true);
    when(mockProfile.getAttribute("sub")).thenReturn("testuser@example.com");

    // Mock ProfileManager
    ProfileManager mockProfileManager = mock(ProfileManager.class);
    when(mockProfileManager.isAuthenticated()).thenReturn(true);
    when(mockProfileManager.getProfile()).thenReturn(Optional.of(mockProfile));

    // Mock CallContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockCallContext.profileManagerFactory())
        .thenReturn((webContext, sessionStore) -> mockProfileManager);

    // Mock ticket ID cookie
    String ticketId = "TEST-12345";
    String encodedTicketId =
        java.net.URLEncoder.encode(ticketId, java.nio.charset.StandardCharsets.UTF_8);
    Cookie mockTicketCookie = mock(Cookie.class);
    when(mockTicketCookie.getName()).thenReturn(AuthUtils.SUPPORT_TICKET_ID_COOKIE_NAME);
    when(mockTicketCookie.getValue()).thenReturn(encodedTicketId);
    when(mockWebContext.getRequestCookies()).thenReturn(List.of(mockTicketCookie));

    // Mock SsoSupportManager
    auth.sso.SsoProvider mockSsoProvider = mock(auth.sso.SsoProvider.class);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoProvider.configs()).thenReturn(realConfigs);

    // Mock user exists
    CorpUserSnapshot existingUserSnapshot = new CorpUserSnapshot();
    CorpuserUrn userUrn = new CorpuserUrn("testuser@example.com");
    existingUserSnapshot.setUrn(userUrn);
    com.linkedin.metadata.aspect.CorpUserAspectArray aspects =
        new com.linkedin.metadata.aspect.CorpUserAspectArray();
    aspects.add(com.linkedin.metadata.aspect.CorpUserAspect.create(new CorpUserInfo()));
    existingUserSnapshot.setAspects(aspects);

    Entity existingUserEntity = new Entity();
    existingUserEntity.setValue(Snapshot.create(existingUserSnapshot));

    when(mockSystemEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(existingUserEntity);

    // Mock ensureUserSupportFlag
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);
    when(mockSystemEntityClient.ingestProposal(any(OperationContext.class), any()))
        .thenReturn("success");

    // Mock getV2 to return EntityResponse with isSupportUser=true
    CorpUserInfo userInfoWithSupportFlag = new CorpUserInfo();
    userInfoWithSupportFlag.setActive(true);
    userInfoWithSupportFlag.setIsSupportUser(true);

    com.linkedin.entity.EntityResponse entityResponse = new com.linkedin.entity.EntityResponse();
    entityResponse.setUrn(userUrn);
    com.linkedin.entity.EnvelopedAspectMap envelopedAspects =
        new com.linkedin.entity.EnvelopedAspectMap();
    com.linkedin.entity.EnvelopedAspect envelopedAspect = new com.linkedin.entity.EnvelopedAspect();
    // Create Aspect from RecordTemplate data
    Aspect aspect = new Aspect(userInfoWithSupportFlag.data());
    envelopedAspect.setValue(aspect);
    envelopedAspects.put(Constants.CORP_USER_INFO_ASPECT_NAME, envelopedAspect);
    entityResponse.setAspects(envelopedAspects);

    when(mockSystemEntityClient.getV2(
            any(OperationContext.class),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq(userUrn),
            eq(java.util.Collections.singleton(Constants.CORP_USER_INFO_ASPECT_NAME))))
        .thenReturn(entityResponse);

    // Mock other required calls
    when(mockCookieConfigs.getAuthCookieSameSite()).thenReturn("LAX");
    when(mockCookieConfigs.getAuthCookieSecure()).thenReturn(false);
    when(mockAuthClient.generateSessionTokenForUser(anyString(), anyString(), any()))
        .thenReturn("test-token");

    // Mock empty group membership
    when(mockSystemEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.GROUP_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);

    Result mockResult = mock(Result.class);
    when(mockResult.withSession(any(Map.class))).thenReturn(mockResult);
    when(mockResult.withCookies(any(play.mvc.Http.Cookie.class))).thenReturn(mockResult);

    // Execute the test - ticket ID should be read and logged
    callbackLogic.handleOidcSupportCallback(
        mockOperationContext, mockCallContext, realConfigs, mockResult);

    // Verify ticket ID cookie was read
    verify(mockWebContext, atLeastOnce()).getRequestCookies();
  }

  @Test
  public void testSendSupportLoginNotification_WithTicketIdAndSourceIP() throws Exception {
    // Use reflection to access private method for testing
    java.lang.reflect.Method method =
        OidcSupportCallbackLogic.class.getDeclaredMethod(
            "sendSupportLoginNotification",
            String.class,
            String.class,
            java.util.Optional.class,
            CallContext.class);
    method.setAccessible(true);

    // Mock CallContext and WebContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);

    // Mock X-Forwarded-For header
    when(mockWebContext.getRequestHeader("X-Forwarded-For"))
        .thenReturn(Optional.of("192.168.1.1, 10.0.0.1"));

    // Mock HttpClient - we need to mock the async call
    // Since the method uses CompletableFuture.runAsync, we can't easily test the HTTP call
    // But we can verify the method doesn't throw exceptions

    // Execute the test
    assertDoesNotThrow(
        () -> {
          method.invoke(
              callbackLogic,
              "urn:li:corpuser:testuser@example.com",
              "Test User",
              Optional.of("TEST-12345"),
              mockCallContext);
        });

    // Give async operation time to complete (basic check)
    Thread.sleep(100);
  }

  @Test
  public void testSendSupportLoginNotification_WithRemoteAddr() throws Exception {
    // Use reflection to access private method for testing
    java.lang.reflect.Method method =
        OidcSupportCallbackLogic.class.getDeclaredMethod(
            "sendSupportLoginNotification",
            String.class,
            String.class,
            java.util.Optional.class,
            CallContext.class);
    method.setAccessible(true);

    // Mock CallContext and WebContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);

    // Mock no X-Forwarded-For, but remoteAddr available
    when(mockWebContext.getRequestHeader("X-Forwarded-For")).thenReturn(Optional.empty());
    when(mockWebContext.getRemoteAddr()).thenReturn("192.168.1.100");

    // Execute the test
    assertDoesNotThrow(
        () -> {
          method.invoke(
              callbackLogic,
              "urn:li:corpuser:testuser@example.com",
              "Test User",
              Optional.empty(),
              mockCallContext);
        });

    // Give async operation time to complete
    Thread.sleep(100);
  }

  @Test
  public void testSendSupportLoginNotification_NoSourceIP() throws Exception {
    // Use reflection to access private method for testing
    java.lang.reflect.Method method =
        OidcSupportCallbackLogic.class.getDeclaredMethod(
            "sendSupportLoginNotification",
            String.class,
            String.class,
            java.util.Optional.class,
            CallContext.class);
    method.setAccessible(true);

    // Mock CallContext and WebContext
    CallContext mockCallContext = mock(CallContext.class);
    WebContext mockWebContext = mock(WebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);

    // Mock no headers and no remoteAddr
    when(mockWebContext.getRequestHeader("X-Forwarded-For")).thenReturn(Optional.empty());
    when(mockWebContext.getRemoteAddr()).thenReturn(null);

    // Execute the test - should handle missing IP gracefully
    assertDoesNotThrow(
        () -> {
          method.invoke(
              callbackLogic,
              "urn:li:corpuser:testuser@example.com",
              "Test User",
              Optional.of("TEST-12345"),
              mockCallContext);
        });

    // Give async operation time to complete
    Thread.sleep(100);
  }

  @Test
  public void testSendSupportLoginNotification_ExceptionHandling() throws Exception {
    // Use reflection to access private method for testing
    java.lang.reflect.Method method =
        OidcSupportCallbackLogic.class.getDeclaredMethod(
            "sendSupportLoginNotification",
            String.class,
            String.class,
            java.util.Optional.class,
            CallContext.class);
    method.setAccessible(true);

    // Mock CallContext to throw exception when getting webContext
    CallContext mockCallContext = mock(CallContext.class);
    when(mockCallContext.webContext()).thenThrow(new RuntimeException("Context error"));

    // Execute the test - should handle exception gracefully
    assertDoesNotThrow(
        () -> {
          method.invoke(
              callbackLogic,
              "urn:li:corpuser:testuser@example.com",
              "Test User",
              Optional.empty(),
              mockCallContext);
        });

    // Give async operation time to complete
    Thread.sleep(100);
  }
}
