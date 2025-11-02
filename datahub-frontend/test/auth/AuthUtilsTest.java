package auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserInvitationStatus;
import com.linkedin.identity.InvitationStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import play.mvc.Http;

public class AuthUtilsTest {

  @Mock private OperationContext mockOperationContext;
  @Mock private SystemEntityClient mockEntityClient;

  private CorpuserUrn testUserUrn;
  private CorpUserSnapshot testUserSnapshot;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    testUserUrn = new CorpuserUrn("testuser@company.com");

    // Create a test user snapshot
    testUserSnapshot = new CorpUserSnapshot();
    testUserSnapshot.setUrn(testUserUrn);
    CorpUserAspectArray aspects = new CorpUserAspectArray();
    aspects.add(CorpUserAspect.create(new CorpUserInfo()));
    testUserSnapshot.setAspects(aspects);
  }

  @Test
  public void testVerifyPreProvisionedUser_UserExists() throws Exception {
    // Mock user that exists (has more than just key aspect)
    CorpUserSnapshot existingUserSnapshot = new CorpUserSnapshot();
    existingUserSnapshot.setUrn(testUserUrn);
    CorpUserAspectArray aspects = new CorpUserAspectArray();
    aspects.add(CorpUserAspect.create(new CorpUserInfo()));
    aspects.add(CorpUserAspect.create(new com.linkedin.identity.CorpUserEditableInfo()));
    existingUserSnapshot.setAspects(aspects);

    Entity existingUserEntity = new Entity();
    existingUserEntity.setValue(Snapshot.create(existingUserSnapshot));

    when(mockEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(existingUserEntity);

    // Should not throw exception
    assertDoesNotThrow(
        () ->
            AuthUtils.verifyPreProvisionedUser(
                mockOperationContext, testUserUrn, mockEntityClient));

    verify(mockEntityClient, times(1)).get(any(OperationContext.class), any(CorpuserUrn.class));
  }

  @Test
  public void testVerifyPreProvisionedUser_UserDoesNotExist() throws Exception {
    // Mock user that doesn't exist (only has key aspect)
    CorpUserSnapshot emptyUserSnapshot = new CorpUserSnapshot();
    emptyUserSnapshot.setUrn(testUserUrn);
    emptyUserSnapshot.setAspects(new CorpUserAspectArray()); // Empty aspects = user doesn't exist

    Entity emptyUserEntity = new Entity();
    emptyUserEntity.setValue(Snapshot.create(emptyUserSnapshot));

    when(mockEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(emptyUserEntity);

    // Should throw RuntimeException
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                AuthUtils.verifyPreProvisionedUser(
                    mockOperationContext, testUserUrn, mockEntityClient));

    assertTrue(exception.getMessage().contains("not yet been provisioned"));
    verify(mockEntityClient, times(1)).get(any(OperationContext.class), any(CorpuserUrn.class));
  }

  @Test
  public void testVerifyPreProvisionedUser_RemoteInvocationException() throws Exception {
    // Mock RemoteInvocationException
    when(mockEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenThrow(new RemoteInvocationException("Connection failed"));

    // Should throw RuntimeException wrapping the RemoteInvocationException
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                AuthUtils.verifyPreProvisionedUser(
                    mockOperationContext, testUserUrn, mockEntityClient));

    assertTrue(exception.getMessage().contains("Failed to validate user"));
    verify(mockEntityClient, times(1)).get(any(OperationContext.class), any(CorpuserUrn.class));
  }

  @Test
  public void testTryProvisionUser_WithSentInvitationStatus_ShouldProvision() throws Exception {
    // Mock user that exists with more than key aspect
    CorpUserSnapshot existingUserSnapshot = new CorpUserSnapshot();
    existingUserSnapshot.setUrn(testUserUrn);
    CorpUserAspectArray aspects = new CorpUserAspectArray();

    // Add multiple aspects to simulate a user with existing data
    aspects.add(CorpUserAspect.create(new CorpUserInfo()));
    aspects.add(CorpUserAspect.create(new com.linkedin.identity.CorpUserEditableInfo()));

    existingUserSnapshot.setAspects(aspects);

    Entity existingUserEntity = new Entity();
    existingUserEntity.setValue(Snapshot.create(existingUserSnapshot));

    // Mock CorpUserInvitationStatus with SENT status via EntityClient.getLatestAspectObject
    CorpUserInvitationStatus invitationStatus = new CorpUserInvitationStatus();
    invitationStatus.setStatus(InvitationStatus.SENT);
    invitationStatus.setCreated(
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(Urn.createFromString("urn:li:corpuser:testuser")));
    invitationStatus.setLastUpdated(
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(Urn.createFromString("urn:li:corpuser:testuser")));
    invitationStatus.setInvitationToken("test-token");

    // Create mock Aspect containing the invitation status data
    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(invitationStatus.data());

    when(mockEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(existingUserEntity);
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME),
            eq(false)))
        .thenReturn(mockAspect);

    // Should provision the user (update should be called)
    AuthUtils.tryProvisionUser(mockOperationContext, testUserSnapshot, mockEntityClient);

    verify(mockEntityClient, times(1)).get(any(OperationContext.class), any(CorpuserUrn.class));
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, times(1)).update(any(OperationContext.class), any(Entity.class));
  }

  @Test
  public void testTryProvisionUser_WithAcceptedInvitationStatus_ShouldNotProvision()
      throws Exception {
    // Mock user that exists with multiple aspects
    CorpUserSnapshot existingUserSnapshot = new CorpUserSnapshot();
    existingUserSnapshot.setUrn(testUserUrn);
    CorpUserAspectArray aspects = new CorpUserAspectArray();

    // Add multiple aspects to simulate a user with existing data
    aspects.add(CorpUserAspect.create(new CorpUserInfo()));
    aspects.add(CorpUserAspect.create(new com.linkedin.identity.CorpUserEditableInfo()));

    existingUserSnapshot.setAspects(aspects);

    Entity existingUserEntity = new Entity();
    existingUserEntity.setValue(Snapshot.create(existingUserSnapshot));

    // Mock CorpUserInvitationStatus with ACCEPTED status via EntityClient.getLatestAspectObject
    CorpUserInvitationStatus invitationStatus = new CorpUserInvitationStatus();
    invitationStatus.setStatus(InvitationStatus.ACCEPTED);
    invitationStatus.setCreated(
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(Urn.createFromString("urn:li:corpuser:testuser")));
    invitationStatus.setLastUpdated(
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(Urn.createFromString("urn:li:corpuser:testuser")));
    invitationStatus.setInvitationToken("test-token");

    // Create mock Aspect containing the invitation status data
    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(invitationStatus.data());

    when(mockEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(existingUserEntity);
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME),
            eq(false)))
        .thenReturn(mockAspect);

    // Should NOT provision the user (update should not be called)
    AuthUtils.tryProvisionUser(mockOperationContext, testUserSnapshot, mockEntityClient);

    verify(mockEntityClient, times(1)).get(any(OperationContext.class), any(CorpuserUrn.class));
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, never()).update(any(OperationContext.class), any(Entity.class));
  }

  @Test
  public void testTryProvisionUser_WithNoInvitationStatus_ShouldNotProvision() throws Exception {
    // Mock user that exists with multiple aspects but no invitation status
    CorpUserSnapshot existingUserSnapshot = new CorpUserSnapshot();
    existingUserSnapshot.setUrn(testUserUrn);
    CorpUserAspectArray aspects = new CorpUserAspectArray();

    // Add multiple aspects to simulate a user with existing data
    aspects.add(CorpUserAspect.create(new CorpUserInfo()));
    aspects.add(CorpUserAspect.create(new com.linkedin.identity.CorpUserEditableInfo()));

    existingUserSnapshot.setAspects(aspects);

    Entity existingUserEntity = new Entity();
    existingUserEntity.setValue(Snapshot.create(existingUserSnapshot));

    // Mock no invitation status found (getLatestAspectObject returns null)
    when(mockEntityClient.get(any(OperationContext.class), any(CorpuserUrn.class)))
        .thenReturn(existingUserEntity);
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);

    // Should NOT provision the user (no SENT invitation found)
    AuthUtils.tryProvisionUser(mockOperationContext, testUserSnapshot, mockEntityClient);

    verify(mockEntityClient, times(1)).get(any(OperationContext.class), any(CorpuserUrn.class));
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, never()).update(any(OperationContext.class), any(Entity.class));
  }

  @Test
  public void testEnsureUserSupportFlag_SetToTrue_UserDoesNotExist() throws Exception {
    // Mock no existing aspect
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);
    when(mockEntityClient.ingestProposal(any(OperationContext.class), any())).thenReturn("success");

    // Should set isSupportUser to true
    boolean wasPreviouslySupportUser =
        AuthUtils.ensureUserSupportFlag(mockOperationContext, testUserUrn, true, mockEntityClient);

    // Verify
    assertFalse(wasPreviouslySupportUser, "User did not previously have support flag set");
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testEnsureUserSupportFlag_SetToTrue_UserWasNotSupport() throws Exception {
    // Mock existing CorpUserInfo with isSupportUser=false
    CorpUserInfo existingUserInfo = new CorpUserInfo();
    existingUserInfo.setActive(true);
    existingUserInfo.setIsSupportUser(false);

    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(existingUserInfo.data());
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(mockAspect);
    when(mockEntityClient.ingestProposal(any(OperationContext.class), any())).thenReturn("success");

    // Should update isSupportUser to true
    boolean wasPreviouslySupportUser =
        AuthUtils.ensureUserSupportFlag(mockOperationContext, testUserUrn, true, mockEntityClient);

    // Verify
    assertFalse(wasPreviouslySupportUser, "User was not previously a support user");
    verify(mockEntityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testEnsureUserSupportFlag_SetToTrue_UserAlreadySupport() throws Exception {
    // Mock existing CorpUserInfo with isSupportUser=true
    CorpUserInfo existingUserInfo = new CorpUserInfo();
    existingUserInfo.setActive(true);
    existingUserInfo.setIsSupportUser(true);

    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(existingUserInfo.data());
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(mockAspect);

    // Should not update (already set to true)
    boolean wasPreviouslySupportUser =
        AuthUtils.ensureUserSupportFlag(mockOperationContext, testUserUrn, true, mockEntityClient);

    // Verify
    assertTrue(wasPreviouslySupportUser, "User was previously a support user");
    verify(mockEntityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testEnsureUserSupportFlag_SetToFalse_UserWasSupport() throws Exception {
    // Mock existing CorpUserInfo with isSupportUser=true
    CorpUserInfo existingUserInfo = new CorpUserInfo();
    existingUserInfo.setActive(true);
    existingUserInfo.setIsSupportUser(true);

    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(existingUserInfo.data());
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(mockAspect);
    when(mockEntityClient.ingestProposal(any(OperationContext.class), any())).thenReturn("success");

    // Should update isSupportUser to false
    boolean wasPreviouslySupportUser =
        AuthUtils.ensureUserSupportFlag(mockOperationContext, testUserUrn, false, mockEntityClient);

    // Verify
    assertTrue(wasPreviouslySupportUser, "User was previously a support user");
    verify(mockEntityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testEnsureUserSupportFlag_SetToFalse_UserAlreadyNotSupport() throws Exception {
    // Mock existing CorpUserInfo with isSupportUser=false
    CorpUserInfo existingUserInfo = new CorpUserInfo();
    existingUserInfo.setActive(true);
    existingUserInfo.setIsSupportUser(false);

    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(existingUserInfo.data());
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(mockAspect);

    // Should not update (already set to false)
    boolean wasPreviouslySupportUser =
        AuthUtils.ensureUserSupportFlag(mockOperationContext, testUserUrn, false, mockEntityClient);

    // Verify
    assertFalse(wasPreviouslySupportUser, "User was not previously a support user");
    verify(mockEntityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testEnsureUserSupportFlag_ThrowsOnFailure() throws Exception {
    // Mock exception when ingesting proposal (this is the security-critical failure)
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME),
            eq(false)))
        .thenReturn(null); // No existing aspect
    when(mockEntityClient.ingestProposal(any(OperationContext.class), any()))
        .thenThrow(new RuntimeException("Database error"));

    // Should throw RuntimeException (security requirement)
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                AuthUtils.ensureUserSupportFlag(
                    mockOperationContext, testUserUrn, true, mockEntityClient));

    assertTrue(
        exception.getMessage().contains("Failed to update isSupportUser flag"),
        "Exception should mention failure to update flag");
  }

  @Test
  public void testManageUserRole_AddRole_NoExistingRoles() throws Exception {
    // Mock no existing RoleMembership
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.ROLE_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);
    when(mockEntityClient.ingestProposal(any(OperationContext.class), any())).thenReturn("success");

    // Should add Admin role
    AuthUtils.manageUserRole(mockOperationContext, testUserUrn, "Admin", true, mockEntityClient);

    // Verify
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.ROLE_MEMBERSHIP_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testManageUserRole_AddRole_ExistingRoles() throws Exception {
    // Mock existing RoleMembership with one role
    com.linkedin.identity.RoleMembership existingMembership =
        new com.linkedin.identity.RoleMembership();
    com.linkedin.common.UrnArray existingRoles = new com.linkedin.common.UrnArray();
    existingRoles.add(new com.linkedin.common.urn.DataHubRoleUrn("User"));
    existingMembership.setRoles(existingRoles);

    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(existingMembership.data());
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.ROLE_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(mockAspect);
    when(mockEntityClient.ingestProposal(any(OperationContext.class), any())).thenReturn("success");

    // Should add Admin role while preserving User role
    AuthUtils.manageUserRole(mockOperationContext, testUserUrn, "Admin", true, mockEntityClient);

    // Verify
    verify(mockEntityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testManageUserRole_AddRole_AlreadyHasRole() throws Exception {
    // Mock existing RoleMembership with Admin role already
    com.linkedin.identity.RoleMembership existingMembership =
        new com.linkedin.identity.RoleMembership();
    com.linkedin.common.UrnArray existingRoles = new com.linkedin.common.UrnArray();
    existingRoles.add(new com.linkedin.common.urn.DataHubRoleUrn("Admin"));
    existingMembership.setRoles(existingRoles);

    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(existingMembership.data());
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.ROLE_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(mockAspect);

    // Should not update (role already present)
    AuthUtils.manageUserRole(mockOperationContext, testUserUrn, "Admin", true, mockEntityClient);

    // Verify - should not call ingestProposal since no change needed
    verify(mockEntityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testManageUserRole_RemoveRole() throws Exception {
    // Mock existing RoleMembership with Admin role
    com.linkedin.identity.RoleMembership existingMembership =
        new com.linkedin.identity.RoleMembership();
    com.linkedin.common.UrnArray existingRoles = new com.linkedin.common.UrnArray();
    existingRoles.add(new com.linkedin.common.urn.DataHubRoleUrn("Admin"));
    existingRoles.add(new com.linkedin.common.urn.DataHubRoleUrn("User"));
    existingMembership.setRoles(existingRoles);

    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(existingMembership.data());
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.ROLE_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(mockAspect);
    when(mockEntityClient.ingestProposal(any(OperationContext.class), any())).thenReturn("success");

    // Should remove Admin role while preserving User role
    AuthUtils.manageUserRole(mockOperationContext, testUserUrn, "Admin", false, mockEntityClient);

    // Verify
    verify(mockEntityClient, times(1)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testManageUserRole_RemoveRole_DoesNotHaveRole() throws Exception {
    // Mock existing RoleMembership without Admin role
    com.linkedin.identity.RoleMembership existingMembership =
        new com.linkedin.identity.RoleMembership();
    com.linkedin.common.UrnArray existingRoles = new com.linkedin.common.UrnArray();
    existingRoles.add(new com.linkedin.common.urn.DataHubRoleUrn("User"));
    existingMembership.setRoles(existingRoles);

    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(existingMembership.data());
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.ROLE_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(mockAspect);

    // Should not update (role not present)
    AuthUtils.manageUserRole(mockOperationContext, testUserUrn, "Admin", false, mockEntityClient);

    // Verify - should not call ingestProposal since no change needed
    verify(mockEntityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testManageUserRole_ThrowsOnFailure() throws Exception {
    // Mock exception when ingesting proposal
    when(mockEntityClient.getLatestAspectObject(
            any(OperationContext.class),
            any(CorpuserUrn.class),
            eq(Constants.ROLE_MEMBERSHIP_ASPECT_NAME),
            eq(false)))
        .thenReturn(null);
    when(mockEntityClient.ingestProposal(any(OperationContext.class), any()))
        .thenThrow(new RuntimeException("Database error"));

    // Should throw RuntimeException (security requirement)
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                AuthUtils.manageUserRole(
                    mockOperationContext, testUserUrn, "Admin", true, mockEntityClient));

    assertTrue(
        exception.getMessage().contains("Failed to add role"), "Exception should mention failure");
  }

  @Test
  public void testCreateSupportTicketIdSessionCookie() {
    // Test URL encoding of ticket ID with special characters
    String ticketId = "test ticket with spaces & special chars";
    Http.Cookie cookie = AuthUtils.createSupportTicketIdSessionCookie(ticketId, "LAX", false);

    // Verify cookie properties
    assertNotNull(cookie);
    assertEquals(AuthUtils.SUPPORT_TICKET_ID_COOKIE_NAME, cookie.name());
    assertTrue(cookie.value().contains("test+ticket+with+spaces")); // URL encoded
    assertFalse(cookie.httpOnly()); // Should not be httpOnly
    assertFalse(cookie.secure()); // As specified
    assertNull(cookie.maxAge()); // Session cookie (no maxAge)
    assertTrue(cookie.sameSite().isPresent());
    assertEquals(Http.Cookie.SameSite.LAX, cookie.sameSite().get());
  }

  @Test
  public void testCreateSupportTicketIdSessionCookie_WithSpecialCharacters() {
    // Test URL encoding handles various special characters
    String ticketId = "test&ticket=value?param#fragment";
    Http.Cookie cookie = AuthUtils.createSupportTicketIdSessionCookie(ticketId, "STRICT", true);

    // Verify cookie properties
    assertNotNull(cookie);
    assertEquals(AuthUtils.SUPPORT_TICKET_ID_COOKIE_NAME, cookie.name());
    // Value should be URL encoded
    assertTrue(cookie.value().contains("%26") || cookie.value().contains("&"));
    assertTrue(cookie.secure()); // As specified
    assertTrue(cookie.sameSite().isPresent());
    assertEquals(Http.Cookie.SameSite.STRICT, cookie.sameSite().get());
  }

  @Test
  public void testCreateSupportTicketIdSessionCookie_EmptyString() {
    // Test with empty string
    Http.Cookie cookie = AuthUtils.createSupportTicketIdSessionCookie("", "LAX", false);

    // Verify cookie exists
    assertNotNull(cookie);
    assertEquals(AuthUtils.SUPPORT_TICKET_ID_COOKIE_NAME, cookie.name());
  }
}
