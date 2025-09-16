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
}
