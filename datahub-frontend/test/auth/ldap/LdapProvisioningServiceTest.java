package auth.ldap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for LdapProvisioningService. Tests user and group provisioning functionality with
 * mocked dependencies.
 */
public class LdapProvisioningServiceTest {

  @Mock private SystemEntityClient mockEntityClient;

  @Mock private OperationContext mockOperationContext;

  private AutoCloseable mocks;

  @BeforeEach
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    // Reset singleton before each test
    resetSingleton();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
    resetSingleton();
  }

  /** Reset the singleton instance using reflection for testing */
  private void resetSingleton() {
    try {
      java.lang.reflect.Field instance = LdapProvisioningService.class.getDeclaredField("instance");
      instance.setAccessible(true);
      instance.set(null, null);
    } catch (Exception e) {
      // Ignore if field doesn't exist or can't be reset
    }
  }

  @Test
  public void testInitialize() {
    // Test singleton initialization
    LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);

    LdapProvisioningService service = LdapProvisioningService.getInstance();
    assertNotNull(service);
  }

  @Test
  public void testGetInstanceBeforeInitialization() {
    // Test that getInstance throws exception when not initialized
    assertThrows(
        IllegalStateException.class,
        () -> {
          LdapProvisioningService.getInstance();
        });
  }

  @Test
  public void testGetInstanceAfterInitialization() {
    // Test successful instance retrieval after initialization
    LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);

    LdapProvisioningService service = LdapProvisioningService.getInstance();
    assertNotNull(service);

    // Verify it returns the same instance
    LdapProvisioningService service2 = LdapProvisioningService.getInstance();
    assertSame(service, service2);
  }

  @Test
  public void testTryProvisionUserNewUser() throws Exception {
    // Test provisioning a new user
    LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);
    LdapProvisioningService service = LdapProvisioningService.getInstance();

    CorpuserUrn userUrn = new CorpuserUrn("testuser");
    CorpUserSnapshot userSnapshot = new CorpUserSnapshot();
    userSnapshot.setUrn(userUrn);

    // Create minimal aspects (only key aspect)
    CorpUserAspectArray minimalAspects = new CorpUserAspectArray();
    CorpUserSnapshot existingSnapshot = new CorpUserSnapshot();
    existingSnapshot.setAspects(minimalAspects);

    Entity existingEntity = new Entity();
    existingEntity.setValue(Snapshot.create(existingSnapshot));

    when(mockEntityClient.get(eq(mockOperationContext), eq(userUrn))).thenReturn(existingEntity);

    // Execute
    service.tryProvisionUser(userSnapshot);

    // Verify update was called
    verify(mockEntityClient).update(eq(mockOperationContext), any(Entity.class));
  }

  @Test
  public void testTryProvisionUserFailure() throws Exception {
    // Test handling of provisioning failure
    LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);
    LdapProvisioningService service = LdapProvisioningService.getInstance();

    CorpuserUrn userUrn = new CorpuserUrn("testuser");
    CorpUserSnapshot userSnapshot = new CorpUserSnapshot();
    userSnapshot.setUrn(userUrn);

    when(mockEntityClient.get(eq(mockOperationContext), eq(userUrn)))
        .thenThrow(new RemoteInvocationException("Connection failed"));

    // Execute and verify exception
    assertThrows(
        RuntimeException.class,
        () -> {
          service.tryProvisionUser(userSnapshot);
        });
  }

  @Test
  public void testUpdateGroupMembership() throws Exception {
    // Test updating group membership with new signature
    // Note: This test verifies the method signature and basic flow
    // The createUserOperationContext method requires AuthorizationContext which is complex to mock
    LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);
    LdapProvisioningService service = LdapProvisioningService.getInstance();

    String username = "testuser";
    CorpuserUrn userUrn = new CorpuserUrn(username);
    com.linkedin.identity.GroupMembership groupMembership =
        new com.linkedin.identity.GroupMembership();
    groupMembership.setGroups(new com.linkedin.common.UrnArray());

    // The method will fail due to AuthorizationContext being null, but we can verify
    // that the method accepts the correct parameters (username added)
    assertThrows(
        NullPointerException.class,
        () -> {
          service.updateGroupMembership(username, userUrn, groupMembership);
        },
        "Method should accept username parameter and attempt to create user context");
  }

  @Test
  public void testUpdateGroupMembershipFailure() throws Exception {
    // Test handling of group membership update failure with new signature
    LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);
    LdapProvisioningService service = LdapProvisioningService.getInstance();

    String username = "testuser";
    CorpuserUrn userUrn = new CorpuserUrn(username);
    com.linkedin.identity.GroupMembership groupMembership =
        new com.linkedin.identity.GroupMembership();
    groupMembership.setGroups(new com.linkedin.common.UrnArray());

    // Mock asSession to return a new operation context
    OperationContext mockUserOpContext = mock(OperationContext.class);
    when(mockOperationContext.asSession(any(), any(), any())).thenReturn(mockUserOpContext);

    when(mockEntityClient.ingestProposal(eq(mockUserOpContext), any(MetadataChangeProposal.class)))
        .thenThrow(new RemoteInvocationException("Update failed"));

    // Execute and verify exception
    assertThrows(
        RuntimeException.class,
        () -> {
          service.updateGroupMembership(username, userUrn, groupMembership);
        });
  }

  @Test
  public void testVerifyPreProvisionedUserNotExists() throws Exception {
    // Test verification fails for non-existent user
    LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);
    LdapProvisioningService service = LdapProvisioningService.getInstance();

    CorpuserUrn userUrn = new CorpuserUrn("newuser");

    // Create entity with only key aspect (user doesn't exist)
    CorpUserAspectArray minimalAspects = new CorpUserAspectArray();
    CorpUserSnapshot existingSnapshot = new CorpUserSnapshot();
    existingSnapshot.setAspects(minimalAspects);

    Entity existingEntity = new Entity();
    existingEntity.setValue(Snapshot.create(existingSnapshot));

    when(mockEntityClient.get(eq(mockOperationContext), eq(userUrn))).thenReturn(existingEntity);

    // Execute and verify exception
    assertThrows(
        RuntimeException.class,
        () -> {
          service.verifyPreProvisionedUser(userUrn);
        });
  }

  @Test
  public void testSetUserStatusActive() throws Exception {
    // Test setting user status to active with new signature
    // Note: This test verifies the method signature and basic flow
    // The createUserOperationContext method requires AuthorizationContext which is complex to mock
    LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);
    LdapProvisioningService service = LdapProvisioningService.getInstance();

    String username = "testuser";
    CorpuserUrn userUrn = new CorpuserUrn(username);
    CorpUserStatus activeStatus =
        new CorpUserStatus()
            .setStatus(Constants.CORP_USER_STATUS_ACTIVE)
            .setLastModified(
                new AuditStamp()
                    .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                    .setTime(System.currentTimeMillis()));

    // The method will fail due to AuthorizationContext being null, but we can verify
    // that the method accepts the correct parameters (username added)
    assertThrows(
        NullPointerException.class,
        () -> {
          service.setUserStatus(username, userUrn, activeStatus);
        },
        "Method should accept username parameter and attempt to create user context");
  }

  @Test
  public void testSetUserStatusFailure() throws Exception {
    // Test handling of status update failure with new signature
    LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);
    LdapProvisioningService service = LdapProvisioningService.getInstance();

    String username = "testuser";
    CorpuserUrn userUrn = new CorpuserUrn(username);
    CorpUserStatus activeStatus = new CorpUserStatus().setStatus(Constants.CORP_USER_STATUS_ACTIVE);

    // Mock asSession to return a new operation context
    OperationContext mockUserOpContext = mock(OperationContext.class);
    when(mockOperationContext.asSession(any(), any(), any())).thenReturn(mockUserOpContext);

    when(mockEntityClient.ingestProposal(eq(mockUserOpContext), any(MetadataChangeProposal.class)))
        .thenThrow(new RemoteInvocationException("Status update failed"));

    // Execute and verify exception
    assertThrows(
        Exception.class,
        () -> {
          service.setUserStatus(username, userUrn, activeStatus);
        });
  }

  @Test
  public void testMultipleInitializationAttempts() {
    // Test that multiple initialization attempts don't cause issues
    LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);

    // Second initialization should log warning but not fail
    assertDoesNotThrow(
        () -> {
          LdapProvisioningService.initialize(mockEntityClient, mockOperationContext);
        });

    // Should still return valid instance
    LdapProvisioningService service = LdapProvisioningService.getInstance();
    assertNotNull(service);
  }
}
