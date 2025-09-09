package auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserInfo;
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
}
