package auth.ldap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import security.AuthenticationManager;

public class LdapProvisioningLogicTest {

  @Mock private SystemEntityClient mockSystemEntityClient;

  @Mock private OperationContext mockSystemOperationContext;

  private LdapProvisioningLogic ldapProvisioningLogic;
  private Map<String, String> testLdapOptions;
  private String testUsername;
  private String testPassword;
  private String testUserDN;
  private Set<String> testGroups;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    ldapProvisioningLogic =
        new LdapProvisioningLogic(mockSystemEntityClient, mockSystemOperationContext);

    testUsername = "testuser";
    testPassword = "testpass";
    testUserDN = "CN=TestUser,OU=Users,DC=example,DC=com";
    testGroups = Set.of("group1", "group2", "admin");

    testLdapOptions = new HashMap<>();
    testLdapOptions.put("userProvider", "ldaps://server.example.com:636/DC=example,DC=com");
    testLdapOptions.put("groupProvider", "ldaps://server.example.com:636");
  }

  // ========== Tests for provisionUser ==========

  @Test
  public void testProvisionUserSuccess() throws Exception {
    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "test@example.com");
      userAttributes.put("displayName", "Test User");
      userAttributes.put("givenName", "Test");
      userAttributes.put("sn", "User");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client responses
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(false); // User and groups don't exist

      // Execute the method
      String result =
          ldapProvisioningLogic.provisionUser(
              testUsername, testPassword, testGroups, testUserDN, testLdapOptions);

      // Verify results
      assertNotNull(result);
      assertEquals(new CorpuserUrn(testUsername).toString(), result);

      // Verify user provisioning
      ArgumentCaptor<Entity> userEntityCaptor = ArgumentCaptor.forClass(Entity.class);
      verify(mockSystemEntityClient)
          .update(eq(mockSystemOperationContext), userEntityCaptor.capture());

      // Verify group provisioning
      ArgumentCaptor<Set> groupEntitiesCaptor = ArgumentCaptor.forClass(Set.class);
      verify(mockSystemEntityClient)
          .batchUpdate(eq(mockSystemOperationContext), groupEntitiesCaptor.capture());

      @SuppressWarnings("unchecked")
      Set<Entity> groupEntities = (Set<Entity>) groupEntitiesCaptor.getValue();
      assertEquals(3, groupEntities.size());

      // Verify group membership update
      ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
          ArgumentCaptor.forClass(MetadataChangeProposal.class);
      verify(mockSystemEntityClient)
          .ingestProposal(eq(mockSystemOperationContext), proposalCaptor.capture(), eq(false));

      MetadataChangeProposal proposal = proposalCaptor.getValue();
      assertEquals(result, proposal.getEntityUrn().toString());
      assertEquals("corpuser", proposal.getEntityType());
      assertEquals("groupMembership", proposal.getAspectName());
    }
  }

  @Test
  public void testProvisionUserWithExistingUserAndGroups() throws Exception {
    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "test@example.com");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client responses - user and groups already exist
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(true);

      // Execute the method
      String result =
          ldapProvisioningLogic.provisionUser(
              testUsername, testPassword, testGroups, testUserDN, testLdapOptions);

      // Verify results
      assertNotNull(result);
      assertEquals(new CorpuserUrn(testUsername).toString(), result);

      // Verify user provisioning was skipped (no update call)
      verify(mockSystemEntityClient, never())
          .update(eq(mockSystemOperationContext), any(Entity.class));

      // Verify group provisioning was skipped (no batchUpdate call)
      verify(mockSystemEntityClient, never())
          .batchUpdate(eq(mockSystemOperationContext), any(Set.class));

      // Verify group membership update still happens
      verify(mockSystemEntityClient)
          .ingestProposal(
              eq(mockSystemOperationContext), any(MetadataChangeProposal.class), eq(false));
    }
  }

  @Test
  public void testProvisionUserWithNullUserDN() throws Exception {
    // Execute the method with null userDN
    String result =
        ldapProvisioningLogic.provisionUser(
            testUsername, testPassword, testGroups, null, testLdapOptions);

    // Verify results
    assertNotNull(result);
    assertEquals(new CorpuserUrn(testUsername).toString(), result);

    // Verify user provisioning still happens
    verify(mockSystemEntityClient).update(eq(mockSystemOperationContext), any(Entity.class));
  }

  @Test
  public void testProvisionUserWithNullLdapOptions() throws Exception {
    // Execute the method with null ldapOptions
    String result =
        ldapProvisioningLogic.provisionUser(
            testUsername, testPassword, testGroups, testUserDN, null);

    // Verify results
    assertNotNull(result);
    assertEquals(new CorpuserUrn(testUsername).toString(), result);

    // Verify user provisioning still happens
    verify(mockSystemEntityClient).update(eq(mockSystemOperationContext), any(Entity.class));
  }

  @Test
  public void testProvisionUserWithNullUsername() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              ldapProvisioningLogic.provisionUser(
                  null, testPassword, testGroups, testUserDN, testLdapOptions);
            });
    assertNotNull(exception);
    assertTrue(exception.getMessage().contains("Failed to provision LDAP user"));
  }

  @Test
  public void testProvisionUserWithNullPassword() {
    // The method actually handles null password gracefully and continues execution
    // It logs a warning but doesn't throw an exception
    assertDoesNotThrow(
        () -> {
          String result =
              ldapProvisioningLogic.provisionUser(
                  testUsername, null, testGroups, testUserDN, testLdapOptions);
          assertNotNull(result);
          assertEquals(new CorpuserUrn(testUsername).toString(), result);
        });
  }

  @Test
  public void testProvisionUserWithNullGroups() {
    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction to return empty map
      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(new HashMap<>());

      RuntimeException exception =
          assertThrows(
              RuntimeException.class,
              () -> {
                ldapProvisioningLogic.provisionUser(
                    testUsername, testPassword, null, testUserDN, testLdapOptions);
              });
      assertNotNull(exception);
      assertTrue(exception.getMessage().contains("Failed to provision LDAP user"));
    }
  }

  @Test
  public void testProvisionUserWithAttributeExtractionException() throws Exception {
    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction to throw exception
      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenThrow(new RuntimeException("LDAP connection failed"));

      // Mock entity client responses
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(false);

      // Execute the method - should not throw exception
      String result =
          ldapProvisioningLogic.provisionUser(
              testUsername, testPassword, testGroups, testUserDN, testLdapOptions);

      // Verify results
      assertNotNull(result);
      assertEquals(new CorpuserUrn(testUsername).toString(), result);

      // Verify user provisioning still happens (with empty attributes)
      verify(mockSystemEntityClient).update(eq(mockSystemOperationContext), any(Entity.class));
    }
  }

  @Test
  public void testProvisionUserWithUserProvisioningException() throws Exception {
    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "test@example.com");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client to throw exception on user update
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(false);
      doThrow(new RuntimeException("User provisioning failed"))
          .when(mockSystemEntityClient)
          .update(eq(mockSystemOperationContext), any(Entity.class));

      // Execute the method - should throw RuntimeException
      RuntimeException exception =
          assertThrows(
              RuntimeException.class,
              () -> {
                ldapProvisioningLogic.provisionUser(
                    testUsername, testPassword, testGroups, testUserDN, testLdapOptions);
              });
      assertNotNull(exception);
    }
  }

  @Test
  public void testProvisionUserWithGroupProvisioningException() throws Exception {
    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "test@example.com");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client responses
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(false); // User doesn't exist, groups don't exist
      doThrow(new RuntimeException("Group provisioning failed"))
          .when(mockSystemEntityClient)
          .batchUpdate(eq(mockSystemOperationContext), any(Set.class));

      // Execute the method - should throw RuntimeException
      RuntimeException exception =
          assertThrows(
              RuntimeException.class,
              () -> {
                ldapProvisioningLogic.provisionUser(
                    testUsername, testPassword, testGroups, testUserDN, testLdapOptions);
              });
      assertNotNull(exception);
    }
  }

  @Test
  public void testProvisionUserWithComplexAttributes() throws Exception {
    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction with complex data
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "john.doe@company.com");
      userAttributes.put("displayName", "John Doe");
      userAttributes.put("givenName", "John");
      userAttributes.put("sn", "Doe");
      userAttributes.put("userPrincipalName", "john.doe@company.com");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client responses
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(false);

      // Execute the method
      String result =
          ldapProvisioningLogic.provisionUser(
              testUsername, testPassword, testGroups, testUserDN, testLdapOptions);

      // Verify results
      assertNotNull(result);
      assertEquals(new CorpuserUrn(testUsername).toString(), result);

      // Verify user entity was created with correct attributes
      ArgumentCaptor<Entity> userEntityCaptor = ArgumentCaptor.forClass(Entity.class);
      verify(mockSystemEntityClient)
          .update(eq(mockSystemOperationContext), userEntityCaptor.capture());

      Entity userEntity = userEntityCaptor.getValue();
      assertNotNull(userEntity);
    }
  }

  @Test
  public void testProvisionUserWithAttributesButNoDisplayName() throws Exception {
    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction without displayName
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "test@example.com");
      userAttributes.put("givenName", "Test");
      userAttributes.put("sn", "User");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client responses
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(false);

      // Execute the method
      String result =
          ldapProvisioningLogic.provisionUser(
              testUsername, testPassword, testGroups, testUserDN, testLdapOptions);

      // Verify results
      assertNotNull(result);
      assertEquals(new CorpuserUrn(testUsername).toString(), result);

      // Verify user provisioning
      verify(mockSystemEntityClient).update(eq(mockSystemOperationContext), any(Entity.class));
    }
  }

  @Test
  public void testProvisionUserWithPartialGroupProvisioning() throws Exception {
    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "test@example.com");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client responses - user doesn't exist, some groups exist
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenAnswer(
              invocation -> {
                Urn urn = invocation.getArgument(1);
                // Only group1 exists
                return urn.toString().contains("group1");
              });

      // Execute the method
      String result =
          ldapProvisioningLogic.provisionUser(
              testUsername, testPassword, testGroups, testUserDN, testLdapOptions);

      // Verify results
      assertNotNull(result);
      assertEquals(new CorpuserUrn(testUsername).toString(), result);

      // Verify user provisioning
      verify(mockSystemEntityClient).update(eq(mockSystemOperationContext), any(Entity.class));

      // Verify group provisioning (should only provision non-existing groups)
      ArgumentCaptor<Set> groupEntitiesCaptor = ArgumentCaptor.forClass(Set.class);
      verify(mockSystemEntityClient)
          .batchUpdate(eq(mockSystemOperationContext), groupEntitiesCaptor.capture());

      @SuppressWarnings("unchecked")
      Set<Entity> groupEntities = (Set<Entity>) groupEntitiesCaptor.getValue();
      assertEquals(2, groupEntities.size()); // Only group2 and admin should be provisioned
    }
  }

  @Test
  public void testProvisionUserWithGroupMembershipException() throws Exception {
    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "test@example.com");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client responses
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(false);
      when(mockSystemEntityClient.ingestProposal(
              eq(mockSystemOperationContext), any(MetadataChangeProposal.class), eq(false)))
          .thenThrow(new RuntimeException("Group membership update failed"));

      // Execute the method - should not throw exception (group membership failure is logged but not
      // propagated)
      String result =
          ldapProvisioningLogic.provisionUser(
              testUsername, testPassword, testGroups, testUserDN, testLdapOptions);

      // Verify results
      assertNotNull(result);
      assertEquals(new CorpuserUrn(testUsername).toString(), result);

      // Verify user and group provisioning still happened
      verify(mockSystemEntityClient).update(eq(mockSystemOperationContext), any(Entity.class));
      verify(mockSystemEntityClient).batchUpdate(eq(mockSystemOperationContext), any(Set.class));
    }
  }

  @Test
  public void testProvisionUserWithSingleGroup() throws Exception {
    Set<String> singleGroup = Set.of("admin");

    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "test@example.com");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client responses
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(false);

      // Execute the method
      String result =
          ldapProvisioningLogic.provisionUser(
              testUsername, testPassword, singleGroup, testUserDN, testLdapOptions);

      // Verify results
      assertNotNull(result);
      assertEquals(new CorpuserUrn(testUsername).toString(), result);

      // Verify group provisioning
      ArgumentCaptor<Set> groupEntitiesCaptor = ArgumentCaptor.forClass(Set.class);
      verify(mockSystemEntityClient)
          .batchUpdate(eq(mockSystemOperationContext), groupEntitiesCaptor.capture());

      @SuppressWarnings("unchecked")
      Set<Entity> groupEntities = (Set<Entity>) groupEntitiesCaptor.getValue();
      assertEquals(1, groupEntities.size());
    }
  }

  @Test
  public void testProvisionUserWithSpecialCharactersInUsername() throws Exception {
    String specialUsername = "test.user@domain";

    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "test.user@domain.com");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, specialUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client responses
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(false);

      // Execute the method
      String result =
          ldapProvisioningLogic.provisionUser(
              specialUsername, testPassword, testGroups, testUserDN, testLdapOptions);

      // Verify results
      assertNotNull(result);
      assertEquals(new CorpuserUrn(specialUsername).toString(), result);

      // Verify user provisioning
      verify(mockSystemEntityClient).update(eq(mockSystemOperationContext), any(Entity.class));
    }
  }

  @Test
  public void testProvisionUserWithSpecialCharactersInGroupNames() throws Exception {
    Set<String> specialGroups = Set.of("group-1", "group_2", "group.3");

    try (MockedStatic<AuthenticationManager> mockedAuthManager =
        mockStatic(AuthenticationManager.class)) {
      // Mock user attributes extraction
      Map<String, String> userAttributes = new HashMap<>();
      userAttributes.put("mail", "test@example.com");

      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.getUserAttributesFromLdap(
                      testUserDN, testLdapOptions, testUsername, testPassword))
          .thenReturn(userAttributes);

      // Mock entity client responses
      when(mockSystemEntityClient.exists(eq(mockSystemOperationContext), any(Urn.class)))
          .thenReturn(false);

      // Execute the method
      String result =
          ldapProvisioningLogic.provisionUser(
              testUsername, testPassword, specialGroups, testUserDN, testLdapOptions);

      // Verify results
      assertNotNull(result);
      assertEquals(new CorpuserUrn(testUsername).toString(), result);

      // Verify group provisioning
      ArgumentCaptor<Set> groupEntitiesCaptor = ArgumentCaptor.forClass(Set.class);
      verify(mockSystemEntityClient)
          .batchUpdate(eq(mockSystemOperationContext), groupEntitiesCaptor.capture());

      @SuppressWarnings("unchecked")
      Set<Entity> groupEntities = (Set<Entity>) groupEntitiesCaptor.getValue();
      assertEquals(3, groupEntities.size());
    }
  }
}
