package com.datahub.notification;

import com.datahub.authentication.Authentication;
import com.datahub.notification.provider.IdentityProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;


public class IdentityProviderTest {

  private static final Urn TEST_USER_1 = Urn.createFromTuple(CORP_USER_ENTITY_NAME, "test");
  private static final Urn TEST_USER_2 = Urn.createFromTuple(CORP_USER_ENTITY_NAME, "test2");
  private static final Set<Urn> TEST_USER_URNS = ImmutableSet.of(
      TEST_USER_1,
      TEST_USER_2
  );

  @Test
  public void testGetUser() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockClient.batchGetV2(
        Mockito.eq(CORP_USER_ENTITY_NAME),
        Mockito.eq(ImmutableSet.of(TEST_USER_1)),
        Mockito.eq(ImmutableSet.of(
            CORP_USER_INFO_ASPECT_NAME,
            CORP_USER_EDITABLE_INFO_NAME,
            CORP_USER_STATUS_ASPECT_NAME
        )),
        Mockito.any(Authentication.class)
    )).thenReturn(
        ImmutableMap.of(TEST_USER_1, mockTestUser1Response())
    );

    final Authentication mockAuthentication = Mockito.mock(Authentication.class);
    final IdentityProvider identityProvider = new IdentityProvider(mockClient, mockAuthentication);
    final IdentityProvider.User result = identityProvider.getUser(TEST_USER_1);

    // Verify the response
    verifyUser(result, expectedUser1());
  }

  @Test
  public void testBatchGetUsers() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockClient.batchGetV2(
        Mockito.eq(CORP_USER_ENTITY_NAME),
        Mockito.eq(TEST_USER_URNS),
        Mockito.eq(ImmutableSet.of(
            CORP_USER_INFO_ASPECT_NAME,
            CORP_USER_EDITABLE_INFO_NAME,
            CORP_USER_STATUS_ASPECT_NAME
        )),
        Mockito.any(Authentication.class)
    )).thenReturn(
        mockTestUsersResponse()
    );


    final Authentication mockAuthentication = Mockito.mock(Authentication.class);
    final IdentityProvider identityProvider = new IdentityProvider(mockClient, mockAuthentication);
    final Map<Urn, IdentityProvider.User> result = identityProvider.batchGetUsers(TEST_USER_URNS);

    // Verify the response
    verifyUser(result.get(TEST_USER_1), expectedUser1());
    verifyUser(result.get(TEST_USER_2), expectedUser2());
  }

  private void verifyUser(IdentityProvider.User actual, IdentityProvider.User expected) {
    Assert.assertEquals(actual.getDisplayName(), expected.getDisplayName());
    Assert.assertEquals(actual.getFirstName(), expected.getFirstName());
    Assert.assertEquals(actual.getLastName(), expected.getLastName());
    Assert.assertEquals(actual.getEmail(), expected.getEmail());
    Assert.assertEquals(actual.getSlack(), expected.getSlack());
    Assert.assertEquals(actual.getTitle(), expected.getTitle());
  }

  private Map<Urn, EntityResponse> mockTestUsersResponse() {
    final Map<Urn, EntityResponse> result = new HashMap<>();
    result.put(TEST_USER_1, mockTestUser1Response());
    result.put(TEST_USER_2, mockTestUser2Response());
    return result;
  }

  private EntityResponse mockTestUser1Response() {
    return mockTestUsersResponse("Test", "User", "Test User", "testuser@gmail.com", "Engineer");
  }

  private EntityResponse mockTestUser2Response() {
    return mockTestUsersResponse("Test", "User 2", "Test User 2", "testuser2@gmail.com", "Sales");
  }

  private EntityResponse mockTestUsersResponse(String firstName, String lastName, String displayName, String email, String title) {
    final EntityResponse user = new EntityResponse();
    user.setUrn(TEST_USER_1);
    user.setEntityName(CORP_USER_ENTITY_NAME);
    final EnvelopedAspectMap testUser1Aspects = new EnvelopedAspectMap();
    testUser1Aspects.put(CORP_USER_INFO_ASPECT_NAME, new EnvelopedAspect()
        .setName(CORP_USER_INFO_ASPECT_NAME)
        .setType(AspectType.VERSIONED)
        .setCreated(mockAuditStamp())
        .setValue(new Aspect(
            new CorpUserInfo()
                .setActive(true)
                .setEmail(email)
                .setFirstName(firstName)
                .setLastName(lastName)
                .setDisplayName(displayName)
                .setFullName(firstName + lastName)
                .setTitle(title).data()
        ))
    );
    testUser1Aspects.put(CORP_USER_EDITABLE_INFO_NAME, new EnvelopedAspect()
        .setName(CORP_USER_EDITABLE_INFO_NAME)
        .setType(AspectType.VERSIONED)
        .setCreated(mockAuditStamp())
        .setValue(new Aspect(
            new CorpUserEditableInfo()
                .setEmail(email)
                .setDisplayName(displayName).data()
        ))
    );
    user.setAspects(testUser1Aspects);
    return user;
  }

  private AuditStamp mockAuditStamp() {
    return new AuditStamp().setActor(TEST_USER_1).setTime(0L);
  }

  private IdentityProvider.User expectedUser1() {
    IdentityProvider.User expectedUser1 = new IdentityProvider.User();
    expectedUser1.setActive(true);
    expectedUser1.setEmail("testuser@gmail.com");
    expectedUser1.setTitle("Engineer");
    expectedUser1.setDisplayName("Test User");
    expectedUser1.setFirstName("Test");
    expectedUser1.setLastName("User");
    return expectedUser1;
  }

  private IdentityProvider.User expectedUser2() {
    IdentityProvider.User expectedUser2 = new IdentityProvider.User();
    expectedUser2.setActive(true);
    expectedUser2.setEmail("testuser2@gmail.com");
    expectedUser2.setTitle("Sales");
    expectedUser2.setDisplayName("Test User 2");
    expectedUser2.setFirstName("Test");
    expectedUser2.setLastName("User 2");
    return expectedUser2;
  }
}