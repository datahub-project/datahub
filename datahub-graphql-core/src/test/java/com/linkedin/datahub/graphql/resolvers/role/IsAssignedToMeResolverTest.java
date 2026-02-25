package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.resolvers.dataset.IsAssignedToMeResolver;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IsAssignedToMeResolverTest {

  private static final Urn TEST_CORP_USER_URN_1 = UrnUtils.getUrn("urn:li:corpuser:test-user-1");
  private static final Urn TEST_CORP_USER_URN_2 = UrnUtils.getUrn("urn:li:corpuser:test-user-2");
  private static final Urn TEST_CORP_USER_URN_3 = UrnUtils.getUrn("urn:li:corpuser:test-user-3");
  private static final Urn TEST_CORP_GROUP_URN_1 = UrnUtils.getUrn("urn:li:corpGroup:test-group-1");
  private static final Urn TEST_CORP_GROUP_URN_2 = UrnUtils.getUrn("urn:li:corpGroup:test-group-2");

  @Test
  public void testReturnsTrueIfCurrentUserIsAssignedToRole() throws Exception {

    CorpUser corpUser1 = new CorpUser();
    corpUser1.setUrn(TEST_CORP_USER_URN_1.toString());
    CorpUser corpUser2 = new CorpUser();
    corpUser2.setUrn(TEST_CORP_USER_URN_2.toString());
    CorpUser corpUser3 = new CorpUser();
    corpUser3.setUrn(TEST_CORP_USER_URN_3.toString());

    ArrayList<RoleUser> roleUsers = new ArrayList<>();
    roleUsers.add(new RoleUser(corpUser1));
    roleUsers.add(new RoleUser(corpUser2));
    roleUsers.add(new RoleUser(corpUser3));

    Actor actor = new Actor();
    actor.setUsers(roleUsers);
    Role role = new Role();
    role.setUrn("urn:li:role:fake-role");
    role.setActors(actor);

    QueryContext mockContext = getMockAllowContext(TEST_CORP_USER_URN_1.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(role);

    GroupService groupService =
        createTestGroupService(TEST_CORP_USER_URN_1, Collections.emptyList());
    IsAssignedToMeResolver resolver = new IsAssignedToMeResolver(groupService);
    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testReturnsFalseIfCurrentUserIsNotAssignedToRole() throws Exception {

    CorpUser corpUser1 = new CorpUser();
    corpUser1.setUrn(TEST_CORP_USER_URN_1.toString());
    CorpUser corpUser2 = new CorpUser();
    corpUser2.setUrn(TEST_CORP_USER_URN_2.toString());
    CorpUser corpUser3 = new CorpUser();
    corpUser3.setUrn(TEST_CORP_USER_URN_3.toString());

    ArrayList<RoleUser> roleUsers = new ArrayList<>();
    roleUsers.add(new RoleUser(corpUser2));
    roleUsers.add(new RoleUser(corpUser3));

    Actor actor = new Actor();
    actor.setUsers(roleUsers);
    Role role = new Role();
    role.setUrn("urn:li:role:fake-role");
    role.setActors(actor);

    QueryContext mockContext = getMockAllowContext(TEST_CORP_USER_URN_1.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(role);

    GroupService groupService =
        createTestGroupService(TEST_CORP_USER_URN_1, Collections.emptyList());
    IsAssignedToMeResolver resolver = new IsAssignedToMeResolver(groupService);
    assertFalse(resolver.get(mockEnv).get());
  }

  @Test
  public void testReturnsTrueIfCurrentUserIsAssignedToRoleViaGroup() throws Exception {

    // Create a group that will be assigned to the role
    CorpGroup corpGroup1 = new CorpGroup();
    corpGroup1.setUrn(TEST_CORP_GROUP_URN_1.toString());

    // Create RoleGroup that links the group to the role
    RoleGroup roleGroup1 = new RoleGroup();
    roleGroup1.setGroup(corpGroup1);

    ArrayList<RoleGroup> roleGroups = new ArrayList<>();
    roleGroups.add(roleGroup1);

    // Create Actor with groups but no direct user assignments
    Actor actor = new Actor();
    actor.setUsers(Collections.emptyList()); // No direct user assignments
    actor.setGroups(roleGroups); // Group is assigned to role

    Role role = new Role();
    role.setUrn("urn:li:role:fake-role");
    role.setActors(actor);

    QueryContext mockContext = getMockAllowContext(TEST_CORP_USER_URN_1.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(role);

    // Mock that the user is a member of the group that's assigned to the role
    List<Urn> userGroups = Collections.singletonList(TEST_CORP_GROUP_URN_1);
    GroupService groupService = createTestGroupService(TEST_CORP_USER_URN_1, userGroups);

    IsAssignedToMeResolver resolver = new IsAssignedToMeResolver(groupService);

    // This should return true because:
    // 1. The user is not directly assigned to the role
    // 2. But the user is a member of a group that IS assigned to the role
    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testReturnsFalseIfCurrentUserIsNotInGroupAssignedToRole() throws Exception {

    // Create a group that will be assigned to the role
    CorpGroup corpGroup1 = new CorpGroup();
    corpGroup1.setUrn(TEST_CORP_GROUP_URN_1.toString());

    // Create RoleGroup that links the group to the role
    RoleGroup roleGroup1 = new RoleGroup();
    roleGroup1.setGroup(corpGroup1);

    ArrayList<RoleGroup> roleGroups = new ArrayList<>();
    roleGroups.add(roleGroup1);

    // Create Actor with groups but no direct user assignments
    Actor actor = new Actor();
    actor.setUsers(Collections.emptyList()); // No direct user assignments
    actor.setGroups(roleGroups); // Group is assigned to role

    Role role = new Role();
    role.setUrn("urn:li:role:fake-role");
    role.setActors(actor);

    QueryContext mockContext = getMockAllowContext(TEST_CORP_USER_URN_1.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(role);

    // Mock that the user is NOT a member of the group that's assigned to the role
    // Instead, they're in a different group
    List<Urn> userGroups = Collections.singletonList(TEST_CORP_GROUP_URN_2);
    GroupService groupService = createTestGroupService(TEST_CORP_USER_URN_1, userGroups);

    IsAssignedToMeResolver resolver = new IsAssignedToMeResolver(groupService);

    // This should return false because:
    // 1. The user is not directly assigned to the role
    // 2. The user is in a group, but that group is NOT assigned to the role
    assertFalse(resolver.get(mockEnv).get());
  }

  private GroupService createTestGroupService(final Urn userUrn, final List<Urn> groupUrns) {
    // Create minimal mocks for the required dependencies
    com.linkedin.entity.client.EntityClient mockEntityClient =
        Mockito.mock(com.linkedin.entity.client.EntityClient.class);
    com.linkedin.metadata.entity.EntityService mockEntityService =
        Mockito.mock(com.linkedin.metadata.entity.EntityService.class);
    com.linkedin.metadata.graph.GraphClient mockGraphClient =
        Mockito.mock(com.linkedin.metadata.graph.GraphClient.class);

    return new GroupService(mockEntityClient, mockEntityService, mockGraphClient) {
      @Override
      public List<Urn> getGroupsForUser(
          io.datahubproject.metadata.context.OperationContext opContext, Urn user) {
        if (user.equals(userUrn)) {
          return groupUrns;
        }
        return Collections.emptyList();
      }
    };
  }
}
