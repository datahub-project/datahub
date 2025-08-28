package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
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

    GroupService groupService = mockGroupService(TEST_CORP_USER_URN_1, Collections.emptyList());
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

    GroupService groupService = mockGroupService(TEST_CORP_USER_URN_1, Collections.emptyList());
    IsAssignedToMeResolver resolver = new IsAssignedToMeResolver(groupService);
    assertFalse(resolver.get(mockEnv).get());
  }

  private GroupService mockGroupService(final Urn userUrn, final List<Urn> groupUrns)
      throws Exception {
    GroupService mockService = Mockito.mock(GroupService.class);
    Mockito.when(mockService.getGroupsForUser(any(), Mockito.eq(userUrn))).thenReturn(groupUrns);
    return mockService;
  }
}
