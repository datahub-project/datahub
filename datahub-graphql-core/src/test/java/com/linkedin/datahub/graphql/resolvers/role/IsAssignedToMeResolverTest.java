package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.resolvers.dataset.IsAssignedToMeResolver;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
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

    IsAssignedToMeResolver resolver = new IsAssignedToMeResolver();
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

    IsAssignedToMeResolver resolver = new IsAssignedToMeResolver();
    assertFalse(resolver.get(mockEnv).get());
  }
}
