package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.FormActorAssignment;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IsFormAssignedToMeResolverTest {

  private static final Urn TEST_USER_1 = UrnUtils.getUrn("urn:li:corpuser:test-1");
  private static final Urn TEST_USER_2 = UrnUtils.getUrn("urn:li:corpuser:test-2");
  private static final Urn TEST_GROUP_1 = UrnUtils.getUrn("urn:li:corpGroup:test-1");
  private static final Urn TEST_GROUP_2 = UrnUtils.getUrn("urn:li:corpGroup:test-2");

  @Test
  public void testGetSuccessUserMatch() throws Exception {
    CorpGroup assignedGroup = new CorpGroup();
    assignedGroup.setUrn(TEST_GROUP_1.toString());

    CorpUser assignedUser = new CorpUser();
    assignedUser.setUrn(TEST_USER_1.toString());

    FormActorAssignment actors = new FormActorAssignment();
    actors.setGroups(new ArrayList<>(ImmutableList.of(assignedGroup)));
    actors.setUsers(new ArrayList<>(ImmutableList.of(assignedUser)));

    QueryContext mockContext = getMockAllowContext(TEST_USER_1.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(actors);

    IsFormAssignedToMeResolver resolver = new IsFormAssignedToMeResolver();
    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetSuccessGroupMatch() throws Exception {
    CorpGroup assignedGroup = new CorpGroup();
    assignedGroup.setUrn(TEST_GROUP_1.toString());

    CorpUser assignedUser = new CorpUser();
    assignedUser.setUrn(TEST_USER_2.toString());

    FormActorAssignment actors = new FormActorAssignment();
    actors.setGroups(new ArrayList<>(ImmutableList.of(assignedGroup)));
    actors.setUsers(new ArrayList<>(ImmutableList.of(assignedUser)));

    QueryContext mockContext =
        getMockAllowContext(TEST_USER_1.toString(), ImmutableList.of(TEST_GROUP_1));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(actors);

    IsFormAssignedToMeResolver resolver = new IsFormAssignedToMeResolver();
    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetSuccessBothMatch() throws Exception {
    CorpGroup assignedGroup = new CorpGroup();
    assignedGroup.setUrn(TEST_GROUP_1.toString());

    CorpUser assignedUser = new CorpUser();
    assignedUser.setUrn(TEST_USER_1.toString());

    FormActorAssignment actors = new FormActorAssignment();
    actors.setGroups(new ArrayList<>(ImmutableList.of(assignedGroup)));
    actors.setUsers(new ArrayList<>(ImmutableList.of(assignedUser)));

    QueryContext mockContext =
        getMockAllowContext(TEST_USER_1.toString(), ImmutableList.of(TEST_GROUP_1));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(actors);

    IsFormAssignedToMeResolver resolver = new IsFormAssignedToMeResolver();
    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetSuccessNoMatchNullAssignment() throws Exception {
    FormActorAssignment actors = new FormActorAssignment();

    QueryContext mockContext =
        getMockAllowContext(TEST_USER_1.toString(), ImmutableList.of(TEST_GROUP_1, TEST_GROUP_2));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(actors);

    IsFormAssignedToMeResolver resolver = new IsFormAssignedToMeResolver();
    assertFalse(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetSuccessNoMatchEmptyAssignment() throws Exception {
    FormActorAssignment actors = new FormActorAssignment();
    actors.setUsers(Collections.emptyList());
    actors.setGroups(Collections.emptyList());

    QueryContext mockContext =
        getMockAllowContext(TEST_USER_1.toString(), ImmutableList.of(TEST_GROUP_1, TEST_GROUP_2));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(actors);

    IsFormAssignedToMeResolver resolver = new IsFormAssignedToMeResolver();
    assertFalse(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetSuccessNoMatchNoAssignmentMatch() throws Exception {
    CorpGroup assignedGroup = new CorpGroup();
    assignedGroup.setUrn(TEST_GROUP_2.toString());

    CorpUser assignedUser = new CorpUser();
    assignedUser.setUrn(TEST_USER_2.toString());

    FormActorAssignment actors = new FormActorAssignment();
    actors.setGroups(new ArrayList<>(ImmutableList.of(assignedGroup)));
    actors.setUsers(new ArrayList<>(ImmutableList.of(assignedUser)));

    QueryContext mockContext =
        getMockAllowContext(TEST_USER_1.toString(), ImmutableList.of(TEST_GROUP_1));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getSource()).thenReturn(actors);

    IsFormAssignedToMeResolver resolver = new IsFormAssignedToMeResolver();
    assertFalse(resolver.get(mockEnv).get());
  }
}
