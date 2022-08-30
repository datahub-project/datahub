package com.linkedin.datahub.graphql.resolvers.role;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchAssignRoleInput;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class BatchAssignRoleResolverTest {
  private static final String ROLE_URN_STRING = "urn:li:dataHubRole:Admin";
  private static final String FIRST_ACTOR_URN_STRING = "urn:li:corpuser:foo";
  private static final String SECOND_ACTOR_URN_STRING = "urn:li:corpuser:bar";

  private EntityClient _entityClient;
  private BatchAssignRoleResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new BatchAssignRoleResolver(_entityClient);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testRoleDoesNotExistFails() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    BatchAssignRoleInput input = new BatchAssignRoleInput();
    input.setRoleUrn(ROLE_URN_STRING);
    List<String> actors = new ArrayList<>();
    actors.add(FIRST_ACTOR_URN_STRING);
    input.setActors(actors);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(_entityClient.exists(eq(Urn.createFromString(ROLE_URN_STRING)), eq(_authentication))).thenReturn(false);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testSomeActorsExist() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    BatchAssignRoleInput input = new BatchAssignRoleInput();
    input.setRoleUrn(ROLE_URN_STRING);
    List<String> actors = new ArrayList<>();
    actors.add(FIRST_ACTOR_URN_STRING);
    actors.add(SECOND_ACTOR_URN_STRING);
    input.setActors(actors);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_entityClient.exists(eq(Urn.createFromString(ROLE_URN_STRING)), eq(_authentication))).thenReturn(true);
    when(_entityClient.exists(eq(Urn.createFromString(FIRST_ACTOR_URN_STRING)), eq(_authentication))).thenReturn(true);
    when(_entityClient.exists(eq(Urn.createFromString(SECOND_ACTOR_URN_STRING)), eq(_authentication))).thenReturn(
        false);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    // Only the first actor should be assigned to the role since the second actor does not exist
    verify(_entityClient, times(1)).ingestProposal(any(), eq(_authentication));
  }

  @Test
  public void testAllActorsExist() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    BatchAssignRoleInput input = new BatchAssignRoleInput();
    input.setRoleUrn(ROLE_URN_STRING);
    List<String> actors = new ArrayList<>();
    actors.add(FIRST_ACTOR_URN_STRING);
    actors.add(SECOND_ACTOR_URN_STRING);
    input.setActors(actors);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_entityClient.exists(eq(Urn.createFromString(ROLE_URN_STRING)), eq(_authentication))).thenReturn(true);
    when(_entityClient.exists(eq(Urn.createFromString(FIRST_ACTOR_URN_STRING)), eq(_authentication))).thenReturn(true);
    when(_entityClient.exists(eq(Urn.createFromString(SECOND_ACTOR_URN_STRING)), eq(_authentication))).thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    // Both actors exist and should be assigned to the role
    verify(_entityClient, times(2)).ingestProposal(any(), eq(_authentication));
  }
}
