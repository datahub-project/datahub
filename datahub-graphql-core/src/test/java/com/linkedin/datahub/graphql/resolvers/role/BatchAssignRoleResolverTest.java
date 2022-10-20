package com.linkedin.datahub.graphql.resolvers.role;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.role.RoleService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchAssignRoleInput;
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
  private Urn roleUrn;
  private RoleService _roleService;
  private BatchAssignRoleResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    roleUrn = Urn.createFromString(ROLE_URN_STRING);
    _roleService = mock(RoleService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new BatchAssignRoleResolver(_roleService);
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
    when(_roleService.exists(eq(roleUrn), eq(_authentication))).thenReturn(false);

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
    doThrow(RuntimeException.class).when(_roleService)
        .assignRoleToActor(eq(SECOND_ACTOR_URN_STRING), eq(roleUrn), eq(_authentication));
    when(_roleService.exists(eq(Urn.createFromString(ROLE_URN_STRING)), eq(_authentication))).thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
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
    when(_roleService.exists(eq(Urn.createFromString(ROLE_URN_STRING)), eq(_authentication))).thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }
}
