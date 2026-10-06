package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.role.RoleService;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchAssignRoleInput;
import com.linkedin.entity.client.EntityClientCache;
import com.linkedin.entity.client.SystemEntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchAssignRoleResolverTest {
  private static final String ROLE_URN_STRING = "urn:li:dataHubRole:Admin";
  private static final String FIRST_ACTOR_URN_STRING = "urn:li:corpuser:foo";
  private static final String SECOND_ACTOR_URN_STRING = "urn:li:corpuser:bar";
  private Urn roleUrn;
  private RoleService _roleService;
  private SystemEntityClient _systemEntityClient;
  private BatchAssignRoleResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    roleUrn = Urn.createFromString(ROLE_URN_STRING);
    _roleService = mock(RoleService.class);
    _systemEntityClient = mock(SystemEntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new BatchAssignRoleResolver(_roleService, _systemEntityClient);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testNullRole() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    BatchAssignRoleInput input = new BatchAssignRoleInput();
    List<String> actors = ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING);
    input.setActors(actors);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testNotNullRole() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    BatchAssignRoleInput input = new BatchAssignRoleInput();
    input.setRoleUrn(ROLE_URN_STRING);
    List<String> actors = ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING);
    input.setActors(actors);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testCacheInvalidationIsCalledForAllAssignedActors() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    EntityClientCache mockEntityClientCache = mock(EntityClientCache.class);
    when(_systemEntityClient.getEntityClientCache()).thenReturn(mockEntityClientCache);

    BatchAssignRoleInput input = new BatchAssignRoleInput();
    input.setRoleUrn(ROLE_URN_STRING);
    List<String> actors = ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING);
    input.setActors(actors);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    verify(mockEntityClientCache, times(1))
        .invalidate(eq(Urn.createFromString(FIRST_ACTOR_URN_STRING)), eq(Set.of("roleMembership")));
    verify(mockEntityClientCache, times(1))
        .invalidate(
            eq(Urn.createFromString(SECOND_ACTOR_URN_STRING)), eq(Set.of("roleMembership")));
  }

  @Test
  public void testCacheInvalidationSkippedWhenCacheIsNull() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    when(_systemEntityClient.getEntityClientCache()).thenReturn(null);

    BatchAssignRoleInput input = new BatchAssignRoleInput();
    input.setRoleUrn(ROLE_URN_STRING);
    List<String> actors = ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING);
    input.setActors(actors);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), eq(actors), eq(roleUrn));
  }

  @Test
  public void testRoleAssignmentSucceedsEvenIfCacheInvalidationFails() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    EntityClientCache mockEntityClientCache = mock(EntityClientCache.class);
    when(_systemEntityClient.getEntityClientCache()).thenReturn(mockEntityClientCache);
    doThrow(new RuntimeException("Cache invalidation failed"))
        .when(mockEntityClientCache)
        .invalidate(any(), any());

    BatchAssignRoleInput input = new BatchAssignRoleInput();
    input.setRoleUrn(ROLE_URN_STRING);
    List<String> actors = ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING);
    input.setActors(actors);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), eq(actors), eq(roleUrn));
  }
}
