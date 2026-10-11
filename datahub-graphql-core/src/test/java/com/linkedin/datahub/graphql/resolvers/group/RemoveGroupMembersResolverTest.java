package com.linkedin.datahub.graphql.resolvers.group;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.google.common.base.Throwables;
import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.RemoveGroupMembersInput;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RemoveGroupMembersResolverTest {
  private static final String GROUP_URN_STRING = "urn:li:corpGroup:testNewGroup";
  private static final String USER_URN_STRING = "urn:li:corpuser:test";

  private static Urn _groupUrn;

  private GroupService _groupService;
  private RemoveGroupMembersResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _groupUrn = Urn.createFromString(GROUP_URN_STRING);

    _groupService = mock(GroupService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);
    RemoveGroupMembersInput input = new RemoveGroupMembersInput();
    input.setGroupUrn(GROUP_URN_STRING);
    input.setUserUrns(new ArrayList<>(Collections.singleton(USER_URN_STRING)));

    _resolver = new RemoveGroupMembersResolver(_groupService);

    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
  }

  @Test
  public void testFailsCannotManageUsersAndGroups() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsExternalGroup() {
    Origin groupOrigin = new Origin();
    groupOrigin.setType(OriginType.EXTERNAL);

    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);
    when(_groupService.groupExists(any(), any())).thenReturn(true);
    when(_groupService.getGroupOrigin(any(), eq(_groupUrn))).thenReturn(groupOrigin);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPassesNativeGroup() throws Exception {
    Origin groupOrigin = new Origin();
    groupOrigin.setType(OriginType.NATIVE);

    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);
    when(_groupService.groupExists(any(), any())).thenReturn(true);
    when(_groupService.getGroupOrigin(any(), eq(_groupUrn))).thenReturn(groupOrigin);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // A native group skips the migration and goes straight to removeGroupMembers, which revokes
    // either membership aspect. Reverting to the native-only path would otherwise pass here.
    verify(_groupService)
        .removeGroupMembers(
            any(), eq(_groupUrn), eq(List.of(Urn.createFromString(USER_URN_STRING))));
    verify(_groupService, never())
        .migrateGroupMembershipToNativeGroupMembership(any(), any(), any());
  }

  @Test
  public void testMigratesOriginLessGroupBeforeRemoving() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);
    when(_groupService.groupExists(any(), any())).thenReturn(true);
    // No origin aspect - the inline migration must run before members are removed.
    when(_groupService.getGroupOrigin(any(), eq(_groupUrn))).thenReturn(null);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    InOrder inOrder = inOrder(_groupService);
    inOrder
        .verify(_groupService)
        .migrateGroupMembershipToNativeGroupMembership(any(), eq(_groupUrn), eq(USER_URN_STRING));
    inOrder.verify(_groupService).removeGroupMembers(any(), eq(_groupUrn), any());
  }

  @Test
  public void testMigrationFailurePreservesCause() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);
    when(_groupService.groupExists(any(), any())).thenReturn(true);
    when(_groupService.getGroupOrigin(any(), eq(_groupUrn))).thenReturn(null);
    RuntimeException cause = new RuntimeException("ingest rejected the proposal");
    doThrow(cause)
        .when(_groupService)
        .migrateGroupMembershipToNativeGroupMembership(any(), any(), any());

    // Without the cause attached, GMS logs show only the wrapper and not the real failure.
    Exception thrown =
        expectThrows(Exception.class, () -> _resolver.get(_dataFetchingEnvironment).join());
    assertTrue(Throwables.getCausalChain(thrown).contains(cause));
    verify(_groupService, never()).removeGroupMembers(any(), any(), any());
  }
}
