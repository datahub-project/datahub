package com.linkedin.datahub.graphql.resolvers.group;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RemoveGroupResolverTest {
  private static final String GROUP_URN_STRING = "urn:li:corpGroup:testGroup";
  private static final Urn GROUP_URN = UrnUtils.getUrn(GROUP_URN_STRING);
  private static final Urn MEMBER_URN = UrnUtils.getUrn("urn:li:corpuser:member");

  private EntityClient _entityClient;
  private GroupService _groupService;
  private RemoveGroupResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _groupService = mock(GroupService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _resolver = new RemoveGroupResolver(_entityClient, _groupService);

    when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(GROUP_URN_STRING);
    when(_groupService.getNativeGroupMembers(eq(GROUP_URN), any())).thenReturn(List.of(MEMBER_URN));
  }

  @Test
  public void testFailsUnauthorized() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testCapturesMembersBeforeDeletingTheGroup() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // The captured list must be read while the edges still exist — after deleteEntity the
    // key-aspect DELETE MCL leads to removeNode(), which reaps them.
    InOrder inOrder = inOrder(_groupService, _entityClient);
    inOrder.verify(_groupService).getNativeGroupMembers(eq(GROUP_URN), any());
    inOrder.verify(_entityClient).deleteEntity(any(), eq(GROUP_URN));
  }

  @Test
  public void testCleansCapturedMembersWhenGroupStaysDeleted() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(_groupService.groupExists(any(), eq(GROUP_URN))).thenReturn(false);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    verify(_groupService, timeout(10000))
        .removeExistingNativeGroupMembers(any(), eq(GROUP_URN), eq(List.of(MEMBER_URN)));
  }

  @Test
  public void testSkipsCleanupWhenGroupWasRecreated() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(_groupService.groupExists(any(), eq(GROUP_URN))).thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // deleteEntityReferences runs after the membership cleanup decision, so observing it means
    // the decision has been made.
    verify(_entityClient, timeout(10000)).deleteEntityReferences(any(), eq(GROUP_URN));
    verify(_groupService, never()).removeExistingNativeGroupMembers(any(), any(), any());
  }

  @Test
  public void testDeleteEntityReferencesStillRunsWhenGroupExistsCheckThrows() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(_groupService.groupExists(any(), eq(GROUP_URN)))
        .thenThrow(new RuntimeException("simulated storage failure"));

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // A failure in the membership cleanup guard must not suppress deleteEntityReferences.
    verify(_entityClient, timeout(10000)).deleteEntityReferences(any(), eq(GROUP_URN));
  }

  @Test
  public void testDeleteStillSucceedsWhenCapturingMembersThrows() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    // Simulates JavaGraphClient#getRelatedEntities blowing up on a single unparseable URN found
    // in the graph index. Without isolating this capture, that would make the group permanently
    // undeletable, since the same corrupt edge would be hit on every retry.
    when(_groupService.getNativeGroupMembers(eq(GROUP_URN), any()))
        .thenThrow(new RuntimeException("simulated corrupt graph edge"));

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    verify(_entityClient, timeout(10000)).deleteEntity(any(), eq(GROUP_URN));
  }
}
