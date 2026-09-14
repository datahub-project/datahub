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
    when(_groupService.getNativeGroupMembers(any(), eq(GROUP_URN))).thenReturn(List.of(MEMBER_URN));
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
    inOrder.verify(_groupService).getNativeGroupMembers(any(), eq(GROUP_URN));
    inOrder.verify(_entityClient).deleteEntity(any(), eq(GROUP_URN));
  }

  @Test
  public void testHandsCapturedMembersToTheCleanupUnconditionally() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // The whole captured list goes over untouched. Gating it on "was the group recreated?" would
    // abandon every member nobody re-added, leaving them holding privileges that authorization
    // reads from this aspect - the exact orphaning this sweep exists to prevent.
    verify(_groupService, timeout(10000))
        .removeStaleNativeGroupMembership(any(), eq(GROUP_URN), eq(List.of(MEMBER_URN)));
    verify(_groupService, never()).groupExists(any(), any());
  }

  @Test
  public void testSkipsCleanupWhenNoMembersWereCaptured() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(_groupService.getNativeGroupMembers(any(), eq(GROUP_URN))).thenReturn(List.of());

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // The cleanup decision is now made after deleteEntityReferences, so observing that call no
    // longer proves the decision has been reached. Wait the sweep out instead of racing it.
    verify(_entityClient, timeout(10000)).deleteEntityReferences(any(), eq(GROUP_URN));
    verify(_groupService, after(1000).never())
        .removeStaleNativeGroupMembership(any(), any(), any());
  }

  @Test
  public void testDeleteEntityReferencesStillRunsWhenCleanupThrows() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    doThrow(new RuntimeException("simulated storage failure"))
        .when(_groupService)
        .removeStaleNativeGroupMembership(any(), eq(GROUP_URN), any());

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // A failure in the membership cleanup must not suppress deleteEntityReferences.
    verify(_entityClient, timeout(10000)).deleteEntityReferences(any(), eq(GROUP_URN));
  }

  @Test
  public void testMembershipCleanupStillRunsWhenDeleteEntityReferencesThrows() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    doThrow(new RuntimeException("simulated reference cleanup failure"))
        .when(_entityClient)
        .deleteEntityReferences(any(), eq(GROUP_URN));

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // The mirror of the test above, and the one that matters now that references are cleaned
    // first: the membership sweep is what keeps a recreated group from handing its privileges to
    // members nobody re-added, so a failure ahead of it must not cost it its turn.
    verify(_groupService, timeout(10000))
        .removeStaleNativeGroupMembership(any(), eq(GROUP_URN), eq(List.of(MEMBER_URN)));
  }

  @Test
  public void testDeleteStillSucceedsWhenCapturingMembersThrows() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    // Simulates the member scroll blowing up on a single unparseable URN found in the graph
    // index. Without isolating this capture, that would make the group permanently undeletable,
    // since the same corrupt edge would be hit on every retry.
    when(_groupService.getNativeGroupMembers(any(), eq(GROUP_URN)))
        .thenThrow(new RuntimeException("simulated corrupt graph edge"));

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    verify(_entityClient, timeout(10000)).deleteEntity(any(), eq(GROUP_URN));
  }

  @Test
  public void testClearsEntityReferencesBeforeMembership() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // deleteEntityReferences discovers referrers by scrolling the graph index, so it is racing
    // the removeNode() that the delete's key-aspect MCL triggers, and every reference it has not
    // reached by then - ownership above all - is silently left behind. The membership cleanup
    // works from the list captured before the delete, so it is immune to the same race and loses
    // nothing by going second.
    verify(_groupService, timeout(10000))
        .removeStaleNativeGroupMembership(any(), eq(GROUP_URN), any());
    InOrder inOrder = inOrder(_entityClient, _groupService);
    inOrder.verify(_entityClient).deleteEntityReferences(any(), eq(GROUP_URN));
    inOrder.verify(_groupService).removeStaleNativeGroupMembership(any(), eq(GROUP_URN), any());
  }
}
