package com.linkedin.datahub.graphql.resolvers.subscription;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntitySubscriptionSummary;
import com.linkedin.datahub.graphql.generated.GetEntitySubscriptionSummaryInput;
import com.linkedin.metadata.service.SubscriptionService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetEntitySubscriptionSummaryResolverTest {
  private GetEntitySubscriptionSummaryResolver _resolver;
  private SubscriptionService _subscriptionService;
  private GroupService _groupService;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _subscriptionService = mock(SubscriptionService.class);
    _groupService = mock(GroupService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    final GetEntitySubscriptionSummaryInput input = new GetEntitySubscriptionSummaryInput();
    input.setEntityUrn(ENTITY_URN_1_STRING);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    _resolver = new GetEntitySubscriptionSummaryResolver(_subscriptionService, _groupService);
  }

  @Test
  public void testGetEntitySubscriptionSummaryExceptionThrown() {
    when(_subscriptionService.isActorSubscribed(
            any(OperationContext.class), eq(ENTITY_URN_1), eq(USER_URN)))
        .thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetEntitySubscriptionSummary() throws Exception {
    List<Urn> largeList =
        Stream.generate(() -> ENTITY_URN_1).limit(50).collect(Collectors.toList());
    when(_subscriptionService.isActorSubscribed(
            any(OperationContext.class), eq(ENTITY_URN_1), eq(USER_URN)))
        .thenReturn(true);
    when(_groupService.getGroupsForUser(any(OperationContext.class), eq(USER_URN)))
        .thenReturn(Collections.emptyList());
    when(_subscriptionService.isAnyGroupSubscribed(
            any(OperationContext.class), eq(ENTITY_URN_1), any()))
        .thenReturn(false);
    when(_subscriptionService.getNumUserSubscriptionsForEntity(
            any(OperationContext.class), eq(ENTITY_URN_1), anyInt()))
        .thenReturn(50);
    when(_subscriptionService.getNumGroupSubscriptionsForEntity(
            any(OperationContext.class), eq(ENTITY_URN_1), anyInt()))
        .thenReturn(25);
    when(_subscriptionService.getGroupSubscribersForEntity(
            any(OperationContext.class), eq(ENTITY_URN_1), anyInt()))
        .thenReturn(Collections.emptyList());
    when(_subscriptionService.getSubscribedUsersForEntity(
            any(OperationContext.class), eq(ENTITY_URN_1), anyInt()))
        .thenReturn(largeList);

    final EntitySubscriptionSummary summary = _resolver.get(_dataFetchingEnvironment).join();
    assertTrue(summary.getIsUserSubscribed());
    assertFalse(summary.getIsUserSubscribedViaGroup());
    assertEquals(summary.getUserSubscriptionCount(), 50);
    assertEquals(summary.getGroupSubscriptionCount(), 25);
    assertEquals(summary.getExampleGroups().size(), 0);
    assertEquals(summary.getSubscribedGroups().size(), 0);
    assertEquals(summary.getSubscribedUsers().size(), 50);
  }
}
