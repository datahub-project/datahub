package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.datahub.subscription.SubscriptionService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntitySubscriptionSummary;
import com.linkedin.datahub.graphql.generated.GetEntitySubscriptionSummaryInput;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


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
    when(_subscriptionService.isUserSubscribed(eq(ENTITY_URN_1), eq(USER_URN), eq(_authentication))).thenThrow(
        new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetEntitySubscriptionSummary() throws Exception {
    when(_subscriptionService.isUserSubscribed(eq(ENTITY_URN_1), eq(USER_URN), eq(_authentication))).thenReturn(true);
    when(_groupService.getGroupsForUser(eq(USER_URN), eq(_authentication))).thenReturn(Collections.emptyList());
    when(_subscriptionService.isAnyGroupSubscribed(eq(ENTITY_URN_1), any(), eq(_authentication))).thenReturn(false);
    when(_subscriptionService.getNumUserSubscriptionsForEntity(eq(ENTITY_URN_1), anyInt(), eq(_authentication)))
        .thenReturn(50);
    when(_subscriptionService.getNumGroupSubscriptionsForEntity(eq(ENTITY_URN_1), anyInt(),
        eq(_authentication))).thenReturn(25);
    when(_subscriptionService.getGroupSubscribersForEntity(eq(ENTITY_URN_1), anyInt(), eq(_authentication)))
        .thenReturn(Collections.emptyList());

    final EntitySubscriptionSummary summary = _resolver.get(_dataFetchingEnvironment).join();
    assertTrue(summary.getIsUserSubscribed());
    assertFalse(summary.getIsUserSubscribedViaGroup());
    assertEquals(summary.getUserSubscriptionCount(), 50);
    assertEquals(summary.getGroupSubscriptionCount(), 25);
    assertEquals(summary.getTopGroups().size(), 0);
  }
}
