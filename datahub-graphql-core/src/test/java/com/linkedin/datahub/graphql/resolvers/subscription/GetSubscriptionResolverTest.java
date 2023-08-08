package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.subscription.SubscriptionService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.GetSubscriptionInput;
import com.linkedin.datahub.graphql.generated.GetSubscriptionResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class GetSubscriptionResolverTest {
  private static final DataHubSubscription MAPPED_SUBSCRIPTION_1 = getMappedSubscription1();
  private GetSubscriptionResolver _resolver;
  private SubscriptionService _subscriptionService;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _subscriptionService = mock(SubscriptionService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    final GetSubscriptionInput input = new GetSubscriptionInput();
    input.setEntityUrn(ENTITY_URN_1_STRING);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    _resolver = new GetSubscriptionResolver(_subscriptionService);
  }

  @Test
  public void testGetSubscriptionExceptionThrown() {
    when(_subscriptionService.getSubscription(eq(ENTITY_URN_1), eq(USER_URN), eq(_authentication))).thenThrow(
        new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetSubscription() throws Exception {
    when(_subscriptionService.getSubscription(eq(ENTITY_URN_1), eq(USER_URN), eq(_authentication))).thenReturn(
        Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));

    final GetSubscriptionResult result = _resolver.get(_dataFetchingEnvironment).join();
    assertTrue(result.getPrivileges().getCanManageEntity());
    final DataHubSubscriptionMatcher matcher1 = new DataHubSubscriptionMatcher(MAPPED_SUBSCRIPTION_1);
    assertTrue(matcher1.matches(result.getSubscription()));
  }
}
