package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.subscription.SubscriptionService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DeleteSubscriptionInput;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class DeleteSubscriptionResolverTest {
  private DeleteSubscriptionResolver _resolver;
  private SubscriptionService _subscriptionService;
  private EntityClient _entityClient;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _subscriptionService = mock(SubscriptionService.class);
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    final DeleteSubscriptionInput input = new DeleteSubscriptionInput();
    input.setSubscriptionUrn(SUBSCRIPTION_URN_1_STRING);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    _resolver = new DeleteSubscriptionResolver(_subscriptionService, _entityClient);
  }

  @Test
  public void testDeleteSubscriptionExceptionThrown() {
    when(_subscriptionService.getSubscriptionInfo(eq(SUBSCRIPTION_URN_1), eq(_authentication))).thenThrow(
        new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testDeleteSubscriptionUnauthorizedForOtherUser() {
    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_2_URN_STRING);
    when(_subscriptionService.getSubscriptionInfo(eq(SUBSCRIPTION_URN_1), eq(_authentication))).thenReturn(
        SUBSCRIPTION_INFO_1);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }


  @Test
  public void testDeleteSubscription() throws Exception {
    when(_subscriptionService.getSubscriptionInfo(eq(SUBSCRIPTION_URN_1), eq(_authentication))).thenReturn(
        SUBSCRIPTION_INFO_1);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_entityClient, times(1)).deleteEntity(eq(SUBSCRIPTION_URN_1), eq(_authentication));
  }
}
