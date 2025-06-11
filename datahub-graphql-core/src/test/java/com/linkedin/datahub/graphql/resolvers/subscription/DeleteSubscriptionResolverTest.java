package com.linkedin.datahub.graphql.resolvers.subscription;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.DeleteSubscriptionInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.SubscriptionService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
    when(_subscriptionService.getSubscriptionInfo(
            any(OperationContext.class), eq(SUBSCRIPTION_URN_1)))
        .thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test(
      expectedExceptions = DataHubGraphQLException.class,
      expectedExceptionsMessageRegExp = ".* missing MANAGE_USER_SUBSCRIPTIONS privilege")
  public void testDeleteSubscriptionUnauthorizedForOtherUser() throws Throwable {
    final String requestUserUrn = USER_2_URN_STRING;

    final AuthorizationRequest request =
        new AuthorizationRequest(
            requestUserUrn, "MANAGE_USER_SUBSCRIPTIONS", Optional.empty(), Collections.emptyList());
    final QueryContext mockContext = getMockDenyContext(requestUserUrn, request);
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    when(_subscriptionService.getSubscriptionInfo(
            any(OperationContext.class), eq(SUBSCRIPTION_URN_1)))
        .thenReturn(SUBSCRIPTION_INFO_1);

    ExecutionException exception =
        expectThrows(ExecutionException.class, () -> _resolver.get(_dataFetchingEnvironment).get());
    throw exception.getCause();
  }

  @Test
  public void testDeleteSubscriptionAuthorizedForOtherUser() throws Exception {
    final String requestUserUrn = USER_2_URN_STRING;

    final AuthorizationRequest request =
        new AuthorizationRequest(
            requestUserUrn, "MANAGE_USER_SUBSCRIPTIONS", Optional.empty(), Collections.emptyList());
    final QueryContext mockContext = getMockAllowContext(requestUserUrn, request);
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    when(_subscriptionService.getSubscriptionInfo(
            any(OperationContext.class), eq(SUBSCRIPTION_URN_1)))
        .thenReturn(SUBSCRIPTION_INFO_1);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_entityClient, times(1))
        .deleteEntity(any(OperationContext.class), eq(SUBSCRIPTION_URN_1));
  }

  @Test
  public void testDeleteSubscription() throws Exception {
    when(_subscriptionService.getSubscriptionInfo(
            any(OperationContext.class), eq(SUBSCRIPTION_URN_1)))
        .thenReturn(SUBSCRIPTION_INFO_1);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_entityClient, times(1))
        .deleteEntity(any(OperationContext.class), eq(SUBSCRIPTION_URN_1));
  }
}
