package com.linkedin.datahub.graphql.resolvers.subscription;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.SLACK_USER_HANDLE;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.USER_URN;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.USER_URN_STRING;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateSubscriptionInput;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.NotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfigInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.metadata.service.SubscriptionService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateSubscriptionResolverTest {
  private static final DataHubSubscription MAPPED_SUBSCRIPTION_1 = getMappedSubscription1();
  private CreateSubscriptionResolver _resolver;
  private SubscriptionService _subscriptionService;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;
  private CreateSubscriptionInput _input;

  @BeforeMethod
  public void setupTest() throws Exception {
    _subscriptionService = mock(SubscriptionService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    _input = new CreateSubscriptionInput();
    _input.setEntityUrn(ENTITY_URN_1_STRING);
    _input.setSubscriptionTypes(SUBSCRIPTION_GRAPHQL_TYPES_1);
    _input.setEntityChangeTypes(ENTITY_CHANGE_GRAPHQL_TYPES_1);

    final SubscriptionNotificationConfigInput notificationConfigInput =
        new SubscriptionNotificationConfigInput();
    final NotificationSettingsInput notificationSettings = new NotificationSettingsInput();
    notificationSettings.setSinkTypes(NOTIFICATION_SINK_GRAPHQL_TYPES);

    final SlackNotificationSettingsInput slackSettings = new SlackNotificationSettingsInput();
    slackSettings.setUserHandle(SLACK_USER_HANDLE);
    notificationSettings.setSlackSettings(slackSettings);

    final EmailNotificationSettingsInput emailSettings = new EmailNotificationSettingsInput();
    emailSettings.setEmail(EMAIL_ADDRESS);
    notificationSettings.setEmailSettings(emailSettings);

    notificationConfigInput.setNotificationSettings(notificationSettings);

    _input.setNotificationConfig(notificationConfigInput);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(_input);

    _resolver = new CreateSubscriptionResolver(_subscriptionService);
  }

  @Test
  public void testCreateSubscriptionUnauthorized() {
    final QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test(
      expectedExceptions = DataHubGraphQLException.class,
      expectedExceptionsMessageRegExp = ".* missing MANAGE_USER_SUBSCRIPTIONS privilege")
  public void testCreateSubscriptionUnauthorizedForOtherUser() throws Throwable {
    final QueryContext mockContext =
        getMockDenyContext(
            USER_URN_STRING,
            new AuthorizationRequest(
                USER_URN_STRING, "MANAGE_USER_SUBSCRIPTIONS", Optional.empty()));

    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    _input.setUserUrn(USER_2_URN_STRING);
    try (MockedStatic<ResolverUtils> mockedStatic = mockStatic(ResolverUtils.class)) {
      mockedStatic
          .when(() -> ResolverUtils.bindArgument(any(CreateSubscriptionInput.class), any()))
          .thenReturn(_input);

      ExecutionException exception =
          expectThrows(
              ExecutionException.class, () -> _resolver.get(_dataFetchingEnvironment).get());
      throw exception.getCause();
    }
  }

  @Test
  public void testCreateSubscriptionAuthorizedForOtherUser() throws Exception {
    final QueryContext mockContext =
        getMockAllowContext(
            USER_URN_STRING,
            new AuthorizationRequest(
                USER_URN_STRING, "MANAGE_USER_SUBSCRIPTIONS", Optional.empty()));

    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    _input.setUserUrn(USER_2_URN_STRING);

    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(_input);

    when(_subscriptionService.createSubscription(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(USER_2_URN_STRING)),
            eq(ENTITY_URN_1),
            eq(SUBSCRIPTION_TYPES_1),
            eq(ENTITY_CHANGE_TYPES_1),
            eq(NOTIFICATION_CONFIG)))
        .thenReturn(Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));

    final DataHubSubscription subscription = _resolver.get(_dataFetchingEnvironment).join();
    final DataHubSubscriptionMatcher matcher1 =
        new DataHubSubscriptionMatcher(MAPPED_SUBSCRIPTION_1);
    assertTrue(matcher1.matches(subscription));
  }

  @Test
  public void testCreateSubscriptionExceptionThrown() {
    when(_subscriptionService.createSubscription(
            any(OperationContext.class),
            eq(USER_URN),
            eq(ENTITY_URN_1),
            eq(SUBSCRIPTION_TYPES_1),
            eq(ENTITY_CHANGE_TYPES_1),
            eq(NOTIFICATION_CONFIG)))
        .thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testCreateSubscription() throws Exception {
    when(_subscriptionService.createSubscription(
            any(OperationContext.class),
            eq(USER_URN),
            eq(ENTITY_URN_1),
            eq(SUBSCRIPTION_TYPES_1),
            eq(ENTITY_CHANGE_TYPES_1),
            eq(NOTIFICATION_CONFIG)))
        .thenReturn(Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));

    final DataHubSubscription subscription = _resolver.get(_dataFetchingEnvironment).join();
    final DataHubSubscriptionMatcher matcher1 =
        new DataHubSubscriptionMatcher(MAPPED_SUBSCRIPTION_1);
    assertTrue(matcher1.matches(subscription));
  }
}
