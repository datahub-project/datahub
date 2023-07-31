package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.subscription.SubscriptionService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.NotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfigInput;
import com.linkedin.datahub.graphql.generated.UpdateSubscriptionInput;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.SLACK_USER_HANDLE;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.USER_URN;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.USER_URN_STRING;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class UpdateSubscriptionResolverTest {
  private static final DataHubSubscription MAPPED_SUBSCRIPTION_1 = getMappedSubscription1();
  private UpdateSubscriptionResolver _resolver;
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

    final UpdateSubscriptionInput input = new UpdateSubscriptionInput();
    input.setSubscriptionUrn(SUBSCRIPTION_URN_1_STRING);
    input.setSubscriptionTypes(SUBSCRIPTION_GRAPHQL_TYPES_1);
    input.setEntityChangeTypes(ENTITY_CHANGE_GRAPHQL_TYPES_1);

    final SubscriptionNotificationConfigInput notificationConfigInput = new SubscriptionNotificationConfigInput();
    final NotificationSettingsInput notificationSettings = new NotificationSettingsInput();
    notificationSettings.setSinkTypes(NOTIFICATION_SINK_GRAPHQL_TYPES);
    final SlackNotificationSettingsInput slackSettings = new SlackNotificationSettingsInput();
    slackSettings.setUserHandle(SLACK_USER_HANDLE);
    notificationSettings.setSlackSettings(slackSettings);
    notificationConfigInput.setNotificationSettings(notificationSettings);

    input.setNotificationConfig(notificationConfigInput);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    _resolver = new UpdateSubscriptionResolver(_subscriptionService);
  }

  @Test
  public void testCreateSubscriptionUnauthorized() {
    final QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetSubscriptionExceptionThrown() {
    when(_subscriptionService.getSubscriptionInfo(
        eq(SUBSCRIPTION_URN_1),
        eq(_authentication)))
        .thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateSubscriptionExceptionThrown() {
    when(_subscriptionService.getSubscriptionInfo(
        eq(SUBSCRIPTION_URN_1),
        eq(_authentication)))
        .thenReturn(SUBSCRIPTION_INFO_1);
    when(_subscriptionService.updateSubscription(
        eq(USER_URN),
        eq(SUBSCRIPTION_URN_1),
        eq(SUBSCRIPTION_INFO_1),
        eq(SUBSCRIPTION_TYPES_1),
        eq(ENTITY_CHANGE_TYPES_1),
        eq(NOTIFICATION_CONFIG),
        eq(_authentication)))
        .thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateSubscription() throws Exception {
    when(_subscriptionService.getSubscriptionInfo(
        eq(SUBSCRIPTION_URN_1),
        eq(_authentication)))
        .thenReturn(SUBSCRIPTION_INFO_1);
    when(_subscriptionService.updateSubscription(
        eq(USER_URN),
        eq(SUBSCRIPTION_URN_1),
        eq(SUBSCRIPTION_INFO_1),
        eq(SUBSCRIPTION_TYPES_1),
        eq(ENTITY_CHANGE_TYPES_1),
        eq(NOTIFICATION_CONFIG),
        eq(_authentication)))
        .thenReturn(Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));

    final DataHubSubscription subscription = _resolver.get(_dataFetchingEnvironment).join();
    final DataHubSubscriptionMatcher matcher1 = new DataHubSubscriptionMatcher(MAPPED_SUBSCRIPTION_1);
    assertTrue(matcher1.matches(subscription));
  }
}
