package com.linkedin.datahub.graphql.resolvers.subscription;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.SLACK_USER_HANDLE;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.USER_URN;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.USER_URN_STRING;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.EntityChangeDetailsInput;
import com.linkedin.datahub.graphql.generated.EntityChangeType;
import com.linkedin.datahub.graphql.generated.NotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfigInput;
import com.linkedin.datahub.graphql.generated.SyncSubscriptionInput;
import com.linkedin.metadata.service.SubscriptionService;
import com.linkedin.subscription.EntityChangeDetails;
import com.linkedin.subscription.EntityChangeDetailsArray;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SyncSubscriptionResolverTest {
  private static final DataHubSubscription MAPPED_SUBSCRIPTION_1 = getMappedSubscription1();
  private static final String GROUP_URN_STRING = "urn:li:corpGroup:testGroup";
  private static final String INVALID_URN_STRING = "urn:li:dataset:invalid";

  private SyncSubscriptionResolver _resolver;
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

    final SyncSubscriptionInput input = new SyncSubscriptionInput();
    input.setEntityUrn(ENTITY_URN_1_STRING);
    input.setActorUrn(USER_URN_STRING);
    input.setEntityChangeTypes(
        List.of(
            new EntityChangeDetailsInput(EntityChangeType.DEPRECATED, null),
            new EntityChangeDetailsInput(EntityChangeType.ASSERTION_FAILED, null)));

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
    input.setNotificationConfig(notificationConfigInput);

    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    _resolver = new SyncSubscriptionResolver(_subscriptionService);
  }

  @Test
  public void testSyncSubscriptionUnauthorizedForOtherUser() {
    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_2_URN_STRING);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testSyncSubscriptionUnauthorizedForGroup() {
    final SyncSubscriptionInput input = new SyncSubscriptionInput();
    input.setEntityUrn(ENTITY_URN_1_STRING);
    input.setActorUrn(GROUP_URN_STRING);
    input.setEntityChangeTypes(
        List.of(new EntityChangeDetailsInput(EntityChangeType.DEPRECATED, null)));
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    final QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testSyncSubscriptionUnauthorized() {
    final QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testSyncSubscriptionInvalidActorUrn() {
    final SyncSubscriptionInput input = new SyncSubscriptionInput();
    input.setEntityUrn(ENTITY_URN_1_STRING);
    input.setActorUrn(INVALID_URN_STRING); // Not a user or group URN
    input.setEntityChangeTypes(
        List.of(new EntityChangeDetailsInput(EntityChangeType.DEPRECATED, null)));
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetSubscriptionExceptionThrown() {
    when(_subscriptionService.getSubscription(
            any(OperationContext.class), eq(ENTITY_URN_1), eq(USER_URN)))
        .thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateSubscriptionExceptionThrown() {
    when(_subscriptionService.getSubscription(
            any(OperationContext.class), eq(ENTITY_URN_1), eq(USER_URN)))
        .thenReturn(Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));
    when(_subscriptionService.updateSubscription(
            any(OperationContext.class),
            eq(USER_URN),
            eq(SUBSCRIPTION_URN_1),
            eq(SUBSCRIPTION_INFO_1),
            eq(null),
            any(),
            any()))
        .thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testCreateSubscriptionExceptionThrown() {
    when(_subscriptionService.getSubscription(
            any(OperationContext.class), eq(ENTITY_URN_1), eq(USER_URN)))
        .thenReturn(null); // No existing subscription
    when(_subscriptionService.createSubscription(
            any(OperationContext.class), eq(USER_URN), eq(ENTITY_URN_1), any(), any(), any()))
        .thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testSyncSubscriptionCreateNew() throws Exception {
    // No existing subscription
    when(_subscriptionService.getSubscription(
            any(OperationContext.class), eq(ENTITY_URN_1), eq(USER_URN)))
        .thenReturn(null);
    when(_subscriptionService.createSubscription(
            any(OperationContext.class), eq(USER_URN), eq(ENTITY_URN_1), any(), any(), any()))
        .thenReturn(Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));

    final DataHubSubscription subscription = _resolver.get(_dataFetchingEnvironment).join();
    final DataHubSubscriptionMatcher matcher =
        new DataHubSubscriptionMatcher(MAPPED_SUBSCRIPTION_1);
    assertTrue(matcher.matches(subscription));
  }

  @Test
  public void testSyncSubscriptionUpdateExisting() throws Exception {
    // Existing subscription found
    when(_subscriptionService.getSubscription(
            any(OperationContext.class), eq(ENTITY_URN_1), eq(USER_URN)))
        .thenReturn(Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));
    when(_subscriptionService.updateSubscription(
            any(OperationContext.class),
            eq(USER_URN),
            eq(SUBSCRIPTION_URN_1),
            eq(SUBSCRIPTION_INFO_1),
            eq(null),
            any(),
            any()))
        .thenReturn(Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));

    final DataHubSubscription subscription = _resolver.get(_dataFetchingEnvironment).join();
    final DataHubSubscriptionMatcher matcher =
        new DataHubSubscriptionMatcher(MAPPED_SUBSCRIPTION_1);
    assertTrue(matcher.matches(subscription));
  }

  @Test
  public void testSyncSubscriptionForGroupAuthorized() throws Exception {
    final SyncSubscriptionInput input = new SyncSubscriptionInput();
    input.setEntityUrn(ENTITY_URN_1_STRING);
    input.setActorUrn(GROUP_URN_STRING);
    input.setEntityChangeTypes(
        List.of(new EntityChangeDetailsInput(EntityChangeType.DEPRECATED, null)));
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    // No existing subscription
    when(_subscriptionService.getSubscription(any(OperationContext.class), eq(ENTITY_URN_1), any()))
        .thenReturn(null);
    when(_subscriptionService.createSubscription(
            any(OperationContext.class), any(), eq(ENTITY_URN_1), any(), any(), any()))
        .thenReturn(Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));

    final DataHubSubscription subscription = _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(subscription);
  }

  @Test
  public void testSyncSubscriptionWithNullNotificationConfig() throws Exception {
    final SyncSubscriptionInput input = new SyncSubscriptionInput();
    input.setEntityUrn(ENTITY_URN_1_STRING);
    input.setActorUrn(USER_URN_STRING);
    input.setEntityChangeTypes(
        List.of(new EntityChangeDetailsInput(EntityChangeType.DEPRECATED, null)));
    input.setNotificationConfig(null); // Null notification config
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // No existing subscription
    when(_subscriptionService.getSubscription(
            any(OperationContext.class), eq(ENTITY_URN_1), eq(USER_URN)))
        .thenReturn(null);
    when(_subscriptionService.createSubscription(
            any(OperationContext.class), eq(USER_URN), eq(ENTITY_URN_1), any(), any(), eq(null)))
        .thenReturn(Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));

    final DataHubSubscription subscription = _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(subscription);
  }

  @Test
  public void testSyncSubscriptionMergeEntityChangeTypes() throws Exception {
    // Create input with new and overlapping entity change types
    final SyncSubscriptionInput input = new SyncSubscriptionInput();
    input.setEntityUrn(ENTITY_URN_1_STRING);
    input.setActorUrn(USER_URN_STRING);
    input.setEntityChangeTypes(
        List.of(
            // This should overwrite the existing DEPRECATED entry
            new EntityChangeDetailsInput(EntityChangeType.DEPRECATED, null),
            // This is a new type not in existing subscription
            new EntityChangeDetailsInput(EntityChangeType.TAG_ADDED, null)));
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // Mock existing subscription with existing entity change types
    // SUBSCRIPTION_INFO_1 contains DEPRECATED and ASSERTION_FAILED
    when(_subscriptionService.getSubscription(
            any(OperationContext.class), eq(ENTITY_URN_1), eq(USER_URN)))
        .thenReturn(Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1));

    // Capture the merged entity change types passed to updateSubscription
    when(_subscriptionService.updateSubscription(
            any(OperationContext.class),
            eq(USER_URN),
            eq(SUBSCRIPTION_URN_1),
            eq(SUBSCRIPTION_INFO_1),
            eq(null),
            any(EntityChangeDetailsArray.class),
            any()))
        .thenAnswer(
            invocation -> {
              EntityChangeDetailsArray mergedTypes = invocation.getArgument(5);

              // Verify merged result contains:
              // 1. DEPRECATED (overwritten from input)
              // 2. ASSERTION_FAILED (preserved from existing)
              // 3. TAG_ADDED (new from input)
              assertEquals(3, mergedTypes.size());

              Set<com.linkedin.subscription.EntityChangeType> types = new HashSet<>();
              for (EntityChangeDetails detail : mergedTypes) {
                types.add(detail.getEntityChangeType());
              }

              assertTrue(types.contains(com.linkedin.subscription.EntityChangeType.DEPRECATED));
              assertTrue(
                  types.contains(com.linkedin.subscription.EntityChangeType.ASSERTION_FAILED));
              assertTrue(types.contains(com.linkedin.subscription.EntityChangeType.TAG_ADDED));

              return Map.entry(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1);
            });

    final DataHubSubscription subscription = _resolver.get(_dataFetchingEnvironment).join();
    assertNotNull(subscription);
  }
}
