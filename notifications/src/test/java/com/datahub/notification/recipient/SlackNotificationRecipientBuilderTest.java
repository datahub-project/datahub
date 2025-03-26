package com.datahub.notification.recipient;

import static org.mockito.Mockito.*;

import com.datahub.notification.NotificationScenarioType;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.NotificationSettingValue;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.SlackIntegrationSettings;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SlackNotificationRecipientBuilderTest {
  @Mock private SettingsService settingsService;
  @Mock private GlobalSettingsInfo globalSettingsInfo;

  private SlackNotificationRecipientBuilder builder;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    MockitoAnnotations.openMocks(this);
    when(settingsService.getGlobalSettings(opContext)).thenReturn(globalSettingsInfo);
    builder = new SlackNotificationRecipientBuilder(settingsService);
  }

  @Test
  public void testBuildGlobalRecipientsWithDisabledSlack() {
    GlobalNotificationSettings globalNotificationSettings = mock(GlobalNotificationSettings.class);
    when(globalNotificationSettings.hasSettings()).thenReturn(true);
    when(globalNotificationSettings.getSettings())
        .thenReturn(
            new NotificationSettingMap(
                Collections.singletonMap(
                    "ASSERTION_STATUS_CHANGE",
                    new NotificationSetting()
                        .setValue(NotificationSettingValue.ENABLED)
                        .setParams(
                            new StringMap(
                                ImmutableMap.of(
                                    "slack.enabled", "false",
                                    "slack.channel", "test-channel"))))));

    when(globalSettingsInfo.hasNotifications()).thenReturn(true);
    when(globalSettingsInfo.getNotifications()).thenReturn(globalNotificationSettings);
    when(globalSettingsInfo.hasIntegrations()).thenReturn(true);
    when(globalSettingsInfo.getIntegrations())
        .thenReturn(
            new GlobalIntegrationSettings()
                .setSlackSettings(new SlackIntegrationSettings().setEnabled(false)));

    List<NotificationRecipient> recipients =
        builder.buildGlobalRecipients(opContext, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertTrue(recipients.isEmpty());
  }

  @Test
  public void testBuildGlobalRecipientsWithEnabledSlack() {
    GlobalNotificationSettings globalNotificationSettings = mock(GlobalNotificationSettings.class);
    when(globalNotificationSettings.hasSettings()).thenReturn(true);
    when(globalNotificationSettings.getSettings())
        .thenReturn(
            new NotificationSettingMap(
                Collections.singletonMap(
                    "ASSERTION_STATUS_CHANGE",
                    new NotificationSetting()
                        .setValue(NotificationSettingValue.ENABLED)
                        .setParams(
                            new StringMap(
                                ImmutableMap.of(
                                    "slack.enabled", "true",
                                    "slack.channel", "test-channel"))))));

    when(globalSettingsInfo.hasNotifications()).thenReturn(true);
    when(globalSettingsInfo.getNotifications()).thenReturn(globalNotificationSettings);
    when(globalSettingsInfo.hasIntegrations()).thenReturn(true);
    when(globalSettingsInfo.getIntegrations())
        .thenReturn(
            new GlobalIntegrationSettings()
                .setSlackSettings(new SlackIntegrationSettings().setEnabled(true)));

    List<NotificationRecipient> recipients =
        builder.buildGlobalRecipients(opContext, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "test-channel");
    Assert.assertEquals(recipients.get(0).getType(), NotificationRecipientType.SLACK_CHANNEL);
  }

  @Test
  public void testBuildGlobalRecipientsWithEmptySettings() {
    when(settingsService.getGlobalSettings(opContext)).thenReturn(null);

    List<NotificationRecipient> recipients =
        builder.buildGlobalRecipients(opContext, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 0);

    // No notification settings
    when(settingsService.getGlobalSettings(opContext)).thenReturn(new GlobalSettingsInfo());

    recipients =
        builder.buildGlobalRecipients(opContext, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 0);

    // No slack settings
    when(settingsService.getGlobalSettings(opContext))
        .thenReturn(new GlobalSettingsInfo().setNotifications(new GlobalNotificationSettings()));

    recipients =
        builder.buildGlobalRecipients(opContext, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 0);
  }

  @Test
  public void testBuildGlobalRecipientsWithDefaultChannel() {
    GlobalNotificationSettings globalNotificationSettings = mock(GlobalNotificationSettings.class);
    when(globalNotificationSettings.hasSettings()).thenReturn(true);
    when(globalNotificationSettings.getSettings())
        .thenReturn(
            new NotificationSettingMap(
                Collections.singletonMap(
                    "ASSERTION_STATUS_CHANGE",
                    new NotificationSetting()
                        .setValue(NotificationSettingValue.ENABLED)
                        .setParams(new StringMap(ImmutableMap.of("slack.enabled", "true"))))));

    when(globalSettingsInfo.hasNotifications()).thenReturn(true);
    when(globalSettingsInfo.hasIntegrations()).thenReturn(true);
    when(globalSettingsInfo.getNotifications()).thenReturn(globalNotificationSettings);
    when(globalSettingsInfo.getIntegrations())
        .thenReturn(
            new GlobalIntegrationSettings()
                .setSlackSettings(
                    new SlackIntegrationSettings()
                        .setEnabled(true)
                        .setDefaultChannelName("default-channel")));

    List<NotificationRecipient> recipients =
        builder.buildGlobalRecipients(opContext, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "default-channel");
    Assert.assertEquals(recipients.get(0).getType(), NotificationRecipientType.SLACK_CHANNEL);
  }

  @Test
  public void testBuildGlobalRecipientsWithNoSettings() {
    when(globalSettingsInfo.hasNotifications()).thenReturn(false);

    List<NotificationRecipient> recipients =
        builder.buildGlobalRecipients(opContext, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertTrue(recipients.isEmpty());
  }

  @Test
  public void testBuildGlobalRecipientsWithNoChannel() {
    GlobalNotificationSettings globalNotificationSettings = mock(GlobalNotificationSettings.class);
    when(globalNotificationSettings.hasSettings()).thenReturn(true);
    when(globalNotificationSettings.getSettings())
        .thenReturn(
            new NotificationSettingMap(
                Collections.singletonMap(
                    "ASSERTION_STATUS_CHANGE",
                    new NotificationSetting()
                        .setValue(NotificationSettingValue.ENABLED)
                        .setParams(new StringMap(ImmutableMap.of("slack.enabled", "true"))))));

    when(globalSettingsInfo.hasNotifications()).thenReturn(true);
    when(globalSettingsInfo.getNotifications()).thenReturn(globalNotificationSettings);
    when(globalSettingsInfo.hasIntegrations()).thenReturn(true);
    when(globalSettingsInfo.getIntegrations())
        .thenReturn(
            new GlobalIntegrationSettings()
                .setSlackSettings(new SlackIntegrationSettings().setEnabled(true)));

    List<NotificationRecipient> recipients =
        builder.buildGlobalRecipients(opContext, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertTrue(recipients.isEmpty());
  }

  @Test
  public void testBuildUserSubscriberRecipientsWithSlackSettings() throws Exception {
    Map<Urn, SubscriptionInfo> userToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> userToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpuser:test");

    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    when(subscriptionInfo.hasNotificationConfig()).thenReturn(false);
    userToSubscriptionMap.put(testUrn, subscriptionInfo);

    NotificationSettings settings = mock(NotificationSettings.class);
    userToNotificationSettings.put(testUrn, settings);

    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.SLACK)));
    when(settings.hasSlackSettings()).thenReturn(true);
    SlackNotificationSettings slackSettings = mock(SlackNotificationSettings.class);
    when(slackSettings.getUserHandle()).thenReturn("U123456");
    when(slackSettings.hasUserHandle()).thenReturn(true);
    when(settings.getSlackSettings()).thenReturn(slackSettings);

    List<NotificationRecipient> recipients =
        builder.buildUserSubscriberRecipients(userToSubscriptionMap, userToNotificationSettings);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "U123456");
    Assert.assertEquals(recipients.get(0).getType(), NotificationRecipientType.SLACK_DM);
    Assert.assertEquals(recipients.get(0).getActor(), testUrn);
  }

  @Test
  public void testBuildUserSubscriberRecipientsWithNoSlackSettings() throws Exception {
    Map<Urn, SubscriptionInfo> userToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> userToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpuser:test");

    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    when(subscriptionInfo.hasNotificationConfig()).thenReturn(false);
    userToSubscriptionMap.put(testUrn, subscriptionInfo);

    NotificationSettings settings = mock(NotificationSettings.class);
    when(settings.hasSlackSettings()).thenReturn(false);
    when(settings.getSinkTypes()).thenReturn(new NotificationSinkTypeArray());
    userToNotificationSettings.put(testUrn, settings);

    List<NotificationRecipient> recipients =
        builder.buildUserSubscriberRecipients(userToSubscriptionMap, userToNotificationSettings);

    Assert.assertTrue(recipients.isEmpty());
  }

  @Test
  public void testBuildUserSubscriberRecipientsWithSubscriptionHandle() throws Exception {
    Map<Urn, SubscriptionInfo> userToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> userToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpuser:test");

    // Setup subscription with Slack handle
    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    SubscriptionNotificationConfig notificationConfig = mock(SubscriptionNotificationConfig.class);
    NotificationSettings subscriptionSettings = mock(NotificationSettings.class);
    SlackNotificationSettings subscriptionSlackSettings = mock(SlackNotificationSettings.class);

    when(subscriptionInfo.hasNotificationConfig()).thenReturn(true);
    when(subscriptionInfo.getNotificationConfig()).thenReturn(notificationConfig);
    when(notificationConfig.hasNotificationSettings()).thenReturn(true);
    when(notificationConfig.getNotificationSettings()).thenReturn(subscriptionSettings);
    when(subscriptionSettings.hasSlackSettings()).thenReturn(true);
    when(subscriptionSettings.getSlackSettings()).thenReturn(subscriptionSlackSettings);
    when(subscriptionSlackSettings.hasUserHandle()).thenReturn(true);
    when(subscriptionSlackSettings.getUserHandle()).thenReturn("U789012");

    userToSubscriptionMap.put(testUrn, subscriptionInfo);

    // Setup user notification settings
    NotificationSettings settings = mock(NotificationSettings.class);
    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.SLACK)));
    userToNotificationSettings.put(testUrn, settings);

    List<NotificationRecipient> recipients =
        builder.buildUserSubscriberRecipients(userToSubscriptionMap, userToNotificationSettings);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "U789012");
    Assert.assertEquals(recipients.get(0).getType(), NotificationRecipientType.SLACK_DM);
    Assert.assertEquals(recipients.get(0).getActor(), testUrn);
  }

  @Test
  public void testBuildGroupSubscriberRecipientsWithSlackSettings() throws Exception {
    Map<Urn, SubscriptionInfo> groupToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> groupToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpGroup:test");

    // Setup basic subscription info
    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    when(subscriptionInfo.hasNotificationConfig()).thenReturn(false);
    groupToSubscriptionMap.put(testUrn, subscriptionInfo);

    // Setup notification settings with multiple channels
    NotificationSettings settings = mock(NotificationSettings.class);
    groupToNotificationSettings.put(testUrn, settings);

    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.SLACK)));
    when(settings.hasSlackSettings()).thenReturn(true);

    SlackNotificationSettings slackSettings = mock(SlackNotificationSettings.class);
    when(slackSettings.hasChannels()).thenReturn(true);
    when(slackSettings.getChannels())
        .thenReturn(new StringArray(Arrays.asList("engineering-alerts", "data-team")));
    when(settings.getSlackSettings()).thenReturn(slackSettings);

    List<NotificationRecipient> recipients =
        builder.buildGroupSubscriberRecipients(groupToSubscriptionMap, groupToNotificationSettings);

    Assert.assertEquals(recipients.size(), 2);
    Assert.assertEquals(recipients.get(0).getId(), "engineering-alerts");
    Assert.assertEquals(recipients.get(0).getType(), NotificationRecipientType.SLACK_CHANNEL);
    Assert.assertEquals(recipients.get(1).getId(), "data-team");
    Assert.assertEquals(recipients.get(1).getType(), NotificationRecipientType.SLACK_CHANNEL);
  }

  @Test
  public void testBuildGroupSubscriberRecipientsWithSubscriptionChannels() throws Exception {
    Map<Urn, SubscriptionInfo> groupToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> groupToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpGroup:test");

    // Setup subscription with explicit channels in subscription config
    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    SubscriptionNotificationConfig notificationConfig = mock(SubscriptionNotificationConfig.class);
    NotificationSettings subscriptionSettings = mock(NotificationSettings.class);
    SlackNotificationSettings subscriptionSlackSettings = mock(SlackNotificationSettings.class);

    when(subscriptionInfo.hasNotificationConfig()).thenReturn(true);
    when(subscriptionInfo.getNotificationConfig()).thenReturn(notificationConfig);
    when(notificationConfig.hasNotificationSettings()).thenReturn(true);
    when(notificationConfig.getNotificationSettings()).thenReturn(subscriptionSettings);
    when(subscriptionSettings.hasSlackSettings()).thenReturn(true);
    when(subscriptionSettings.getSlackSettings()).thenReturn(subscriptionSlackSettings);
    when(subscriptionSlackSettings.hasChannels()).thenReturn(true);
    when(subscriptionSlackSettings.getChannels())
        .thenReturn(
            new StringArray(Arrays.asList("subscription-channel-1", "subscription-channel-2")));

    groupToSubscriptionMap.put(testUrn, subscriptionInfo);

    // Setup basic notification settings
    NotificationSettings settings = mock(NotificationSettings.class);
    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.SLACK)));
    groupToNotificationSettings.put(testUrn, settings);

    List<NotificationRecipient> recipients =
        builder.buildGroupSubscriberRecipients(groupToSubscriptionMap, groupToNotificationSettings);

    Assert.assertEquals(recipients.size(), 2);
    Assert.assertEquals(recipients.get(0).getId(), "subscription-channel-1");
    Assert.assertEquals(recipients.get(0).getType(), NotificationRecipientType.SLACK_CHANNEL);
    Assert.assertEquals(recipients.get(1).getId(), "subscription-channel-2");
    Assert.assertEquals(recipients.get(1).getType(), NotificationRecipientType.SLACK_CHANNEL);
  }

  @Test
  public void testBuildGroupSubscriberRecipientsWithNoSlackSettings() throws Exception {
    Map<Urn, SubscriptionInfo> groupToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> groupToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpGroup:test");

    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    when(subscriptionInfo.hasNotificationConfig()).thenReturn(false);
    groupToSubscriptionMap.put(testUrn, subscriptionInfo);

    NotificationSettings settings = mock(NotificationSettings.class);
    when(settings.hasSlackSettings()).thenReturn(false);
    when(settings.getSinkTypes()).thenReturn(new NotificationSinkTypeArray());
    groupToNotificationSettings.put(testUrn, settings);

    List<NotificationRecipient> recipients =
        builder.buildGroupSubscriberRecipients(groupToSubscriptionMap, groupToNotificationSettings);

    Assert.assertTrue(recipients.isEmpty());
  }

  @Test
  public void testBuildGroupSubscriberRecipientsWithNoChannels() throws Exception {
    Map<Urn, SubscriptionInfo> groupToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> groupToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpGroup:test");

    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    when(subscriptionInfo.hasNotificationConfig()).thenReturn(false);
    groupToSubscriptionMap.put(testUrn, subscriptionInfo);

    NotificationSettings settings = mock(NotificationSettings.class);
    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.SLACK)));
    when(settings.hasSlackSettings()).thenReturn(true);

    SlackNotificationSettings slackSettings = mock(SlackNotificationSettings.class);
    when(slackSettings.hasChannels()).thenReturn(false);
    when(settings.getSlackSettings()).thenReturn(slackSettings);

    groupToNotificationSettings.put(testUrn, settings);

    List<NotificationRecipient> recipients =
        builder.buildGroupSubscriberRecipients(groupToSubscriptionMap, groupToNotificationSettings);

    Assert.assertTrue(recipients.isEmpty());
  }

  @Test
  public void testBuildGroupSubscriberRecipientsWithSlackDisabled() throws Exception {
    Map<Urn, SubscriptionInfo> groupToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> groupToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpGroup:test");

    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    when(subscriptionInfo.hasNotificationConfig()).thenReturn(false);
    groupToSubscriptionMap.put(testUrn, subscriptionInfo);

    NotificationSettings settings = mock(NotificationSettings.class);
    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.emptyList())); // Slack not in sink types
    groupToNotificationSettings.put(testUrn, settings);

    List<NotificationRecipient> recipients =
        builder.buildGroupSubscriberRecipients(groupToSubscriptionMap, groupToNotificationSettings);

    Assert.assertTrue(recipients.isEmpty());
  }

  @Test
  public void testBuildGroupSubscriberRecipientsWithMultipleGroups() throws Exception {
    Map<Urn, SubscriptionInfo> groupToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> groupToNotificationSettings = new HashMap<>();

    Urn group1Urn = Urn.createFromString("urn:li:corpGroup:group1");
    Urn group2Urn = Urn.createFromString("urn:li:corpGroup:group2");

    // Setup group 1
    SubscriptionInfo subscriptionInfo1 = mock(SubscriptionInfo.class);
    when(subscriptionInfo1.hasNotificationConfig()).thenReturn(false);
    groupToSubscriptionMap.put(group1Urn, subscriptionInfo1);

    NotificationSettings settings1 = mock(NotificationSettings.class);
    when(settings1.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.SLACK)));
    when(settings1.hasSlackSettings()).thenReturn(true);

    SlackNotificationSettings slackSettings1 = mock(SlackNotificationSettings.class);
    when(slackSettings1.hasChannels()).thenReturn(true);
    when(slackSettings1.getChannels())
        .thenReturn(new StringArray(Arrays.asList("group1-channel1", "group1-channel2")));
    when(settings1.getSlackSettings()).thenReturn(slackSettings1);

    groupToNotificationSettings.put(group1Urn, settings1);

    // Setup group 2
    SubscriptionInfo subscriptionInfo2 = mock(SubscriptionInfo.class);
    when(subscriptionInfo2.hasNotificationConfig()).thenReturn(false);
    groupToSubscriptionMap.put(group2Urn, subscriptionInfo2);

    NotificationSettings settings2 = mock(NotificationSettings.class);
    when(settings2.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.SLACK)));
    when(settings2.hasSlackSettings()).thenReturn(true);

    SlackNotificationSettings slackSettings2 = mock(SlackNotificationSettings.class);
    when(slackSettings2.hasChannels()).thenReturn(true);
    when(slackSettings2.getChannels())
        .thenReturn(new StringArray(Collections.singletonList("group2-channel")));
    when(settings2.getSlackSettings()).thenReturn(slackSettings2);

    groupToNotificationSettings.put(group2Urn, settings2);

    List<NotificationRecipient> recipients =
        builder.buildGroupSubscriberRecipients(groupToSubscriptionMap, groupToNotificationSettings);

    Assert.assertEquals(recipients.size(), 3);
    Set<String> ids =
        Set.of(recipients.get(0).getId(), recipients.get(1).getId(), recipients.get(2).getId());
    Assert.assertTrue(ids.contains("group1-channel1"));
    Assert.assertTrue(ids.contains("group1-channel2"));
    Assert.assertTrue(ids.contains("group2-channel"));

    recipients.forEach(
        recipient ->
            Assert.assertEquals(recipient.getType(), NotificationRecipientType.SLACK_CHANNEL));
  }

  @Test
  public void testBuildActorRecipientsWithUserUrns() throws Exception {
    // Setup user URNs
    Urn userUrn1 = Urn.createFromString("urn:li:corpuser:user1");
    Urn userUrn2 = Urn.createFromString("urn:li:corpuser:user2");
    List<Urn> actorUrns = Arrays.asList(userUrn1, userUrn2);

    // Setup mock user settings
    CorpUserSettings userSettings1 = mock(CorpUserSettings.class);
    CorpUserSettings userSettings2 = mock(CorpUserSettings.class);
    Map<Urn, CorpUserSettings> userToSettings = new HashMap<>();
    userToSettings.put(userUrn1, userSettings1);
    userToSettings.put(userUrn2, userSettings2);

    when(settingsService.batchGetCorpUserSettings(opContext, Arrays.asList(userUrn1, userUrn2)))
        .thenReturn(userToSettings);

    // Setup notification settings for users
    NotificationSettings notificationSettings1 =
        createSlackNotificationSettings(
            "U123456", // Slack user handle
            Collections.emptyList(), // No channels for user
            NotificationScenarioType.ASSERTION_STATUS_CHANGE,
            true);
    NotificationSettings notificationSettings2 =
        createSlackNotificationSettings(
            "U789012",
            Collections.emptyList(),
            NotificationScenarioType.ASSERTION_STATUS_CHANGE,
            true);

    when(userSettings1.hasNotificationSettings()).thenReturn(true);
    when(userSettings1.getNotificationSettings()).thenReturn(notificationSettings1);
    when(userSettings2.hasNotificationSettings()).thenReturn(true);
    when(userSettings2.getNotificationSettings()).thenReturn(notificationSettings2);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 2);
    Assert.assertEquals(recipients.get(0).getId(), "U123456");
    Assert.assertEquals(recipients.get(0).getType(), NotificationRecipientType.SLACK_DM);
    Assert.assertEquals(recipients.get(0).getActor(), userUrn1);
    Assert.assertEquals(recipients.get(1).getId(), "U789012");
    Assert.assertEquals(recipients.get(1).getType(), NotificationRecipientType.SLACK_DM);
    Assert.assertEquals(recipients.get(1).getActor(), userUrn2);
  }

  @Test
  public void testBuildActorRecipientsWithGroupUrns() throws Exception {
    // Setup group URNs
    Urn groupUrn1 = Urn.createFromString("urn:li:corpGroup:group1");
    Urn groupUrn2 = Urn.createFromString("urn:li:corpGroup:group2");
    List<Urn> actorUrns = Arrays.asList(groupUrn1, groupUrn2);

    // Setup mock group settings
    CorpGroupSettings groupSettings1 = mock(CorpGroupSettings.class);
    CorpGroupSettings groupSettings2 = mock(CorpGroupSettings.class);
    Map<Urn, CorpGroupSettings> groupToSettings = new HashMap<>();
    groupToSettings.put(groupUrn1, groupSettings1);
    groupToSettings.put(groupUrn2, groupSettings2);

    when(settingsService.batchGetCorpGroupSettings(opContext, Arrays.asList(groupUrn1, groupUrn2)))
        .thenReturn(groupToSettings);

    // Setup notification settings for groups with channels
    NotificationSettings notificationSettings1 =
        createSlackNotificationSettings(
            null, // No user handle for group
            Arrays.asList("channel1", "channel2"),
            NotificationScenarioType.ASSERTION_STATUS_CHANGE,
            true);
    NotificationSettings notificationSettings2 =
        createSlackNotificationSettings(
            null,
            Arrays.asList("channel3"),
            NotificationScenarioType.ASSERTION_STATUS_CHANGE,
            true);

    when(groupSettings1.hasNotificationSettings()).thenReturn(true);
    when(groupSettings1.getNotificationSettings()).thenReturn(notificationSettings1);
    when(groupSettings2.hasNotificationSettings()).thenReturn(true);
    when(groupSettings2.getNotificationSettings()).thenReturn(notificationSettings2);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 3); // 2 channels for group1 + 1 channel for group2
    Assert.assertEquals(recipients.get(0).getId(), "channel1");
    Assert.assertEquals(recipients.get(0).getType(), NotificationRecipientType.SLACK_CHANNEL);
    Assert.assertEquals(recipients.get(0).getActor(), groupUrn1);
    Assert.assertEquals(recipients.get(1).getId(), "channel2");
    Assert.assertEquals(recipients.get(1).getType(), NotificationRecipientType.SLACK_CHANNEL);
    Assert.assertEquals(recipients.get(1).getActor(), groupUrn1);
    Assert.assertEquals(recipients.get(2).getId(), "channel3");
    Assert.assertEquals(recipients.get(2).getType(), NotificationRecipientType.SLACK_CHANNEL);
    Assert.assertEquals(recipients.get(2).getActor(), groupUrn2);
  }

  @Test
  public void testBuildActorRecipientsWithMixedUrns() throws Exception {
    // Setup mixed URNs
    Urn userUrn = Urn.createFromString("urn:li:corpuser:user1");
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:group1");
    List<Urn> actorUrns = Arrays.asList(userUrn, groupUrn);

    // Setup mock settings
    CorpUserSettings userSettings = mock(CorpUserSettings.class);
    CorpGroupSettings groupSettings = mock(CorpGroupSettings.class);

    Map<Urn, CorpUserSettings> userToSettings = Collections.singletonMap(userUrn, userSettings);
    Map<Urn, CorpGroupSettings> groupToSettings = Collections.singletonMap(groupUrn, groupSettings);

    when(settingsService.batchGetCorpUserSettings(opContext, Collections.singletonList(userUrn)))
        .thenReturn(userToSettings);
    when(settingsService.batchGetCorpGroupSettings(opContext, Collections.singletonList(groupUrn)))
        .thenReturn(groupToSettings);

    // Setup notification settings
    NotificationSettings userNotificationSettings =
        createSlackNotificationSettings(
            "U123456",
            Collections.emptyList(),
            NotificationScenarioType.ASSERTION_STATUS_CHANGE,
            true);
    NotificationSettings groupNotificationSettings =
        createSlackNotificationSettings(
            null,
            Collections.singletonList("channel1"),
            NotificationScenarioType.ASSERTION_STATUS_CHANGE,
            true);

    when(userSettings.hasNotificationSettings()).thenReturn(true);
    when(userSettings.getNotificationSettings()).thenReturn(userNotificationSettings);
    when(groupSettings.hasNotificationSettings()).thenReturn(true);
    when(groupSettings.getNotificationSettings()).thenReturn(groupNotificationSettings);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 2);
    // Check user DM recipient
    Assert.assertEquals(recipients.get(0).getId(), "U123456");
    Assert.assertEquals(recipients.get(0).getType(), NotificationRecipientType.SLACK_DM);
    Assert.assertEquals(recipients.get(0).getActor(), userUrn);
    // Check group channel recipient
    Assert.assertEquals(recipients.get(1).getId(), "channel1");
    Assert.assertEquals(recipients.get(1).getType(), NotificationRecipientType.SLACK_CHANNEL);
    Assert.assertEquals(recipients.get(1).getActor(), groupUrn);
  }

  @Test
  public void testBuildActorRecipientsWithNoSettings() throws Exception {
    Urn userUrn = Urn.createFromString("urn:li:corpuser:user1");
    List<Urn> actorUrns = Collections.singletonList(userUrn);

    CorpUserSettings userSettings = mock(CorpUserSettings.class);
    when(userSettings.hasNotificationSettings()).thenReturn(false);

    Map<Urn, CorpUserSettings> userToSettings = Collections.singletonMap(userUrn, userSettings);
    when(settingsService.batchGetCorpUserSettings(opContext, actorUrns)).thenReturn(userToSettings);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertTrue(recipients.isEmpty());
  }

  @Test
  public void testBuildActorRecipientsWithScenarioSpecificSettings() throws Exception {
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:group1");
    List<Urn> actorUrns = Collections.singletonList(groupUrn);

    // Setup notification settings with scenario-specific channel
    NotificationSettings settings = new NotificationSettings();
    NotificationSetting scenarioSetting =
        new NotificationSetting()
            .setValue(NotificationSettingValue.ENABLED)
            .setParams(
                new StringMap(
                    ImmutableMap.of(
                        "slack.enabled", "true",
                        "slack.channel", "scenario-specific-channel")));

    settings.setSettings(
        new NotificationSettingMap(
            Collections.singletonMap(
                NotificationScenarioType.ASSERTION_STATUS_CHANGE.toString(), scenarioSetting)));
    settings.setSinkTypes(
        new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.SLACK)));

    // Setup group settings
    CorpGroupSettings groupSettings = mock(CorpGroupSettings.class);
    when(groupSettings.hasNotificationSettings()).thenReturn(true);
    when(groupSettings.getNotificationSettings()).thenReturn(settings);

    Map<Urn, CorpGroupSettings> groupToSettings = Collections.singletonMap(groupUrn, groupSettings);
    when(settingsService.batchGetCorpGroupSettings(opContext, actorUrns))
        .thenReturn(groupToSettings);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "scenario-specific-channel");
    Assert.assertEquals(recipients.get(0).getType(), NotificationRecipientType.SLACK_CHANNEL);
    Assert.assertEquals(recipients.get(0).getActor(), groupUrn);
  }

  @Test
  public void testBuildActorRecipientsWithDisabledScenario() throws Exception {
    Urn groupUrn = Urn.createFromString("urn:li:corpGroup:group1");
    List<Urn> actorUrns = Collections.singletonList(groupUrn);

    // Setup notification settings with disabled scenario
    NotificationSettings settings = mock(NotificationSettings.class);
    NotificationSetting scenarioSetting =
        new NotificationSetting()
            .setValue(NotificationSettingValue.ENABLED)
            .setParams(
                new StringMap(
                    ImmutableMap.of(
                        "slack.enabled", "false",
                        "slack.channel", "scenario-specific-channel")));

    when(settings.getSettings())
        .thenReturn(
            new NotificationSettingMap(
                Collections.singletonMap(
                    NotificationScenarioType.ASSERTION_STATUS_CHANGE.toString(), scenarioSetting)));
    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.SLACK)));

    // Setup group settings
    CorpGroupSettings groupSettings = mock(CorpGroupSettings.class);
    when(groupSettings.hasNotificationSettings()).thenReturn(true);
    when(groupSettings.getNotificationSettings()).thenReturn(settings);

    Map<Urn, CorpGroupSettings> groupToSettings = Collections.singletonMap(groupUrn, groupSettings);
    when(settingsService.batchGetCorpGroupSettings(opContext, actorUrns))
        .thenReturn(groupToSettings);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertTrue(recipients.isEmpty());
  }

  // Helper method to create slack notification settings
  private NotificationSettings createSlackNotificationSettings(
      @Nullable String userHandle,
      @Nonnull List<String> channels,
      @Nonnull NotificationScenarioType type,
      boolean enabled) {
    NotificationSettings settings = mock(NotificationSettings.class);
    SlackNotificationSettings slackSettings = mock(SlackNotificationSettings.class);

    // Set up user handle if provided
    when(slackSettings.hasUserHandle()).thenReturn(userHandle != null);
    when(slackSettings.getUserHandle()).thenReturn(userHandle);

    // Set up channels
    when(slackSettings.hasChannels()).thenReturn(!channels.isEmpty());
    when(slackSettings.getChannels()).thenReturn(new StringArray(channels));

    when(settings.hasSlackSettings()).thenReturn(true);
    when(settings.getSlackSettings()).thenReturn(slackSettings);
    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.SLACK)));

    NotificationSetting scenarioSetting =
        new NotificationSetting()
            .setValue(NotificationSettingValue.ENABLED)
            .setParams(
                new StringMap(Collections.singletonMap("slack.enabled", String.valueOf(enabled))));

    when(settings.hasSettings()).thenReturn(true);
    when(settings.getSettings())
        .thenReturn(
            new NotificationSettingMap(Collections.singletonMap(type.toString(), scenarioSetting)));

    return settings;
  }
}
