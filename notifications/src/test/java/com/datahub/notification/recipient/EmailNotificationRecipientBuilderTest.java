package com.datahub.notification.recipient;

import static org.mockito.Mockito.*;

import com.datahub.notification.NotificationScenarioType;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.NotificationSettingValue;
import com.linkedin.settings.global.EmailIntegrationSettings;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EmailNotificationRecipientBuilderTest {
  @Mock private SettingsService settingsService;
  @Mock private GlobalSettingsInfo globalSettingsInfo;

  private EmailNotificationRecipientBuilder builder;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    MockitoAnnotations.openMocks(this);
    when(settingsService.getGlobalSettings(opContext)).thenReturn(globalSettingsInfo);
    builder = new EmailNotificationRecipientBuilder(settingsService);
  }

  @Test
  public void testBuildGlobalRecipientsWithEnabledEmail() {
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
                                    "email.enabled",
                                    "true",
                                    "email.address",
                                    "test@example.com"))))));
    when(globalSettingsInfo.hasNotifications()).thenReturn(true);
    when(globalSettingsInfo.getNotifications()).thenReturn(globalNotificationSettings);

    List<NotificationRecipient> recipients =
        builder.buildGlobalRecipients(opContext, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "test@example.com");
  }

  @Test
  public void testBuildGlobalRecipientsWithDefaultEmail() {
    GlobalNotificationSettings globalNotificationSettings = mock(GlobalNotificationSettings.class);
    when(globalNotificationSettings.hasSettings()).thenReturn(true);
    // No explicit email override.
    when(globalNotificationSettings.getSettings())
        .thenReturn(
            new NotificationSettingMap(
                Collections.singletonMap(
                    "ASSERTION_STATUS_CHANGE",
                    new NotificationSetting()
                        .setValue(NotificationSettingValue.ENABLED)
                        .setParams(new StringMap(ImmutableMap.of("email.enabled", "true"))))));
    when(globalSettingsInfo.hasNotifications()).thenReturn(true);
    when(globalSettingsInfo.hasIntegrations()).thenReturn(true);
    when(globalSettingsInfo.getNotifications()).thenReturn(globalNotificationSettings);
    when(globalSettingsInfo.getIntegrations())
        .thenReturn(
            new GlobalIntegrationSettings()
                .setEmailSettings(new EmailIntegrationSettings().setDefaultEmail("test@test.com")));

    List<NotificationRecipient> recipients =
        builder.buildGlobalRecipients(opContext, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "test@test.com");
  }

  @Test
  public void testBuildUserSubscriberRecipientsWithEmailSettings() throws Exception {
    Map<Urn, SubscriptionInfo> userToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> userToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpuser:test");

    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    when(subscriptionInfo.hasNotificationConfig()).thenReturn(false);
    when(subscriptionInfo.getNotificationConfig()).thenReturn(null);
    userToSubscriptionMap.put(testUrn, subscriptionInfo);

    NotificationSettings settings = mock(NotificationSettings.class);
    userToNotificationSettings.put(testUrn, settings);

    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.EMAIL)));
    when(settings.hasEmailSettings()).thenReturn(true);
    EmailNotificationSettings globalEmailSettings = mock(EmailNotificationSettings.class);
    when(globalEmailSettings.getEmail()).thenReturn("user@example.com");
    when(globalEmailSettings.hasEmail()).thenReturn(true);
    when(settings.getEmailSettings()).thenReturn(globalEmailSettings);

    List<NotificationRecipient> recipients =
        builder.buildUserSubscriberRecipients(userToSubscriptionMap, userToNotificationSettings);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "user@example.com");
    Assert.assertEquals(recipients.get(0).getActor(), testUrn);
  }

  @Test
  public void testBuildUserSubscriberRecipientsWithSubscriptionEmail() throws Exception {
    Map<Urn, SubscriptionInfo> userToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> userToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpuser:test");
    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    NotificationSettings settings = mock(NotificationSettings.class);
    EmailNotificationSettings emailSettings = mock(EmailNotificationSettings.class);

    // Simulate subscriptionInfo having notification config with an email setting
    SubscriptionNotificationConfig notificationConfig = mock(SubscriptionNotificationConfig.class);
    NotificationSettings subscriptionNotificationSettings = mock(NotificationSettings.class);

    userToSubscriptionMap.put(testUrn, subscriptionInfo);
    userToNotificationSettings.put(testUrn, settings);

    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.EMAIL)));
    when(subscriptionInfo.hasNotificationConfig()).thenReturn(true);
    when(subscriptionInfo.getNotificationConfig()).thenReturn(notificationConfig);
    when(notificationConfig.hasNotificationSettings()).thenReturn(true);
    when(notificationConfig.getNotificationSettings()).thenReturn(subscriptionNotificationSettings);
    when(subscriptionNotificationSettings.hasEmailSettings()).thenReturn(true);
    when(subscriptionNotificationSettings.getEmailSettings()).thenReturn(emailSettings);
    when(emailSettings.hasEmail()).thenReturn(true);
    when(emailSettings.getEmail()).thenReturn("user@example.com");

    List<NotificationRecipient> recipients =
        builder.buildUserSubscriberRecipients(userToSubscriptionMap, userToNotificationSettings);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "user@example.com");
    Assert.assertEquals(recipients.get(0).getActor(), testUrn);
  }

  @Test
  public void testBuildGroupSubscriberRecipientsWithEmailSettings() throws Exception {
    Map<Urn, SubscriptionInfo> groupToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> groupToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpGroup:test");

    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    when(subscriptionInfo.hasNotificationConfig()).thenReturn(false);
    when(subscriptionInfo.getNotificationConfig()).thenReturn(null);
    groupToSubscriptionMap.put(testUrn, subscriptionInfo);

    NotificationSettings settings = mock(NotificationSettings.class);
    groupToNotificationSettings.put(testUrn, settings);

    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.EMAIL)));
    when(settings.hasEmailSettings()).thenReturn(true);
    EmailNotificationSettings globalEmailSettings = mock(EmailNotificationSettings.class);
    when(globalEmailSettings.getEmail()).thenReturn("group@example.com");
    when(globalEmailSettings.hasEmail()).thenReturn(true);
    when(settings.getEmailSettings()).thenReturn(globalEmailSettings);

    List<NotificationRecipient> recipients =
        builder.buildGroupSubscriberRecipients(groupToSubscriptionMap, groupToNotificationSettings);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "group@example.com");
    Assert.assertEquals(recipients.get(0).getActor(), testUrn);
  }

  @Test
  public void testBuildGroupSubscriberRecipientsWithSubscriptionEmail() throws Exception {
    Map<Urn, SubscriptionInfo> groupToSubscriptionMap = new HashMap<>();
    Map<Urn, NotificationSettings> groupToNotificationSettings = new HashMap<>();
    Urn testUrn = Urn.createFromString("urn:li:corpGroup:test");
    SubscriptionInfo subscriptionInfo = mock(SubscriptionInfo.class);
    NotificationSettings settings = mock(NotificationSettings.class);
    EmailNotificationSettings emailSettings = mock(EmailNotificationSettings.class);

    // Simulate subscriptionInfo having notification config with an email setting
    SubscriptionNotificationConfig notificationConfig = mock(SubscriptionNotificationConfig.class);
    NotificationSettings subscriptionNotificationSettings = mock(NotificationSettings.class);

    groupToSubscriptionMap.put(testUrn, subscriptionInfo);
    groupToNotificationSettings.put(testUrn, settings);

    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.EMAIL)));
    when(subscriptionInfo.hasNotificationConfig()).thenReturn(true);
    when(subscriptionInfo.getNotificationConfig()).thenReturn(notificationConfig);
    when(notificationConfig.hasNotificationSettings()).thenReturn(true);
    when(notificationConfig.getNotificationSettings()).thenReturn(subscriptionNotificationSettings);
    when(subscriptionNotificationSettings.hasEmailSettings()).thenReturn(true);
    when(subscriptionNotificationSettings.getEmailSettings()).thenReturn(emailSettings);
    when(emailSettings.hasEmail()).thenReturn(true);
    when(emailSettings.getEmail()).thenReturn("group@example.com");

    List<NotificationRecipient> recipients =
        builder.buildGroupSubscriberRecipients(groupToSubscriptionMap, groupToNotificationSettings);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "group@example.com");
    Assert.assertEquals(recipients.get(0).getActor(), testUrn);
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
        createNotificationSettings(
            "user1@example.com", NotificationScenarioType.ASSERTION_STATUS_CHANGE, true);
    NotificationSettings notificationSettings2 =
        createNotificationSettings(
            "user2@example.com", NotificationScenarioType.ASSERTION_STATUS_CHANGE, true);

    when(userSettings1.hasNotificationSettings()).thenReturn(true);
    when(userSettings1.getNotificationSettings()).thenReturn(notificationSettings1);
    when(userSettings2.hasNotificationSettings()).thenReturn(true);
    when(userSettings2.getNotificationSettings()).thenReturn(notificationSettings2);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 2);
    Assert.assertEquals(recipients.get(0).getId(), "user1@example.com");
    Assert.assertEquals(recipients.get(0).getActor(), userUrn1);
    Assert.assertEquals(recipients.get(1).getId(), "user2@example.com");
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

    // Setup notification settings for groups
    NotificationSettings notificationSettings1 =
        createNotificationSettings(
            "group1@example.com", NotificationScenarioType.ASSERTION_STATUS_CHANGE, true);
    NotificationSettings notificationSettings2 =
        createNotificationSettings(
            "group2@example.com", NotificationScenarioType.ASSERTION_STATUS_CHANGE, true);

    when(groupSettings1.hasNotificationSettings()).thenReturn(true);
    when(groupSettings1.getNotificationSettings()).thenReturn(notificationSettings1);
    when(groupSettings2.hasNotificationSettings()).thenReturn(true);
    when(groupSettings2.getNotificationSettings()).thenReturn(notificationSettings2);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 2);
    Assert.assertEquals(recipients.get(0).getId(), "group1@example.com");
    Assert.assertEquals(recipients.get(0).getActor(), groupUrn1);
    Assert.assertEquals(recipients.get(1).getId(), "group2@example.com");
    Assert.assertEquals(recipients.get(1).getActor(), groupUrn2);
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
        createNotificationSettings(
            "user@example.com", NotificationScenarioType.ASSERTION_STATUS_CHANGE, true);
    NotificationSettings groupNotificationSettings =
        createNotificationSettings(
            "group@example.com", NotificationScenarioType.ASSERTION_STATUS_CHANGE, true);

    when(userSettings.hasNotificationSettings()).thenReturn(true);
    when(userSettings.getNotificationSettings()).thenReturn(userNotificationSettings);
    when(groupSettings.hasNotificationSettings()).thenReturn(true);
    when(groupSettings.getNotificationSettings()).thenReturn(groupNotificationSettings);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 2);
    Assert.assertEquals(recipients.get(0).getId(), "user@example.com");
    Assert.assertEquals(recipients.get(0).getActor(), userUrn);
    Assert.assertEquals(recipients.get(1).getId(), "group@example.com");
    Assert.assertEquals(recipients.get(1).getActor(), groupUrn);
  }

  @Test
  public void testBuildActorRecipientsWithMixedUrnsNoSettingsEnabled() throws Exception {
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
        createNotificationSettings(
            "user@example.com", NotificationScenarioType.ASSERTION_STATUS_CHANGE, false);
    NotificationSettings groupNotificationSettings =
        createNotificationSettings(
            "group@example.com", NotificationScenarioType.ASSERTION_STATUS_CHANGE, false);

    when(userSettings.hasNotificationSettings()).thenReturn(true);
    when(userSettings.getNotificationSettings()).thenReturn(userNotificationSettings);
    when(groupSettings.hasNotificationSettings()).thenReturn(true);
    when(groupSettings.getNotificationSettings()).thenReturn(groupNotificationSettings);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 0);
  }

  @Test
  public void testBuildActorRecipientsWithNoNotificationSettings() throws Exception {
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
  public void testBuildActorRecipientsWithScenarioSpecificEmail() throws Exception {
    Urn userUrn = Urn.createFromString("urn:li:corpuser:user1");
    List<Urn> actorUrns = Collections.singletonList(userUrn);

    // Setup notification settings with scenario-specific email
    NotificationSettings settings = new NotificationSettings();
    NotificationSetting scenarioSetting =
        new NotificationSetting()
            .setValue(NotificationSettingValue.ENABLED)
            .setParams(
                new StringMap(
                    ImmutableMap.of(
                        "email.enabled", "true",
                        "email.address", "scenario@example.com")));

    settings.setScenarioSettings(
        new NotificationSettingMap(
            Collections.singletonMap(
                NotificationScenarioType.ASSERTION_STATUS_CHANGE.toString(), scenarioSetting)));
    settings.setSinkTypes(
        new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.EMAIL)));

    // Setup user settings
    CorpUserSettings userSettings = mock(CorpUserSettings.class);
    when(userSettings.hasNotificationSettings()).thenReturn(true);
    when(userSettings.getNotificationSettings()).thenReturn(settings);

    Map<Urn, CorpUserSettings> userToSettings = Collections.singletonMap(userUrn, userSettings);
    when(settingsService.batchGetCorpUserSettings(opContext, actorUrns)).thenReturn(userToSettings);

    List<NotificationRecipient> recipients =
        builder.buildActorRecipients(
            opContext, actorUrns, NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    Assert.assertEquals(recipients.size(), 1);
    Assert.assertEquals(recipients.get(0).getId(), "scenario@example.com");
    Assert.assertEquals(recipients.get(0).getActor(), userUrn);
  }

  // Helper method to create notification settings
  private NotificationSettings createNotificationSettings(
      String email, NotificationScenarioType type, boolean enabled) {
    NotificationSettings settings = mock(NotificationSettings.class);
    EmailNotificationSettings emailSettings = mock(EmailNotificationSettings.class);

    when(emailSettings.hasEmail()).thenReturn(true);
    when(emailSettings.getEmail()).thenReturn(email);

    when(settings.hasEmailSettings()).thenReturn(true);
    when(settings.getEmailSettings()).thenReturn(emailSettings);
    when(settings.getSinkTypes())
        .thenReturn(
            new NotificationSinkTypeArray(Collections.singleton(NotificationSinkType.EMAIL)));

    NotificationSetting scenarioSetting =
        new NotificationSetting()
            .setValue(NotificationSettingValue.ENABLED)
            .setParams(
                new StringMap(Collections.singletonMap("email.enabled", String.valueOf(enabled))));

    when(settings.hasScenarioSettings()).thenReturn(true);
    when(settings.getScenarioSettings())
        .thenReturn(
            new NotificationSettingMap(Collections.singletonMap(type.toString(), scenarioSetting)));

    return settings;
  }
}
