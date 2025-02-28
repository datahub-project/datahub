package com.datahub.notification.recipient;

import static org.mockito.Mockito.*;

import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.NotificationSettings;
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
  @Mock private SettingsProvider settingsProvider;
  @Mock private GlobalSettingsInfo globalSettingsInfo;

  private EmailNotificationRecipientBuilder builder;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    MockitoAnnotations.openMocks(this);
    when(settingsProvider.getGlobalSettings(opContext)).thenReturn(globalSettingsInfo);
    builder = new EmailNotificationRecipientBuilder(settingsProvider);
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
}
