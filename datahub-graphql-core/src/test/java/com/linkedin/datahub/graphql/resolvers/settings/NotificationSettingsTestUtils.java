package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.metadata.service.SettingsService.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettings;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.identity.CorpUserSettings;
import java.util.ArrayList;
import java.util.List;

public class NotificationSettingsTestUtils {
  public static final String USER_URN_STRING = "urn:li:corpuser:testUser";
  public static final Urn USER_URN = UrnUtils.getUrn(USER_URN_STRING);
  public static final String GROUP_URN_STRING = "urn:li:corpGroup:testGroup";
  public static final Urn GROUP_URN = UrnUtils.getUrn(GROUP_URN_STRING);
  public static final String SLACK_USER_HANDLE = "testUser";
  public static final List<String> SLACK_CHANNELS =
      ImmutableList.of("testChannel1", "testChannel2");
  public static final String EMAIL_ADDRESS = "test@gmail.com";

  public static final com.linkedin.event.notification.settings.SlackNotificationSettings
      USER_SLACK_NOTIFICATION_SETTINGS =
          new com.linkedin.event.notification.settings.SlackNotificationSettings()
              .setUserHandle(SLACK_USER_HANDLE);

  public static final com.linkedin.event.notification.settings.EmailNotificationSettings
      USER_EMAIL_NOTIFICATION_SETTINGS =
          new com.linkedin.event.notification.settings.EmailNotificationSettings()
              .setEmail(EMAIL_ADDRESS);
  public static final com.linkedin.event.notification.settings.SlackNotificationSettings
      GROUP_SLACK_NOTIFICATION_SETTINGS =
          new com.linkedin.event.notification.settings.SlackNotificationSettings()
              .setChannels(new StringArray(SLACK_CHANNELS));

  public static final com.linkedin.event.notification.settings.EmailNotificationSettings
      GROUP_EMAIL_NOTIFICATION_SETTINGS =
          new com.linkedin.event.notification.settings.EmailNotificationSettings()
              .setEmail(EMAIL_ADDRESS);
  public static final com.linkedin.event.notification.settings.NotificationSettings
      USER_NOTIFICATION_SETTINGS =
          new com.linkedin.event.notification.settings.NotificationSettings()
              .setSlackSettings(USER_SLACK_NOTIFICATION_SETTINGS)
              .setEmailSettings(USER_EMAIL_NOTIFICATION_SETTINGS)
              .setSinkTypes(
                  new NotificationSinkTypeArray(
                      ImmutableList.of(
                          com.linkedin.event.notification.NotificationSinkType.EMAIL,
                          com.linkedin.event.notification.NotificationSinkType.SLACK)));
  public static final com.linkedin.identity.CorpUserSettings CORP_USER_SETTINGS =
      new com.linkedin.identity.CorpUserSettings()
          .setNotificationSettings(USER_NOTIFICATION_SETTINGS);
  public static final CorpUserSettings UPDATED_CORP_USER_SETTINGS =
      DEFAULT_CORP_USER_SETTINGS.setNotificationSettings(USER_NOTIFICATION_SETTINGS);
  public static final com.linkedin.event.notification.settings.NotificationSettings
      GROUP_NOTIFICATION_SETTINGS =
          new com.linkedin.event.notification.settings.NotificationSettings()
              .setSlackSettings(GROUP_SLACK_NOTIFICATION_SETTINGS)
              .setEmailSettings(GROUP_EMAIL_NOTIFICATION_SETTINGS)
              .setSinkTypes(
                  new NotificationSinkTypeArray(
                      ImmutableList.of(
                          com.linkedin.event.notification.NotificationSinkType.EMAIL,
                          com.linkedin.event.notification.NotificationSinkType.SLACK)));
  public static final com.linkedin.identity.CorpGroupSettings CORP_GROUP_SETTINGS =
      new com.linkedin.identity.CorpGroupSettings()
          .setNotificationSettings(GROUP_NOTIFICATION_SETTINGS);

  public static NotificationSettings getMappedUserNotificationSettings() {
    final NotificationSettings notificationSettings = new NotificationSettings();
    final SlackNotificationSettings slackNotificationSettings = new SlackNotificationSettings();
    slackNotificationSettings.setUserHandle(SLACK_USER_HANDLE);
    notificationSettings.setSlackSettings(slackNotificationSettings);
    final EmailNotificationSettings emailNotificationSettings = new EmailNotificationSettings();
    emailNotificationSettings.setEmail(EMAIL_ADDRESS);
    notificationSettings.setEmailSettings(emailNotificationSettings);
    notificationSettings.setSinkTypes(
        new ArrayList<>(ImmutableList.of(NotificationSinkType.EMAIL, NotificationSinkType.SLACK)));
    return notificationSettings;
  }

  public static NotificationSettings getMappedGroupNotificationSettings() {
    final NotificationSettings notificationSettings = new NotificationSettings();
    final SlackNotificationSettings slackNotificationSettings = new SlackNotificationSettings();
    slackNotificationSettings.setChannels(SLACK_CHANNELS);
    notificationSettings.setSlackSettings(slackNotificationSettings);
    final EmailNotificationSettings emailNotificationSettings = new EmailNotificationSettings();
    emailNotificationSettings.setEmail(EMAIL_ADDRESS);
    notificationSettings.setEmailSettings(emailNotificationSettings);
    notificationSettings.setSinkTypes(
        new ArrayList<>(ImmutableList.of(NotificationSinkType.EMAIL, NotificationSinkType.SLACK)));
    return notificationSettings;
  }

  private NotificationSettingsTestUtils() {}
}
