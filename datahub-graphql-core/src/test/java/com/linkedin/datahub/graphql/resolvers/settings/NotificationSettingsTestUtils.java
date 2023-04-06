package com.linkedin.datahub.graphql.resolvers.settings;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettings;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingsMapper;
import com.linkedin.identity.CorpUserSettings;
import java.util.List;

import static com.linkedin.metadata.service.SettingsService.*;


public class NotificationSettingsTestUtils {
  public static final String USER_URN_STRING = "urn:li:corpuser:testUser";
  public static final Urn USER_URN = UrnUtils.getUrn(USER_URN_STRING);
  public static final String GROUP_URN_STRING = "urn:li:corpGroup:testGroup";
  public static final Urn GROUP_URN = UrnUtils.getUrn(GROUP_URN_STRING);
  public static final String SLACK_USER_HANDLE = "testUser";
  public static final List<String> SLACK_CHANNELS = ImmutableList.of("testChannel1", "testChannel2");
  public static final com.linkedin.event.notification.settings.SlackNotificationSettings
      USER_SLACK_NOTIFICATION_SETTINGS =
      new com.linkedin.event.notification.settings.SlackNotificationSettings().setUserHandle(SLACK_USER_HANDLE);
  public static final com.linkedin.event.notification.settings.SlackNotificationSettings
      GROUP_SLACK_NOTIFICATION_SETTINGS =
      new com.linkedin.event.notification.settings.SlackNotificationSettings().setChannels(new StringArray(
          SLACK_CHANNELS));
  public static final com.linkedin.event.notification.settings.NotificationSettings USER_NOTIFICATION_SETTINGS =
      new com.linkedin.event.notification.settings.NotificationSettings().setSlackSettings(
          USER_SLACK_NOTIFICATION_SETTINGS);
  public static final com.linkedin.identity.CorpUserSettings CORP_USER_SETTINGS =
      new com.linkedin.identity.CorpUserSettings().setNotificationSettings(USER_NOTIFICATION_SETTINGS);
  public static final CorpUserSettings UPDATED_CORP_USER_SETTINGS =
      DEFAULT_CORP_USER_SETTINGS.setNotificationSettings(USER_NOTIFICATION_SETTINGS);
  public static final com.linkedin.event.notification.settings.NotificationSettings GROUP_NOTIFICATION_SETTINGS =
      new com.linkedin.event.notification.settings.NotificationSettings().setSlackSettings(
          GROUP_SLACK_NOTIFICATION_SETTINGS);
  public static final com.linkedin.identity.CorpGroupSettings CORP_GROUP_SETTINGS =
      new com.linkedin.identity.CorpGroupSettings().setNotificationSettings(GROUP_NOTIFICATION_SETTINGS);

  public static NotificationSettings getMappedUserNotificationSettings() {
    final NotificationSettings notificationSettings = new NotificationSettings();
    final SlackNotificationSettings slackNotificationSettings = new SlackNotificationSettings();
    slackNotificationSettings.setUserHandle(SLACK_USER_HANDLE);
    notificationSettings.setSlackSettings(slackNotificationSettings);
    return notificationSettings;
  }

  public static NotificationSettings getMappedGroupNotificationSettings() {
    final NotificationSettings notificationSettings = new NotificationSettings();
    final SlackNotificationSettings slackNotificationSettings = new SlackNotificationSettings();
    slackNotificationSettings.setChannels(SLACK_CHANNELS);
    notificationSettings.setSlackSettings(slackNotificationSettings);
    return notificationSettings;
  }

  private NotificationSettingsTestUtils() {
  }
}
