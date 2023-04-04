package com.linkedin.datahub.graphql.resolvers.settings;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.ActorType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
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
      new com.linkedin.event.notification.settings.NotificationSettings()
          .setActorUrn(USER_URN)
          .setActorType(com.linkedin.common.ActorType.USER)
          .setSlackSettings(USER_SLACK_NOTIFICATION_SETTINGS);
  public static final com.linkedin.identity.CorpUserSettings CORP_USER_SETTINGS =
      new com.linkedin.identity.CorpUserSettings().setNotifications(USER_NOTIFICATION_SETTINGS);
  public static final CorpUserSettings UPDATED_CORP_USER_SETTINGS =
      DEFAULT_CORP_USER_SETTINGS.setNotifications(USER_NOTIFICATION_SETTINGS);
  public static final com.linkedin.event.notification.settings.NotificationSettings GROUP_NOTIFICATION_SETTINGS =
      new com.linkedin.event.notification.settings.NotificationSettings()
          .setActorUrn(GROUP_URN)
          .setActorType(ActorType.GROUP)
          .setSlackSettings(GROUP_SLACK_NOTIFICATION_SETTINGS);
  public static final com.linkedin.identity.CorpGroupSettings CORP_GROUP_SETTINGS =
      new com.linkedin.identity.CorpGroupSettings().setNotifications(GROUP_NOTIFICATION_SETTINGS);
}
