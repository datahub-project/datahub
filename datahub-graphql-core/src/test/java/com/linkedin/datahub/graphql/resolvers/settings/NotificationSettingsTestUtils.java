package com.linkedin.datahub.graphql.resolvers.settings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationScenarioType;
import com.linkedin.datahub.graphql.generated.NotificationSetting;
import com.linkedin.datahub.graphql.generated.NotificationSettingValue;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettings;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.settings.NotificationSettingMap;
import java.util.ArrayList;
import java.util.Collections;
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
      SHARED_SLACK_NOTIFICATION_SETTINGS =
          new com.linkedin.event.notification.settings.SlackNotificationSettings()
              .setChannels(new StringArray(SLACK_CHANNELS));

  public static final com.linkedin.event.notification.settings.EmailNotificationSettings
      SHARED_EMAIL_NOTIFICATION_SETTINGS =
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
      new CorpUserSettings()
          .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
          .setNotificationSettings(USER_NOTIFICATION_SETTINGS);
  public static final com.linkedin.event.notification.settings.NotificationSettings
      GROUP_NOTIFICATION_SETTINGS =
          new com.linkedin.event.notification.settings.NotificationSettings()
              .setSlackSettings(SHARED_SLACK_NOTIFICATION_SETTINGS)
              .setEmailSettings(SHARED_EMAIL_NOTIFICATION_SETTINGS)
              .setSinkTypes(
                  new NotificationSinkTypeArray(
                      ImmutableList.of(
                          com.linkedin.event.notification.NotificationSinkType.EMAIL,
                          com.linkedin.event.notification.NotificationSinkType.SLACK)));

  public static final com.linkedin.event.notification.settings.NotificationSettings
      NOTIFICATION_SETTINGS_SETTINGS_ONLY =
          new com.linkedin.event.notification.settings.NotificationSettings()
              .setSinkTypes(new NotificationSinkTypeArray(Collections.emptyList()))
              .setSettings(
                  new NotificationSettingMap(
                      ImmutableMap.of(
                          NotificationScenarioType.PROPOSAL_STATUS_CHANGE.toString(),
                          new com.linkedin.settings.NotificationSetting()
                              .setParams(new StringMap(ImmutableMap.of("email.enabled", "true")))
                              .setValue(com.linkedin.settings.NotificationSettingValue.ENABLED),
                          NotificationScenarioType.ASSERTION_STATUS_CHANGE.toString(),
                          new com.linkedin.settings.NotificationSetting()
                              .setParams(new StringMap(ImmutableMap.of("email.enabled", "false")))
                              .setValue(com.linkedin.settings.NotificationSettingValue.DISABLED))));

  public static final com.linkedin.event.notification.settings.NotificationSettings
      NOTIFICATION_SETTINGS_ALL_SETTINGS =
          new com.linkedin.event.notification.settings.NotificationSettings()
              .setSlackSettings(SHARED_SLACK_NOTIFICATION_SETTINGS)
              .setEmailSettings(SHARED_EMAIL_NOTIFICATION_SETTINGS)
              .setSinkTypes(
                  new NotificationSinkTypeArray(
                      ImmutableList.of(
                          com.linkedin.event.notification.NotificationSinkType.EMAIL,
                          com.linkedin.event.notification.NotificationSinkType.SLACK)))
              .setSettings(
                  new NotificationSettingMap(
                      ImmutableMap.of(
                          NotificationScenarioType.PROPOSAL_STATUS_CHANGE.toString(),
                          new com.linkedin.settings.NotificationSetting()
                              .setParams(new StringMap(ImmutableMap.of("email.enabled", "true")))
                              .setValue(com.linkedin.settings.NotificationSettingValue.ENABLED),
                          NotificationScenarioType.ASSERTION_STATUS_CHANGE.toString(),
                          new com.linkedin.settings.NotificationSetting()
                              .setParams(new StringMap(ImmutableMap.of("email.enabled", "false")))
                              .setValue(com.linkedin.settings.NotificationSettingValue.DISABLED))));
  public static final com.linkedin.identity.CorpUserSettings CORP_USER_SETTINGS_SETTINGS_ONLY =
      new com.linkedin.identity.CorpUserSettings()
          // Default user settings!
          .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
          .setNotificationSettings(NOTIFICATION_SETTINGS_SETTINGS_ONLY);

  public static final com.linkedin.identity.CorpUserSettings CORP_USER_SETTINGS_ALL_SETTINGS =
      new com.linkedin.identity.CorpUserSettings()
          .setNotificationSettings(NOTIFICATION_SETTINGS_ALL_SETTINGS);
  public static final com.linkedin.identity.CorpGroupSettings CORP_GROUP_SETTINGS =
      new com.linkedin.identity.CorpGroupSettings()
          .setNotificationSettings(GROUP_NOTIFICATION_SETTINGS);
  public static final com.linkedin.identity.CorpGroupSettings CORP_GROUP_SETTINGS_SETTINGS_ONLY =
      new com.linkedin.identity.CorpGroupSettings()
          .setNotificationSettings(NOTIFICATION_SETTINGS_SETTINGS_ONLY);
  public static final com.linkedin.identity.CorpGroupSettings CORP_GROUP_SETTINGS_ALL_SETTINGS =
      new com.linkedin.identity.CorpGroupSettings()
          .setNotificationSettings(NOTIFICATION_SETTINGS_ALL_SETTINGS);

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

  public static NotificationSettings getMappedSettingsOnlyNotificationSettings() {
    final NotificationSettings notificationSettings = new NotificationSettings();
    notificationSettings.setSinkTypes(new ArrayList<>(Collections.emptyList()));

    final NotificationSetting setting1 = new NotificationSetting();
    setting1.setParams(
        new ArrayList<>(ImmutableList.of(new StringMapEntry("email.enabled", "false"))));
    setting1.setValue(NotificationSettingValue.DISABLED);
    setting1.setType(NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    final NotificationSetting setting2 = new NotificationSetting();
    setting2.setParams(
        new ArrayList<>(ImmutableList.of(new StringMapEntry("email.enabled", "true"))));
    setting2.setValue(NotificationSettingValue.ENABLED);
    setting2.setType(NotificationScenarioType.PROPOSAL_STATUS_CHANGE);

    notificationSettings.setSettings(new ArrayList<>(ImmutableList.of(setting1, setting2)));

    return notificationSettings;
  }

  public static NotificationSettings getMappedExistingSettingsNotificationSettings() {
    final NotificationSettings notificationSettings = new NotificationSettings();
    final SlackNotificationSettings slackNotificationSettings = new SlackNotificationSettings();
    slackNotificationSettings.setChannels(SLACK_CHANNELS);
    notificationSettings.setSlackSettings(slackNotificationSettings);
    final EmailNotificationSettings emailNotificationSettings = new EmailNotificationSettings();
    emailNotificationSettings.setEmail(EMAIL_ADDRESS);
    notificationSettings.setEmailSettings(emailNotificationSettings);
    notificationSettings.setSinkTypes(
        new ArrayList<>(ImmutableList.of(NotificationSinkType.EMAIL, NotificationSinkType.SLACK)));

    // Case 1: Overwritten
    final NotificationSetting setting1 = new NotificationSetting();
    setting1.setParams(
        new ArrayList<>(ImmutableList.of(new StringMapEntry("email.enabled", "true"))));
    setting1.setValue(NotificationSettingValue.ENABLED);
    setting1.setType(NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    // Case 2: Existing stays
    final NotificationSetting setting2 = new NotificationSetting();
    setting2.setParams(
        new ArrayList<>(ImmutableList.of(new StringMapEntry("email.enabled", "true"))));
    setting2.setValue(NotificationSettingValue.ENABLED);
    setting2.setType(NotificationScenarioType.PROPOSAL_STATUS_CHANGE);

    // Case 3: New stays
    final NotificationSetting setting3 = new NotificationSetting();
    setting3.setParams(
        new ArrayList<>(ImmutableList.of(new StringMapEntry("email.enabled", "true"))));
    setting3.setValue(NotificationSettingValue.ENABLED);
    setting3.setType(NotificationScenarioType.ENTITY_DEPRECATION_CHANGE);

    notificationSettings.setSettings(
        new ArrayList<>(ImmutableList.of(setting1, setting2, setting3)));

    return notificationSettings;
  }

  private NotificationSettingsTestUtils() {}
}
