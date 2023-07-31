package com.linkedin.datahub.graphql.types.notification.mappers;

import com.linkedin.datahub.graphql.generated.IntendedUserType;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettings;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.stream.Collectors;


public class NotificationSettingsMapper
    implements ModelMapper<com.linkedin.event.notification.settings.NotificationSettings, NotificationSettings> {

  public static final NotificationSettingsMapper INSTANCE = new NotificationSettingsMapper();

  public static NotificationSettings map(
      @Nonnull final com.linkedin.event.notification.settings.NotificationSettings notificationSettings) {
    return INSTANCE.apply(notificationSettings);
  }

  @Override
  public NotificationSettings apply(
      @Nonnull final com.linkedin.event.notification.settings.NotificationSettings notificationSettings) {
    final NotificationSettings result = new NotificationSettings();
    result.setSinkTypes(new ArrayList<>());

    if (notificationSettings.hasSinkTypes()) {
      result.setSinkTypes(notificationSettings.getSinkTypes().stream().map(v -> NotificationSinkType.valueOf(v.toString())).collect(Collectors.toList()));
    }

    if (notificationSettings.hasSlackSettings()) {
      result.setSlackSettings(mapSlackSettings(notificationSettings.getSlackSettings()));
    }
    return result;
  }

  private SlackNotificationSettings mapSlackSettings(
      @Nonnull final com.linkedin.event.notification.settings.SlackNotificationSettings slackSettings) {
    final SlackNotificationSettings result = new SlackNotificationSettings();
    if (slackSettings.hasUserHandle()) {
      result.setUserHandle(slackSettings.getUserHandle());
    }
    if (slackSettings.hasChannels()) {
      result.setChannels(slackSettings.getChannels());
    }

    return result;
  }
}
