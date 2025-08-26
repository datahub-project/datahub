package com.linkedin.datahub.graphql.types.notification.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettings;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class NotificationSettingsMapper
    implements ModelMapper<
        com.linkedin.event.notification.settings.NotificationSettings, NotificationSettings> {

  public static final NotificationSettingsMapper INSTANCE = new NotificationSettingsMapper();

  public static NotificationSettings map(
      @Nullable final QueryContext context,
      @Nonnull
          final com.linkedin.event.notification.settings.NotificationSettings
              notificationSettings) {
    return INSTANCE.apply(context, notificationSettings);
  }

  @Override
  public NotificationSettings apply(
      @Nullable final QueryContext context,
      @Nonnull
          final com.linkedin.event.notification.settings.NotificationSettings
              notificationSettings) {
    final NotificationSettings result = new NotificationSettings();
    result.setSinkTypes(new ArrayList<>());

    if (notificationSettings.hasSinkTypes()) {
      result.setSinkTypes(
          notificationSettings.getSinkTypes().stream()
              .map(v -> NotificationSinkType.valueOf(v.toString()))
              .collect(Collectors.toList()));
    }

    if (notificationSettings.hasSlackSettings()) {
      result.setSlackSettings(mapSlackSettings(notificationSettings.getSlackSettings()));
    }

    if (notificationSettings.hasEmailSettings()) {
      result.setEmailSettings(mapEmailSettings(notificationSettings.getEmailSettings()));
    }

    if (notificationSettings.hasSettings()) {
      result.setSettings(
          NotificationSettingMapMapper.mapNotificationSettings(
              context, notificationSettings.getSettings()));
    }

    return result;
  }

  private SlackNotificationSettings mapSlackSettings(
      @Nonnull
          final com.linkedin.event.notification.settings.SlackNotificationSettings slackSettings) {
    final SlackNotificationSettings result = new SlackNotificationSettings();
    if (slackSettings.hasUserHandle()) {
      result.setUserHandle(slackSettings.getUserHandle());
    }
    if (slackSettings.hasChannels()) {
      result.setChannels(slackSettings.getChannels());
    }

    return result;
  }

  private EmailNotificationSettings mapEmailSettings(
      @Nonnull
          final com.linkedin.event.notification.settings.EmailNotificationSettings emailSettings) {
    final EmailNotificationSettings result = new EmailNotificationSettings();
    if (emailSettings.hasEmail()) {
      result.setEmail(emailSettings.getEmail());
    }
    return result;
  }
}
