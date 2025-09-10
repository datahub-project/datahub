package com.linkedin.datahub.graphql.types.notification.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettings;
import com.linkedin.datahub.graphql.generated.TeamsNotificationSettings;
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

    if (notificationSettings.hasTeamsSettings()) {
      result.setTeamsSettings(mapTeamsSettings(notificationSettings.getTeamsSettings()));
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

  private TeamsNotificationSettings mapTeamsSettings(
      @Nonnull
          final com.linkedin.event.notification.settings.TeamsNotificationSettings teamsSettings) {
    final TeamsNotificationSettings result = new TeamsNotificationSettings();
    if (teamsSettings.hasUser()) {
      com.linkedin.datahub.graphql.generated.TeamsUser graphqlUser =
          new com.linkedin.datahub.graphql.generated.TeamsUser();
      com.linkedin.settings.global.TeamsUser user = teamsSettings.getUser();

      // Map the TeamsUser fields properly
      if (user.hasAzureUserId()) {
        graphqlUser.setAzureUserId(user.getAzureUserId());
      }
      if (user.hasTeamsUserId()) {
        graphqlUser.setTeamsUserId(user.getTeamsUserId());
      }
      if (user.hasEmail()) {
        graphqlUser.setEmail(user.getEmail());
      }

      if (user.hasDisplayName()) {
        graphqlUser.setDisplayName(user.getDisplayName());
      }
      if (user.hasLastUpdated()) {
        graphqlUser.setLastUpdated(user.getLastUpdated());
      }

      result.setUser(graphqlUser);
    }
    if (teamsSettings.hasChannels()) {
      result.setChannels(
          teamsSettings.getChannels().stream()
              .map(
                  channel -> {
                    com.linkedin.datahub.graphql.generated.TeamsChannel graphqlChannel =
                        new com.linkedin.datahub.graphql.generated.TeamsChannel();
                    graphqlChannel.setId(channel.getId());
                    if (channel.hasName()) {
                      graphqlChannel.setName(channel.getName());
                    }
                    return graphqlChannel;
                  })
              .collect(Collectors.toList()));
    }
    return result;
  }
}
