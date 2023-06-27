package com.datahub.notification.recipient;

import com.datahub.authentication.Authentication;
import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableList;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.global.GlobalSettingsInfo;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SlackNotificationRecipientBuilder extends NotificationRecipientBuilder {
  private static final Predicate<? super NotificationSettings> predicate = NotificationSettings::hasSlackSettings;
  private static final String SLACK_CHANNEL_CUSTOM_TYPE = "SLACK_CHANNEL";
  private static final String SLACK_DM_CUSTOM_TYPE = "SLACK_DM";

  public SlackNotificationRecipientBuilder(
      @Nonnull final SettingsProvider settingsProvider,
      @Nonnull EntityClient entityClient,
      @Nonnull Authentication authentication) {
    super(settingsProvider, entityClient, authentication, predicate);
  }

  @Override
  public List<NotificationRecipient> buildGlobalRecipients(@Nonnull final NotificationScenarioType type) {
    final GlobalSettingsInfo globalSettingsInfo = _settingsProvider.getGlobalSettings();
    final NotificationSetting setting = globalSettingsInfo.getNotifications().getSettings().get(type.toString());
    // If notifications are disabled for this notification type, skip.
    if (!isSlackEnabled(globalSettingsInfo) || (hasParam(setting.getParams(), "slack.enabled")
        && Boolean.FALSE.equals(Boolean.valueOf(setting.getParams().get("slack.enabled"))))) {
      // Skip notification type.
      return Collections.emptyList();
    }
    // Slack is enabled. Determine which channel to send to.
    String maybeSlackChannel = hasParam(setting.getParams(), "slack.channel")
        ? setting.getParams().get("slack.channel")
        : getDefaultSlackChanel(globalSettingsInfo);

    if (maybeSlackChannel != null) {
      return ImmutableList.of(
          new NotificationRecipient()
              .setId(maybeSlackChannel)
              .setType(NotificationRecipientType.CUSTOM)
              .setCustomType("SLACK_CHANNEL"));
    } else {
      // No Resolved slack channel -- warn!
      log.warn(String.format("Failed to resolve slack channel to send notification of type %s to!", type));
      return Collections.emptyList();
    }
  }

  // TODO: make sure we can use the override slack channel/DM on the subscription itself
  @Override
  protected List<NotificationRecipient> buildUserNotificationRecipients(
      @Nonnull final List<NotificationSettings> userNotificationSettings) {
    return userNotificationSettings.stream()
        .map(NotificationSettings::getSlackSettings)
        .filter(Objects::nonNull)
        .filter(SlackNotificationSettings::hasUserHandle)
        .map(slackNotificationSettings -> new NotificationRecipient()
            .setId(Objects.requireNonNull(slackNotificationSettings.getUserHandle()))
            .setType(NotificationRecipientType.CUSTOM)
            .setCustomType(SLACK_DM_CUSTOM_TYPE))
        .collect(Collectors.toList());
  }

  @Override
  protected List<NotificationRecipient> buildGroupNotificationRecipients(
      @Nonnull final List<NotificationSettings> groupNotificationSettings) {
    return groupNotificationSettings.stream()
        .map(NotificationSettings::getSlackSettings)
        .filter(Objects::nonNull)
        .filter(SlackNotificationSettings::hasChannels)
        .flatMap(slackNotificationSettings -> Objects.requireNonNull(slackNotificationSettings.getChannels())
            .stream()
            .map(channel -> new NotificationRecipient()
                .setId(channel)
                .setType(NotificationRecipientType.CUSTOM)
                .setCustomType(SLACK_CHANNEL_CUSTOM_TYPE)))
        .collect(Collectors.toList());
  }

  private boolean isSlackEnabled(@Nullable final GlobalSettingsInfo globalSettingsInfo) {
    return globalSettingsInfo != null
        && globalSettingsInfo.getIntegrations().hasSlackSettings()
        && globalSettingsInfo.getIntegrations().getSlackSettings().isEnabled();
  }

  private String getDefaultSlackChanel(@Nullable final GlobalSettingsInfo globalSettingsInfo) {
    return globalSettingsInfo != null
        && globalSettingsInfo.getIntegrations().hasSlackSettings()
        ? globalSettingsInfo.getIntegrations().getSlackSettings().getDefaultChannelName()
        : null;
  }

  private boolean hasParam(@Nullable final Map<String, String> params, final String param) {
    return params != null && params.containsKey(param);
  }
}
