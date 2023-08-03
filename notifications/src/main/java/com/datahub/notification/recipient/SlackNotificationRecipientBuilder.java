package com.datahub.notification.recipient;

import com.datahub.authentication.Authentication;
import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.global.GlobalSettingsInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.linkedin.subscription.SubscriptionInfo;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SlackNotificationRecipientBuilder extends NotificationRecipientBuilder {
  private static final Predicate<? super NotificationSettings> PREDICATE = NotificationSettings::hasSlackSettings;
  private static final String SLACK_CHANNEL_CUSTOM_TYPE = "SLACK_CHANNEL";
  private static final String SLACK_DM_CUSTOM_TYPE = "SLACK_DM";

  public SlackNotificationRecipientBuilder(
      @Nonnull final SettingsProvider settingsProvider,
      @Nonnull EntityClient entityClient,
      @Nonnull Authentication authentication) {
    super(settingsProvider, entityClient, authentication, PREDICATE);
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

  private boolean isSlackEnabledForActor(@Nonnull final Map<Urn, NotificationSettings> actorToNotificationSettings, @Nonnull final Urn urn) {
    return actorToNotificationSettings.containsKey(urn) && actorToNotificationSettings.get(urn).getSinkTypes().contains(NotificationSinkType.SLACK);
  }

  @Nullable
  private String getUserRecipientIdFromSubscription(Map.Entry<Urn, SubscriptionInfo> urnToSubscriptionInfo) {
    if (
        urnToSubscriptionInfo.getValue().hasNotificationConfig()
          && urnToSubscriptionInfo.getValue().getNotificationConfig().hasNotificationSettings()
          && urnToSubscriptionInfo.getValue().getNotificationConfig().getNotificationSettings().hasSlackSettings()
    ) {
      SlackNotificationSettings slackSettings = urnToSubscriptionInfo.getValue().getNotificationConfig().getNotificationSettings().getSlackSettings();
      return slackSettings.getUserHandle() != null ? slackSettings.getUserHandle() : null;
    }
    return null;
  }

  /*
   * For each user that has a Subscription, try to create a NotificationRecipient object and return this list of NotificationRecipients.
   * If a user has a member ID set on the subscription, use that, otherwise default to what's in their settings
   */
  @Override
  protected List<NotificationRecipient> buildUserSubscriberRecipients(
      @Nonnull final Map<Urn, SubscriptionInfo> userToSubscriptionMap,
      @Nonnull final Map<Urn, NotificationSettings> userToNotificationSettings
      ) {
    List<NotificationRecipient> notificationRecipients = new ArrayList<>();
    userToSubscriptionMap.entrySet().forEach(entry -> {
      // first, ensure slack is enabled for the user
      if (isSlackEnabledForActor(userToNotificationSettings, entry.getKey())) {
        NotificationRecipient notificationRecipient = new NotificationRecipient().setType(NotificationRecipientType.CUSTOM).setCustomType(SLACK_DM_CUSTOM_TYPE);
        String recipientIdFromSubscription = getUserRecipientIdFromSubscription(entry);
        if (recipientIdFromSubscription != null) {
          notificationRecipient.setId(recipientIdFromSubscription);
        } else {
          NotificationSettings notificationSettings = userToNotificationSettings.get(entry.getKey());
          if (!notificationSettings.hasSlackSettings()) {
            log.warn(String.format("Unable to create NotificationRecipient for user %s as they do not have Slack Setting configured", entry.getKey()));
            return;
          }
          notificationRecipient.setId(Objects.requireNonNull(notificationSettings.getSlackSettings().getUserHandle()));
        }
        notificationRecipients.add(notificationRecipient);
      }
    });
    return notificationRecipients;
  }

  @Nullable
  private List<String> getGroupRecipientIdsFromSubscription(Map.Entry<Urn, SubscriptionInfo> urnToSubscriptionInfo) {
    if (
        urnToSubscriptionInfo.getValue().hasNotificationConfig()
          && urnToSubscriptionInfo.getValue().getNotificationConfig().hasNotificationSettings()
          && urnToSubscriptionInfo.getValue().getNotificationConfig().getNotificationSettings().hasSlackSettings()
    ) {
      SlackNotificationSettings slackSettings = urnToSubscriptionInfo.getValue().getNotificationConfig().getNotificationSettings().getSlackSettings();
      return slackSettings.getChannels() != null ? slackSettings.getChannels() : null;
    }
    return null;
  }

  /*
   * For each group that has a Subscription, try to create a NotificationRecipient object and return this list of NotificationRecipients.
   * If a group has channels set on the subscription, use that, otherwise default to what's in their settings
   */
  @Override
  protected List<NotificationRecipient> buildGroupSubscriberRecipients(
      @Nonnull final Map<Urn, SubscriptionInfo> groupToSubscriptionMap,
      @Nonnull final Map<Urn, NotificationSettings> groupToNotificationSettings
      ) {
    List<NotificationRecipient> notificationRecipients = new ArrayList<>();
    groupToSubscriptionMap.entrySet().forEach(entry -> {
      // first, ensure slack is enabled for the group
      if (isSlackEnabledForActor(groupToNotificationSettings, entry.getKey())) {
        List<String> recipientIdsFromSubscription = getGroupRecipientIdsFromSubscription(entry);
        if (recipientIdsFromSubscription != null && recipientIdsFromSubscription.size() > 0) {
          recipientIdsFromSubscription.forEach(id -> {
            notificationRecipients.add(new NotificationRecipient()
              .setId(id)
              .setType(NotificationRecipientType.CUSTOM)
              .setCustomType(SLACK_CHANNEL_CUSTOM_TYPE));
          });
        } else {
          NotificationSettings notificationSettings = groupToNotificationSettings.get(entry.getKey());
          if (!notificationSettings.hasSlackSettings() || !notificationSettings.getSlackSettings().hasChannels()) {
            log.warn(String.format("Unable to create NotificationRecipient for user %s as they do not have Slack Setting configured", entry.getKey()));
            return;
          }
          notificationSettings.getSlackSettings().getChannels().forEach(channel -> {
            notificationRecipients.add(new NotificationRecipient()
                .setId(channel)
                .setType(NotificationRecipientType.CUSTOM)
                .setCustomType(SLACK_CHANNEL_CUSTOM_TYPE));
          });
        }
      }
    });
    return notificationRecipients;
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
