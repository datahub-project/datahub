package com.datahub.notification.recipient;

import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.subscription.SubscriptionInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmailNotificationRecipientBuilder extends NotificationRecipientBuilder {

  public EmailNotificationRecipientBuilder(@Nonnull final SettingsProvider settingsProvider) {
    super(settingsProvider, NotificationSettings::hasEmailSettings);
  }

  @Override
  public List<NotificationRecipient> buildGlobalRecipients(
      @Nonnull OperationContext opContext, @Nonnull final NotificationScenarioType type) {
    final GlobalSettingsInfo globalSettingsInfo = _settingsProvider.getGlobalSettings(opContext);

    if (globalSettingsInfo == null
        || !globalSettingsInfo.hasNotifications()
        || !globalSettingsInfo.getNotifications().hasSettings()) {
      return Collections.emptyList();
    }

    final NotificationSetting setting =
        globalSettingsInfo.getNotifications().getSettings().get(type.toString());

    if (!isEmailNotificationEnabled(setting)) {
      return Collections.emptyList();
    }

    return createRecipientsFromGlobalSetting(setting, globalSettingsInfo);
  }

  @Override
  protected List<NotificationRecipient> buildUserSubscriberRecipients(
      @Nonnull final Map<Urn, SubscriptionInfo> userToSubscriptionMap,
      @Nonnull final Map<Urn, NotificationSettings> userToNotificationSettings) {
    return createRecipientsFromSubscriptions(userToSubscriptionMap, userToNotificationSettings);
  }

  @Override
  protected List<NotificationRecipient> buildGroupSubscriberRecipients(
      @Nonnull final Map<Urn, SubscriptionInfo> groupToSubscriptionMap,
      @Nonnull final Map<Urn, NotificationSettings> groupToNotificationSettings) {
    return createRecipientsFromSubscriptions(groupToSubscriptionMap, groupToNotificationSettings);
  }

  private List<NotificationRecipient> createRecipientsFromSubscriptions(
      @Nonnull final Map<Urn, ? extends SubscriptionInfo> subscriptionMap,
      @Nonnull final Map<Urn, NotificationSettings> notificationSettingsMap) {

    List<NotificationRecipient> recipients = new ArrayList<>();
    subscriptionMap.forEach(
        (urn, subscriptionInfo) -> {
          if (isEmailEnabledForActor(notificationSettingsMap, urn)) {
            extractEmailForSubscription(subscriptionInfo, notificationSettingsMap.get(urn))
                .ifPresent(
                    email ->
                        recipients.add(
                            new NotificationRecipient()
                                .setId(email)
                                .setType(NotificationRecipientType.EMAIL)
                                .setActor(urn)));
          }
        });

    return recipients;
  }

  private Optional<String> extractEmailForSubscription(
      SubscriptionInfo subscriptionInfo, NotificationSettings settings) {
    String email = null;
    // Attempt to get email from subscription
    String emailFromSubscription = extractEmailFromSubscription(subscriptionInfo);
    if (emailFromSubscription != null) {
      email = emailFromSubscription;
    } else if (settings != null
        && settings.hasEmailSettings()
        && settings.getEmailSettings().hasEmail()) {
      // Fallback to user/group settings
      email = settings.getEmailSettings().getEmail();
    }
    return Optional.ofNullable(email);
  }

  @Nullable
  private String extractEmailFromSubscription(@Nonnull final SubscriptionInfo subscriptionInfo) {
    if (subscriptionInfo.hasNotificationConfig()
        && subscriptionInfo.getNotificationConfig().hasNotificationSettings()
        && subscriptionInfo.getNotificationConfig().getNotificationSettings().hasEmailSettings()) {

      EmailNotificationSettings emailSettings =
          subscriptionInfo.getNotificationConfig().getNotificationSettings().getEmailSettings();
      if (emailSettings.hasEmail()) {
        return emailSettings.getEmail();
      }
    }
    return null;
  }

  private boolean isEmailEnabledForActor(
      @Nonnull final Map<Urn, NotificationSettings> actorToNotificationSettings,
      @Nonnull final Urn urn) {
    NotificationSettings settings = actorToNotificationSettings.get(urn);
    return settings != null && settings.getSinkTypes().contains(NotificationSinkType.EMAIL);
  }

  private boolean isEmailNotificationEnabled(@Nonnull final NotificationSetting setting) {
    return hasParam(setting.getParams(), "email.enabled")
        && Boolean.parseBoolean(setting.getParams().get("email.enabled"));
  }

  private List<NotificationRecipient> createRecipientsFromGlobalSetting(
      @Nonnull final NotificationSetting setting,
      @Nonnull final GlobalSettingsInfo globalSettingsInfo) {

    // Email is enabled. Determine which email to send to.
    String maybeEmail =
        hasParam(setting.getParams(), "email.address")
            ? setting.getParams().get("email.address")
            : getDefaultEmail(globalSettingsInfo);

    if (maybeEmail != null) {
      return ImmutableList.of(
          new NotificationRecipient().setId(maybeEmail).setType(NotificationRecipientType.EMAIL));
    } else {
      log.warn(
          "Failed to resolve email address for global notification setting! Skipping sending notification.");
      return Collections.emptyList();
    }
  }

  private boolean hasParam(@Nullable final Map<String, String> params, final String param) {
    return params != null && params.containsKey(param);
  }

  @Nullable
  private String getDefaultEmail(@Nonnull final GlobalSettingsInfo globalSettingsInfo) {
    if (globalSettingsInfo.hasIntegrations()
        && globalSettingsInfo.getIntegrations().hasEmailSettings()) {
      return globalSettingsInfo.getIntegrations().getEmailSettings().getDefaultEmail(GetMode.NULL);
    }
    return null;
  }
}
