package com.datahub.notification.recipient;

import com.datahub.notification.NotificationScenarioType;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.subscription.SubscriptionInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmailNotificationRecipientBuilder extends NotificationRecipientBuilder {

  public EmailNotificationRecipientBuilder(@Nonnull final SettingsService settingsService) {
    super(settingsService, NotificationSettings::hasEmailSettings);
  }

  @Override
  public List<NotificationRecipient> buildGlobalRecipients(
      @Nonnull OperationContext opContext, @Nonnull final NotificationScenarioType type) {
    final GlobalSettingsInfo globalSettingsInfo = _settingsService.getGlobalSettings(opContext);

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
  public List<NotificationRecipient> buildActorRecipients(@Nonnull final OperationContext opContext, @Nonnull final List<Urn> actorUrns, @Nonnull final NotificationScenarioType type) {

    final List<Urn> userUrns = actorUrns.stream().filter(urn -> Constants.CORP_USER_ENTITY_NAME.equals(urn.getEntityType())).collect(Collectors.toList());
    final List<Urn> groupUrns = actorUrns.stream().filter(urn -> Constants.CORP_GROUP_ENTITY_NAME.equals(urn.getEntityType())).collect(Collectors.toList());

    final List<NotificationRecipient> recipients = new ArrayList<>();

    if (!userUrns.isEmpty()) {
      recipients.addAll(buildUserActorRecipients(opContext, userUrns, type));
    }

    if (!groupUrns.isEmpty()) {
      recipients.addAll(buildGroupActorRecipients(opContext, groupUrns, type));
    }

    return recipients;
  }

  private List<NotificationRecipient> buildUserActorRecipients(
    @Nonnull final OperationContext opContext,
    @Nonnull final List<Urn> userUrns,
    @Nonnull final NotificationScenarioType type
  ) {
    final List<NotificationRecipient> recipients = new ArrayList<>();

    // 1. For each user, extract their settings.
    final Map<Urn, CorpUserSettings> userToSettings = _settingsService.batchGetCorpUserSettings(opContext, userUrns);

    // 1.a. Filter out users who have no notification settings.
    final Map<Urn, NotificationSettings> userToNotificationSettings = userToSettings.entrySet().stream()
            .filter(entry -> entry.getValue().hasNotificationSettings())
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getNotificationSettings()));

    // 2. For each user with settings, determine whether we are allowed to send to them based on settings.
    for (Map.Entry<Urn, NotificationSettings> entry : userToNotificationSettings.entrySet()) {

      final Urn userUrn = entry.getKey();
      final NotificationSettings notificationSettings = entry.getValue();

      if (isActorSettingsEnabledForScenario(notificationSettings, type) && isEmailEnabledForActor(userToNotificationSettings, userUrn)) {
        // Determine which email to send to.
        String maybeEmail = extractEmailFromNotificationSettingsForScenarioType(notificationSettings,  type);
        if (maybeEmail == null) {
          log.warn("Failed to resolve email address for user {}! Skipping sending notification.", userUrn);
          continue;
        }
        // Now we can build and add the recipient.
        recipients.add(buildRecipient(NotificationRecipientType.EMAIL, maybeEmail, userUrn));
      }
    }
    return recipients;
  }

  private List<NotificationRecipient> buildGroupActorRecipients(
      @Nonnull final OperationContext opContext,
      @Nonnull final List<Urn> groupUrns,
      @Nonnull final NotificationScenarioType type
  ) {
      final List<NotificationRecipient> recipients = new ArrayList<>();

      // 1. For each group, extract their settings.
      final Map<Urn, CorpGroupSettings> groupToSettings = _settingsService.batchGetCorpGroupSettings(opContext, groupUrns);

      // 1.a. Filter out groups who have no notification settings.
      final Map<Urn, NotificationSettings> groupToNotificationSettings = groupToSettings.entrySet().stream()
              .filter(entry -> entry.getValue().hasNotificationSettings())
              .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getNotificationSettings()));

      // 2. For each group with settings, determine whether we are allowed to send to them based on settings.
      for (final Map.Entry<Urn, NotificationSettings> entry : groupToNotificationSettings.entrySet()) {

        final Urn groupUrn = entry.getKey();
        final NotificationSettings notificationSettings = entry.getValue();

        if (isActorSettingsEnabledForScenario(notificationSettings, type) && isEmailEnabledForActor(groupToNotificationSettings, groupUrn)) {
            // Determine which email to send to.
            String maybeEmail = extractEmailFromNotificationSettingsForScenarioType(notificationSettings,  type);
            if (maybeEmail == null) {
            log.warn("Failed to resolve email address for group {}! Skipping sending notification.", groupUrn);
            continue;
            }
            // Now we can build and add the recipient.
            recipients.add(buildRecipient(NotificationRecipientType.EMAIL, maybeEmail, groupUrn));
        }
      }
      return recipients;
  }

  private boolean isActorSettingsEnabledForScenario(@Nonnull final NotificationSettings notificationSettings, @Nonnull final NotificationScenarioType type) {
    if (notificationSettings.hasScenarioSettings()) {
      final Map<String, NotificationSetting> scenarioSettings = notificationSettings.getScenarioSettings();
      if (scenarioSettings.containsKey(type.toString())) {
          return isEmailNotificationEnabled(scenarioSettings.get(type.toString()));
      }
    }
    return false;
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

  @Nullable
  private String extractEmailFromNotificationSettingsForScenarioType(
          @Nonnull final NotificationSettings settings, @Nonnull final NotificationScenarioType type) {
    // First, see if there is a scenario-specific override email address.
    final Map<String, NotificationSetting> scenarioSettings = settings.getScenarioSettings();
    if (scenarioSettings.containsKey(type.toString())) {
      final NotificationSetting setting = scenarioSettings.get(type.toString());
      if (hasParam(setting.getParams(), "email.address") && !setting.getParams().get("email.address").isEmpty()) {
        return setting.getParams().get("email.address");
      }
    }
    // First check if there is a default email address.
    if (settings.hasEmailSettings() && settings.getEmailSettings().hasEmail()) {
      return settings.getEmailSettings().getEmail();
    }
    // Else, we were unable to resolve an email address!
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
