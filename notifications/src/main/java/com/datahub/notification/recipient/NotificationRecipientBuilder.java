package com.datahub.notification.recipient;

import com.datahub.authentication.Authentication;
import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.subscription.SubscriptionInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;


@Slf4j
public abstract class NotificationRecipientBuilder {
  final SettingsProvider _settingsProvider;
  final EntityClient _entityClient;
  final Authentication _authentication;
  final Predicate<? super NotificationSettings> _predicate;

  protected NotificationRecipientBuilder(@Nonnull final SettingsProvider settingsProvider,
      @Nonnull final EntityClient entityClient,
      @Nonnull final Authentication authentication, @Nonnull final Predicate<? super NotificationSettings> predicate) {
    _settingsProvider = settingsProvider;
    _entityClient = entityClient;
    _authentication = authentication;
    _predicate = predicate;
  }

  /**
   * Builds a list of global recipients.
   */
  public abstract List<NotificationRecipient> buildGlobalRecipients(@Nonnull final NotificationScenarioType type);

  /**
   * Builds a list of recipients based on subscriber information.
   */
  public List<NotificationRecipient> buildSubscriberRecipients(@Nonnull Map<Urn, SubscriptionInfo> subscriptions) {
    final Set<Urn> userUrns = new HashSet<>();
    final Set<Urn> groupUrns = new HashSet<>();
    for (Map.Entry<Urn, SubscriptionInfo> entry : subscriptions.entrySet()) {
      final SubscriptionInfo subscriptionInfo = entry.getValue();
      final com.linkedin.common.urn.Urn actorUrn = subscriptionInfo.getActorUrn();
      switch (subscriptionInfo.getActorType()) {
        case CORP_USER_ENTITY_NAME:
          userUrns.add(actorUrn);
          break;
        case CORP_GROUP_ENTITY_NAME:
          groupUrns.add(actorUrn);
          break;
        default:
          log.warn("Unsupported actor type: " + subscriptionInfo.getActorType());
          break;
      }
    }

    final List<NotificationSettings> userNotificationSettings =
        getNotificationSettings(CORP_USER_ENTITY_NAME, userUrns, NotificationSettings::hasSlackSettings);
    final List<NotificationRecipient> userRecipients = buildUserNotificationRecipients(userNotificationSettings);
    final List<NotificationSettings> groupNotificationSettings =
        getNotificationSettings(CORP_GROUP_ENTITY_NAME, groupUrns, NotificationSettings::hasSlackSettings);
    final List<NotificationRecipient> groupRecipients =
        buildGroupNotificationRecipients(groupNotificationSettings);

    final List<NotificationRecipient> notificationRecipients =
        new ArrayList<>(userRecipients.size() + groupRecipients.size());
    notificationRecipients.addAll(userRecipients);
    notificationRecipients.addAll(groupRecipients);

    return notificationRecipients;
  }

  public List<NotificationSettings> getNotificationSettings(@Nonnull final String entityName,
      @Nonnull final Set<Urn> actorUrns, Predicate<? super NotificationSettings> predicate) {
    Map<Urn, EntityResponse> notificationSettingsMap;
    try {
      notificationSettingsMap = Objects.requireNonNull(_entityClient.batchGetV2(entityName,
          actorUrns,
          ImmutableSet.of(NOTIFICATION_SETTINGS_ASPECT_NAME),
          _authentication));
    } catch (Exception e) {
      log.error("Failed to fetch notification settings for actors {}", actorUrns, e);
      return Collections.emptyList();
    }

    return notificationSettingsMap
        .values()
        .stream()
        .filter(entityResponse -> entityResponse.getAspects().containsKey(NOTIFICATION_SETTINGS_ASPECT_NAME))
        .map(entityResponse -> new NotificationSettings(
            entityResponse.getAspects().get(NOTIFICATION_SETTINGS_ASPECT_NAME).getValue().data()))
        .filter(predicate)
        .collect(Collectors.toList());
  }

  protected abstract List<NotificationRecipient> buildUserNotificationRecipients(
      @Nonnull final List<NotificationSettings> userNotificationSettings);

  protected abstract List<NotificationRecipient> buildGroupNotificationRecipients(
      @Nonnull final List<NotificationSettings> groupNotificationSettings);
}
