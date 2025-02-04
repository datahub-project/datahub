package com.datahub.notification.recipient;

import static com.linkedin.metadata.Constants.*;

import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.subscription.SubscriptionInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class NotificationRecipientBuilder {
  final SettingsService _settingsService;
  final Predicate<? super NotificationSettings> _predicate;

  protected NotificationRecipientBuilder(
      @Nonnull final SettingsService settingsService,
      @Nonnull final Predicate<? super NotificationSettings> predicate) {
    _settingsService = settingsService;
    _predicate = predicate;
  }

  /** Builds a list of global recipients. */
  public abstract List<NotificationRecipient> buildGlobalRecipients(
      @Nonnull OperationContext opContext, @Nonnull final NotificationScenarioType type);

  /** Builds an individual actor recipient */
  public abstract List<NotificationRecipient> buildActorRecipients(
      @Nonnull final OperationContext opContext,
      @Nonnull final List<Urn> actorUrns,
      @Nonnull final NotificationScenarioType type);

  /** Builds an individual recipient object with a set of params */
  @Nonnull
  public NotificationRecipient buildRecipient(
      @Nonnull NotificationRecipientType recipientType,
      @Nonnull String recipientId,
      @Nullable Urn actorUrn) {
    NotificationRecipient recipient =
        new NotificationRecipient().setType(recipientType).setId(recipientId);
    if (actorUrn != null) {
      recipient.setActor(actorUrn);
    }
    return recipient;
  }

  /** Builds a list of recipients based on subscriber information. */
  public List<NotificationRecipient> buildSubscriberRecipients(
      @Nonnull OperationContext opContext, @Nonnull Map<Urn, SubscriptionInfo> subscriptions) {
    final Map<Urn, SubscriptionInfo> userToSubscriptionMap = new HashMap<>();
    final Map<Urn, SubscriptionInfo> groupToSubscriptionMap = new HashMap<>();
    for (Map.Entry<Urn, SubscriptionInfo> entry : subscriptions.entrySet()) {
      final SubscriptionInfo subscriptionInfo = entry.getValue();
      final com.linkedin.common.urn.Urn recipientActorUrn = subscriptionInfo.getActorUrn();

      switch (subscriptionInfo.getActorType()) {
        case CORP_USER_ENTITY_NAME:
          userToSubscriptionMap.put(recipientActorUrn, subscriptionInfo);
          break;
        case CORP_GROUP_ENTITY_NAME:
          groupToSubscriptionMap.put(recipientActorUrn, subscriptionInfo);
          break;
        default:
          log.warn("Unsupported actor type: " + subscriptionInfo.getActorType());
          break;
      }
    }

    final Set<Urn> userUrns = new HashSet<>(userToSubscriptionMap.keySet());
    final Set<Urn> groupUrns = new HashSet<>(groupToSubscriptionMap.keySet());

    final Map<Urn, NotificationSettings> userToNotificationSettings =
        getNotificationSettings(opContext, CORP_USER_ENTITY_NAME, userUrns);

    final Map<Urn, NotificationSettings> groupToNotificationSettings =
        getNotificationSettings(opContext, CORP_GROUP_ENTITY_NAME, groupUrns);

    final List<NotificationRecipient> userRecipients =
        buildUserSubscriberRecipients(userToSubscriptionMap, userToNotificationSettings);

    final List<NotificationRecipient> groupRecipients =
        buildGroupSubscriberRecipients(groupToSubscriptionMap, groupToNotificationSettings);

    final List<NotificationRecipient> notificationRecipients =
        new ArrayList<>(userRecipients.size() + groupRecipients.size());
    notificationRecipients.addAll(userRecipients);
    notificationRecipients.addAll(groupRecipients);

    return notificationRecipients;
  }

  public Map<Urn, NotificationSettings> getNotificationSettings(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final Set<Urn> actorUrns) {
    Map<Urn, EntityResponse> notificationSettingsMap;

    if (actorUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    String aspectName =
        entityName.equals(CORP_USER_ENTITY_NAME)
            ? CORP_USER_SETTINGS_ASPECT_NAME
            : CORP_GROUP_SETTINGS_ASPECT_NAME;
    try {
      notificationSettingsMap =
          Objects.requireNonNull(
              _settingsService
                  .getEntityClient()
                  .batchGetV2(opContext, entityName, actorUrns, ImmutableSet.of(aspectName)));
    } catch (Exception e) {
      log.error("Failed to fetch notification settings for actors {}", actorUrns, e);
      return Collections.emptyMap();
    }

    return notificationSettingsMap.entrySet().stream()
        .filter(entry -> entry.getValue().getAspects().containsKey(aspectName))
        .collect(
            Collectors.toMap(
                entry -> entry.getKey(),
                entry -> mapToNotificationSettings(aspectName, entry.getValue())));
  }

  private NotificationSettings mapToNotificationSettings(
      @Nonnull final String aspectName, @Nonnull final EntityResponse entityResponse) {
    if (aspectName.equals(CORP_USER_SETTINGS_ASPECT_NAME)) {
      return mapUserToNotificationSettings(entityResponse);
    } else if (aspectName.equals(CORP_GROUP_SETTINGS_ASPECT_NAME)) {
      return mapGroupToNotificationSettings(entityResponse);
    }
    return new NotificationSettings();
  }

  private NotificationSettings mapUserToNotificationSettings(
      @Nonnull final EntityResponse entityResponse) {
    CorpUserSettings corpUserSettings =
        new CorpUserSettings(
            entityResponse.getAspects().get(CORP_USER_SETTINGS_ASPECT_NAME).getValue().data());
    if (corpUserSettings.hasNotificationSettings()) {
      return new NotificationSettings(corpUserSettings.getNotificationSettings().data());
    }
    return new NotificationSettings();
  }

  private NotificationSettings mapGroupToNotificationSettings(
      @Nonnull final EntityResponse entityResponse) {
    CorpGroupSettings corpGroupSettings =
        new CorpGroupSettings(
            entityResponse.getAspects().get(CORP_GROUP_SETTINGS_ASPECT_NAME).getValue().data());
    if (corpGroupSettings.hasNotificationSettings()) {
      return new NotificationSettings(corpGroupSettings.getNotificationSettings().data());
    }
    return new NotificationSettings();
  }

  protected abstract List<NotificationRecipient> buildUserSubscriberRecipients(
      @Nonnull final Map<Urn, SubscriptionInfo> userToSubscriptionMap,
      @Nonnull final Map<Urn, NotificationSettings> userToNotificationSettings);

  protected abstract List<NotificationRecipient> buildGroupSubscriberRecipients(
      @Nonnull final Map<Urn, SubscriptionInfo> groupToSubscriptionMap,
      @Nonnull final Map<Urn, NotificationSettings> groupToNotificationSettings);
}
