package com.linkedin.metadata.kafka.hook.notification;

import com.datahub.authentication.Authentication;
import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.template.NotificationTemplateType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.datahub.notification.recipient.NotificationRecipientBuilder;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.settings.NotificationSettingValue;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.notification.NotificationUtils.*;


/**
 * This serves as a base class for MAE-based notification generators.
 */
@Slf4j
public abstract class BaseMclNotificationGenerator implements MclNotificationGenerator {

  // TODO - decide whether this limit is reasonable!
  private static final Integer MAX_DOWNSTREAMS_TO_FETCH_OWNERSHIP = 1000;
  private static final Integer MAX_DOWNSTREAMS_HOP = 1000;

  // Should stay disabled due to concerns around performance with upstream queries on notifications
  private static final boolean ENABLE_DOWNSTREAM_ENTITIES = false;

  protected final EventProducer _eventProducer;
  protected final EntityClient _entityClient;
  protected final GraphClient _graphClient;
  protected final SettingsProvider _settingsProvider;
  protected final Authentication _systemAuthentication;
  protected final Map<NotificationSinkType, NotificationRecipientBuilder> _recipientBuilders;

  public BaseMclNotificationGenerator(
      @Nonnull final EventProducer eventProducer,
      @Nonnull final EntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final SettingsProvider settingsProvider,
      @Nonnull final Authentication systemAuthentication,
      @Nonnull final Map<NotificationSinkType, NotificationRecipientBuilder> recipientBuilders) {
    _eventProducer = Objects.requireNonNull(eventProducer);
    _entityClient = Objects.requireNonNull(entityClient);
    _graphClient = Objects.requireNonNull(graphClient);
    _settingsProvider = Objects.requireNonNull(settingsProvider);
    _systemAuthentication = Objects.requireNonNull(systemAuthentication);
    _recipientBuilders = Objects.requireNonNull(recipientBuilders);
  }

  @Override
  public abstract void generate(@Nonnull MetadataChangeLog event);

  protected boolean isEligibleForGlobalRecipients(@Nonnull final NotificationScenarioType type) {
    final GlobalSettingsInfo globalSettingsInfo = _settingsProvider.getGlobalSettings();
    return globalSettingsInfo != null
        && globalSettingsInfo.getNotifications().hasSettings()
        && globalSettingsInfo.getNotifications().getSettings().containsKey(type.toString())
        && NotificationSettingValue.ENABLED.equals(
        globalSettingsInfo.getNotifications().getSettings().get(type.toString()).getValue());
  }

  protected boolean isEligibleForOwnerRecipients() {
    return false;
  }

  protected boolean isEligibleForRelatedOwnerRecipients() {
    return false;
  }

  public boolean isEligibleForSubscriberRecipients() {
    return false;
  }

  protected List<NotificationRecipient> buildRecipients(
      @Nonnull final NotificationScenarioType notificationScenarioType,
      @Nonnull final Urn entityUrn, @Nullable EntityChangeType entityChangeType) {
    final List<NotificationRecipient> recipients = new ArrayList<>();

    // If we should globally broadcast, build the broadcast recipient.
    if (isEligibleForGlobalRecipients(notificationScenarioType)) {
      recipients.addAll(buildGlobalRecipients(notificationScenarioType));
    }

    // TODO: Support sending notifications to owners.
    if (isEligibleForOwnerRecipients()) {
      recipients.addAll(buildOwnerRecipients());
    }

    // TODO: Support sending notifications to related (e.g. downstream) owners.
    if (isEligibleForRelatedOwnerRecipients()) {
      recipients.addAll(buildRelatedOwnerRecipients());
    }

    if (entityChangeType != null && isEligibleForSubscriberRecipients()) {
      recipients.addAll(buildSubscriberRecipients(entityUrn, entityChangeType));
    }

    return recipients;
  }

  @Nonnull
  protected List<NotificationRecipient> buildGlobalRecipients(@Nonnull final NotificationScenarioType type) {
    return _recipientBuilders
        .values()
        .stream()
        .flatMap(builder -> builder.buildGlobalRecipients(type).stream())
        .collect(Collectors.toList());
  }

  protected List<NotificationRecipient> buildOwnerRecipients() {
    throw new NotImplementedException();
  }

  protected List<NotificationRecipient> buildRelatedOwnerRecipients() {
    throw new NotImplementedException();
  }

  @Nonnull
  protected List<NotificationRecipient> buildSubscriberRecipients(@Nonnull final Urn entityUrn, @Nonnull final
  EntityChangeType changeType) {
    final Set<Urn> downstreamEntityUrns = new HashSet<>();

    if (ENABLE_DOWNSTREAM_ENTITIES) {
      downstreamEntityUrns.addAll(getDownstreamEntities(entityUrn));
    }

    final Map<Urn, SubscriptionInfo> subscriptionInfoMap =
        getSubscriptionInfoMap(entityUrn, downstreamEntityUrns, changeType);
    // We split up the subscriptions by sink type.
    final Map<NotificationSinkType, Set<Urn>> sinkTypeToSubscriptionUrns = getSinkTypeToSubscriptionUrnsMap(subscriptionInfoMap);
    final List<NotificationRecipient> recipients = new ArrayList<>();
    for (final NotificationSinkType sinkType : sinkTypeToSubscriptionUrns.keySet()) {
      final Map<Urn, SubscriptionInfo> sinkSubscriptions = subscriptionInfoMap.entrySet()
          .stream()
          .filter(entry -> sinkTypeToSubscriptionUrns.get(sinkType).contains(entry.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      final List<NotificationRecipient> sinkRecipients = _recipientBuilders.get(sinkType)
          .buildSubscriberRecipients(sinkSubscriptions);
      recipients.addAll(sinkRecipients);
    }

    return getUniqueRecipients(recipients);
  }

  @Nonnull
  protected Map<Urn, SubscriptionInfo> getSubscriptionInfoMap(@Nonnull final Urn entityUrn,
      @Nonnull final Set<Urn> downstreamEntityUrns, @Nonnull final EntityChangeType changeType) {
    final Set<Urn> subscriptionUrns = getEntitySubscriptionUrns(entityUrn, changeType);

    if (ENABLE_DOWNSTREAM_ENTITIES) {
      final Set<Urn> downstreamSubscriptionUrns = getDownstreamEntitySubscriptionUrns(downstreamEntityUrns, changeType);
      subscriptionUrns.addAll(downstreamSubscriptionUrns);
    }

    Map<Urn, EntityResponse> subscriptions;

    try {
      subscriptions = Objects.requireNonNull(_entityClient.batchGetV2(SUBSCRIPTION_ENTITY_NAME, subscriptionUrns,
          ImmutableSet.of(SUBSCRIPTION_INFO_ASPECT_NAME),
          _systemAuthentication));
    } catch (Exception e) {
      log.error("Failed to fetch subscriptions for entity {}", entityUrn, e);
      return Collections.emptyMap();
    }

    return subscriptions.entrySet()
        .stream()
        .filter(entry -> entry.getValue().getAspects().containsKey(SUBSCRIPTION_INFO_ASPECT_NAME))
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> mapSubscriptionInfo(entry.getValue())));
  }

  @Nonnull
  protected Set<Urn> getEntitySubscriptionUrns(@Nonnull final Urn entityUrn,
      @Nonnull final EntityChangeType changeType) {
    final Filter filter = createSubscriberFilter(entityUrn, changeType);

    return getFilteredSubscriptionUrns(filter);
  }

  @Nonnull
  protected Set<Urn> getDownstreamEntitySubscriptionUrns(@Nonnull final Set<Urn> entityUrns,
      @Nonnull final EntityChangeType changeType) {
    final Filter filter = createDownstreamSubscriberFilter(entityUrns, changeType);

    return getFilteredSubscriptionUrns(filter);
  }

  @Nonnull
  protected Set<Urn> getFilteredSubscriptionUrns(@Nonnull final Filter filter) {
    SearchResult searchResult;
    try {
      searchResult = _entityClient.filter(
          SUBSCRIPTION_ENTITY_NAME,
          filter,
          null,
          0,
          1000,
          _systemAuthentication
      );
    } catch (Exception e) {
      log.error("Failed to fetch subscriptions for filter {}", filter, e);
      return Collections.emptySet();
    }

    return searchResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toSet());
  }

  @Nonnull
  protected Map<NotificationSinkType, Set<Urn>> getSinkTypeToSubscriptionUrnsMap(
      @Nonnull final Map<Urn, SubscriptionInfo> subscriptionInfoMap) {
    final Map<NotificationSinkType, Set<Urn>> sinkTypeToSubscriptionUrns = new HashMap<>();
    for (final Map.Entry<Urn, SubscriptionInfo> entry : subscriptionInfoMap.entrySet()) {
      final SubscriptionInfo subscriptionInfo = entry.getValue();
      if (subscriptionInfo.hasNotificationConfig()) {
        final SubscriptionNotificationConfig notificationConfig = subscriptionInfo.getNotificationConfig();
        for (final NotificationSinkType sinkType : notificationConfig.getSinkTypes()) {
          if (!sinkTypeToSubscriptionUrns.containsKey(sinkType)) {
            sinkTypeToSubscriptionUrns.put(sinkType, new HashSet<>());
          }
          sinkTypeToSubscriptionUrns.get(sinkType).add(entry.getKey());
        }
      }
    }
    return sinkTypeToSubscriptionUrns;
  }

  protected PlatformEvent createPlatformEvent(final NotificationRequest request) {
    PlatformEvent event = new PlatformEvent();
    event.setName(NOTIFICATION_REQUEST_EVENT_NAME);
    event.setPayload(GenericRecordUtils.serializePayload(request));
    event.setHeader(new PlatformEventHeader()
        .setTimestampMillis(System.currentTimeMillis())
    );
    return event;
  }

  protected NotificationRequest buildNotificationRequest(
      @Nonnull final String templateType,
      @Nonnull final Map<String, String> templateParams,
      @Nonnull final Set<NotificationRecipient> recipients) {
    final NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(
        new NotificationMessage()
            .setTemplate(NotificationTemplateType.valueOf(templateType))
            .setParameters(new StringMap(templateParams))
    );
    notificationRequest.setRecipients(new NotificationRecipientArray(recipients));
    return notificationRequest;
  }

  @Nullable
  protected Ownership getEntityOwnership(final Urn urn) {
    // Fetch the latest version of "ownership" aspect for the resource.
    return batchGetEntityOwnership(urn.getEntityType(), ImmutableSet.of(urn)).get(urn);
  }

  @Nonnull
  protected Map<Urn, Ownership> batchGetEntityOwnership(@Nonnull String entityType, @Nonnull final Set<Urn> urns) {
    if (urns.size() == 0) {
      return Collections.emptyMap();
    }
    try {
      final Map<Urn, EntityResponse> response = _entityClient.batchGetV2(
          entityType,
          urns,
          Collections.singleton(OWNERSHIP_ASPECT_NAME),
          _systemAuthentication);

      return response.entrySet().stream()
          .filter(entry -> entry.getValue() != null && entry.getValue().getAspects().get(OWNERSHIP_ASPECT_NAME) != null)
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              entry -> new Ownership(entry.getValue().getAspects().get(OWNERSHIP_ASPECT_NAME).getValue()
                  .data())));
    } catch (Exception e) {
      log.error("Failed to batch fetch ownership!", e);
      return Collections.emptyMap();
    }
  }

  @Nonnull
  protected Set<Urn> getDownstreamEntities(@Nonnull final Urn entityUrn) {
    try {
      final EntityLineageResult results = _graphClient.getLineageEntities(
          entityUrn.toString(),
          LineageDirection.DOWNSTREAM,
          0,
          1000,
          MAX_DOWNSTREAMS_HOP,
          _systemAuthentication.getActor().toUrnStr()
      );

      return results.getRelationships().stream().map(LineageRelationship::getEntity).collect(Collectors.toSet());
    } catch (Exception e) {
      log.error(String.format("Failed to retrieve downstream owners for entity urn %s.", entityUrn));
      return Collections.emptySet();
    }
  }

  @Nonnull
  protected List<Urn> getDownstreamOwners(final Urn entityUrn) {
    try {
      final EntityLineageResult results = _graphClient.getLineageEntities(
          entityUrn.toString(),
          LineageDirection.DOWNSTREAM,
          0,
          MAX_DOWNSTREAMS_TO_FETCH_OWNERSHIP,
          MAX_DOWNSTREAMS_HOP,
          _systemAuthentication.getActor().toUrnStr()
      );

      // Now fetch the ownership for each entity type in batch.
      final Map<String, Set<Urn>> downstreamEntityUrns = new HashMap<>();
      for (LineageRelationship relationship : results.getRelationships()) {
        downstreamEntityUrns.putIfAbsent(relationship.getEntity().getEntityType(), new HashSet<>());
        downstreamEntityUrns.get(relationship.getEntity().getEntityType()).add(relationship.getEntity());
      }

      final List<Urn> ownerUrns = new ArrayList<>();
      for (Map.Entry<String, Set<Urn>> entry : downstreamEntityUrns.entrySet()) {
        final Map<Urn, Ownership> ownerships = batchGetEntityOwnership(
            entry.getKey(),
            entry.getValue()
        );
        ownerships.entrySet()
            .stream()
            .filter(e -> e.getValue() != null)
            .forEach(e -> {
              // Add each owner from the ownership to the master list of owners.
              ownerUrns.addAll(
                  e.getValue().getOwners().stream().map(Owner::getOwner).collect(Collectors.toList())
              );
            });
      }
      return ownerUrns;
    } catch (Exception e) {
      log.error(String.format("Failed to retrieve downstream owners for entity urn %s.", entityUrn));
      return Collections.emptyList();
    }
  }

  @Nullable
  protected DataMap getAspectData(Urn urn, String aspectName) {
    try {
      EntityResponse response = _entityClient.getV2(
          urn.getEntityType(),
          urn,
          ImmutableSet.of(aspectName),
          _systemAuthentication
      );
      if (response != null && response.getAspects().containsKey(aspectName)) {
        return response.getAspects().get(aspectName).getValue().data();
      } else {
        log.warn(String.format("Failed to get aspect data for  urn %s aspect %s", urn, aspectName));
        return null;
      }
    } catch (Exception e) {
      log.error(String.format("Failed to get aspect data for  urn %s aspect %s", urn, aspectName));
      return null;
    }
  }

  protected void sendNotificationRequest(
      @Nonnull final NotificationRequest notificationRequest) {
    _eventProducer.producePlatformEvent(
        Constants.NOTIFICATION_REQUEST_EVENT_NAME,
        null,
        createPlatformEvent(notificationRequest)
    );
  }
}