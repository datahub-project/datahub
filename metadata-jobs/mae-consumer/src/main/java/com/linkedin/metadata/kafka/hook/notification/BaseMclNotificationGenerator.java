package com.linkedin.metadata.kafka.hook.notification;

import com.datahub.authentication.Authentication;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
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
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.NotificationSettingValue;
import com.linkedin.settings.global.GlobalSettingsInfo;
import java.util.ArrayList;
import java.util.Collection;
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

import static com.linkedin.metadata.Constants.*;


/**
 * This serves as a base class for MAE-based notification generators.
 */
@Slf4j
public abstract class BaseMclNotificationGenerator implements MclNotificationGenerator {

  // TODO - decide whether this limit is reasonable!
  private static final Integer MAX_DOWNSTREAMS_TO_FETCH_OWNERSHIP = 1000;
  private static final Integer MAX_DOWNSTREAMS_HOP = 1000;

  protected final EventProducer _eventProducer;
  protected final EntityClient _entityClient;
  protected final GraphClient _graphClient;
  protected final SettingsProvider _settingsProvider;
  protected final Authentication _systemAuthentication;

  public BaseMclNotificationGenerator(
      @Nonnull final EventProducer eventProducer,
      @Nonnull final EntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final SettingsProvider settingsProvider,
      @Nonnull final Authentication systemAuthentication) {
    _eventProducer = Objects.requireNonNull(eventProducer);
    _entityClient = Objects.requireNonNull(entityClient);
    _graphClient = Objects.requireNonNull(graphClient);
    _settingsProvider = Objects.requireNonNull(settingsProvider);
    _systemAuthentication = Objects.requireNonNull(systemAuthentication);
  }

  @Override
  public abstract void generate(@Nonnull MetadataChangeLog event);

  protected boolean isEligibleForGlobalRecipients(NotificationScenarioType type) {
    GlobalSettingsInfo globalSettingsInfo = _settingsProvider.getGlobalSettings();
    return globalSettingsInfo != null
        && globalSettingsInfo.getNotifications().hasSettings()
        && globalSettingsInfo.getNotifications().getSettings().containsKey(type.toString())
        && NotificationSettingValue.ENABLED.equals(
        globalSettingsInfo.getNotifications().getSettings().get(type.toString()).getValue());
  }

  protected boolean isEligibleForOwnerRecipients(NotificationScenarioType type) {
    return false;
  }

  protected boolean isEligibleForRelatedOwnerRecipients(NotificationScenarioType type) {
    return false;
  }

  protected boolean isEligibleForSubscriberRecipients(NotificationScenarioType type) {
    return false;
  }

  protected List<NotificationRecipient> buildGlobalRecipients(NotificationScenarioType type) {
    GlobalSettingsInfo globalSettingsInfo = _settingsProvider.getGlobalSettings();
    NotificationSetting setting = globalSettingsInfo.getNotifications().getSettings().get(type.toString());
    List<NotificationRecipient> slackRecipients = buildGlobalSlackRecipients(type, setting);
    // Here's where we'd add other integration types (email, msft teams). For now none!
    return slackRecipients;
  }

  protected List<NotificationRecipient> buildGlobalSlackRecipients(NotificationScenarioType type, NotificationSetting setting) {
    // If notifications are disabled for this notification type, skip.
    if (!isSlackEnabled() || (hasParam(setting.getParams(), "slack.enabled")
        && Boolean.FALSE.equals(Boolean.valueOf(setting.getParams().get("slack.enabled"))))) {
      // Skip notification type.
      return Collections.emptyList();
    }
    // Slack is enabled. Determine which channel to send to.
    String maybeSlackChannel = hasParam(setting.getParams(), "slack.channel")
        ? setting.getParams().get("slack.channel")
        : getDefaultSlackChanel();

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

  protected boolean isSlackEnabled() {
    GlobalSettingsInfo globalSettingsInfo = _settingsProvider.getGlobalSettings();
    return globalSettingsInfo != null
        && globalSettingsInfo.getIntegrations().hasSlackSettings()
        && globalSettingsInfo.getIntegrations().getSlackSettings().isEnabled();
  }

  protected String getDefaultSlackChanel() {
    GlobalSettingsInfo globalSettingsInfo = _settingsProvider.getGlobalSettings();
    return globalSettingsInfo != null
        && globalSettingsInfo.getIntegrations().hasSlackSettings()
        ? globalSettingsInfo.getIntegrations().getSlackSettings().getDefaultChannelName()
        : null;
  }

  protected boolean hasParam(@Nullable final Map<String, String> params, final String param) {
    return params != null && params.containsKey(param);
  }

  protected List<NotificationRecipient> buildRecipients(NotificationScenarioType notificationScenarioType, Urn entityUrn) {
    List<NotificationRecipient> recipients = new ArrayList<>();

    // If we should globally broadcast, build the broadcast recipient.
    if (isEligibleForGlobalRecipients(notificationScenarioType)) {
      recipients.addAll(buildGlobalRecipients(notificationScenarioType));
    }

    // TODO: Support sending notifications to owners.
    if (isEligibleForOwnerRecipients(notificationScenarioType)) {
      recipients.addAll(buildOwnerRecipients(entityUrn));
    }

    // TODO: Support sending notifications to related (e.g. downstream) owners.
    if (isEligibleForRelatedOwnerRecipients(notificationScenarioType)) {
      recipients.addAll(buildRelatedOwnerRecipients(entityUrn));
    }

    // TODO: Support sending notifications to subscribers.
    if (isEligibleForSubscriberRecipients(notificationScenarioType)) {
      recipients.addAll(buildSubscriberRecipients(entityUrn));
    }

    return recipients;
  }

  protected List<NotificationRecipient> buildOwnerRecipients(final Urn entityUrn) {
    throw new NotImplementedException();
  }

  protected List<NotificationRecipient> buildRelatedOwnerRecipients(final Urn entityUrn) {
    throw new NotImplementedException();
  }

  protected List<NotificationRecipient> buildSubscriberRecipients(final Urn entityUrn) {
    throw new NotImplementedException();
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
      @Nonnull final Set<Urn> users,
      @Nonnull final Set<Urn> groups) {
    // Merge users + group users.
    final Set<Urn> finalUsers = new HashSet<>();
    finalUsers.addAll(users);
    finalUsers.addAll(groups.stream()
      .map(this::getGroupMembers)
      .flatMap(Collection::stream)
      .collect(Collectors.toList()));

    // Add recipient for each user.
    Set<NotificationRecipient> recipients = finalUsers.stream()
        .map(user -> new NotificationRecipient().setType(NotificationRecipientType.USER).setId(user.toString()))
        .collect(Collectors.toSet());
    return buildNotificationRequest(templateType, templateParams, recipients);
  }

  protected NotificationRequest buildNotificationRequest(
      @Nonnull final String templateType,
      @Nonnull final Map<String, String> templateParams,
      @Nonnull final Set<NotificationRecipient> recipients) {
    final NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(
        new NotificationMessage()
            .setTemplate(templateType)
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
              Map.Entry::getKey, entry -> new Ownership(entry.getValue().getAspects().get(OWNERSHIP_ASPECT_NAME).getValue()
                  .data())));

    } catch (Exception e) {
      log.error("Failed to batch fetch ownership!", e);
      return Collections.emptyMap();
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

  @Nonnull
  protected List<Urn> getGroupMembers(@Nonnull final Urn groupUrn) {
    try {
      // At max send notifications to first 500 group members.
      final EntityRelationships groupMemberEdges = _graphClient.getRelatedEntities(
          groupUrn.toString(),
          ImmutableList.of(Constants.IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME, Constants.IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME),
          RelationshipDirection.INCOMING,
          0,
          500,
          SYSTEM_ACTOR);

      return groupMemberEdges.getRelationships().stream().map(EntityRelationship::getEntity).collect(
          Collectors.toList());

    } catch (Exception e) {
      log.error(String.format("Failed to fetch membership for group %s. Skipping sending notification to group members!", groupUrn), e);
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
        log.warn(String.format("Failed to get aspect data for  urn %s aspect %s", urn.toString(), aspectName));
        return null;
      }
    } catch (Exception e) {
      log.error(String.format("Failed to get aspect data for  urn %s aspect %s", urn.toString(), aspectName));
      return null;
    }
  }

  protected void sendNotificationRequest(@Nonnull final NotificationRequest notificationRequest) {
    _eventProducer.producePlatformEvent(
        Constants.NOTIFICATION_REQUEST_EVENT_NAME,
        null,
        createPlatformEvent(notificationRequest)
    );
  }
}