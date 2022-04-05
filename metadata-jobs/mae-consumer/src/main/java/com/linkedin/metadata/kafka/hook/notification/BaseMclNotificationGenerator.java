package com.linkedin.metadata.kafka.hook.notification;

import com.datahub.authentication.Authentication;
import com.datahub.notification.SettingsProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
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

    final NotificationRequest notificationRequest = new NotificationRequest();
    notificationRequest.setMessage(
        new NotificationMessage()
            .setTemplate(templateType)
            .setParameters(new StringMap(templateParams))
    );

    List<NotificationRecipient> recipients = new ArrayList<>();

    // Add user recipients.
    recipients.addAll(users.stream()
      .map(user -> new NotificationRecipient()
          .setType(NotificationRecipientType.USER)
          .setId(user.toString()))
      .collect(Collectors.toList()));

    // Add group recipients. Notice that this hydrates group membership on the fly.
    recipients.addAll(groups.stream()
        .map(this::getGroupMembers)
        .flatMap(Collection::stream)
        .map(user -> new NotificationRecipient()
            .setType(NotificationRecipientType.USER)
            .setId(user.toString()))
        .collect(Collectors.toList()));

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
    final EntityLineageResult results;
    results = _graphClient.getLineageEntities(
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
  }

  @Nonnull
  protected List<Urn> getGroupMembers(@Nonnull final Urn groupUrn) {
    try {
      // At max send notifications to first 500 group members.
      final EntityRelationships groupMemberEdges = _graphClient.getRelatedEntities(
          groupUrn.toString(),
          ImmutableList.of(Constants.GROUP_MEMBERSHIP_RELATIONSHIP_NAME),
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

  protected void sendNotificationRequest(@Nonnull final NotificationRequest notificationRequest) {
    _eventProducer.producePlatformEvent(
        Constants.NOTIFICATION_REQUEST_EVENT_NAME,
        null,
        createPlatformEvent(notificationRequest)
    );
  }
}