package com.linkedin.metadata.kafka.hook.notification.incident;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.notification.NotificationUtils.*;

import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentState;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.BaseMclNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.DownstreamSummary;
import com.linkedin.metadata.service.util.AssertionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.subscription.EntityChangeType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * An extension of {@link BaseMclNotificationGenerator} which generates notifications on incident
 * creation and status changes.
 */
@Slf4j
public class IncidentNotificationGenerator extends BaseMclNotificationGenerator {
  private final FeatureFlags _featureFlags;

  public IncidentNotificationGenerator(
      @Nonnull final OperationContext systemOpContext,
      @Nonnull final EventProducer eventProducer,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final SettingsProvider settingsProvider,
      @Nonnull final NotificationRecipientBuilders notificationRecipientBuilders,
      @Nonnull final FeatureFlags featureFlags) {
    super(
        systemOpContext,
        eventProducer,
        entityClient,
        graphClient,
        settingsProvider,
        notificationRecipientBuilders);
    _featureFlags = featureFlags;
  }

  @Override
  public boolean isEligibleForSubscriberRecipients() {
    return _featureFlags.isSubscriptionsEnabled();
  }

  @Override
  public void generate(@Nonnull MetadataChangeLog event) {
    if (!isEligibleForProcessing(event)) {
      return;
    }

    log.debug(String.format("Found eligible incident MCL. urn: %s", event.getEntityUrn()));

    if (isNewIncident(event)) {
      if (event.getAspect() == null) {
        return;
      }

      log.debug(
          String.format(
              "Found eligible new incident event to notify. urn: %s",
              event.getEntityUrn().toString()));

      sendNewIncidentNotifications(
          event.getEntityUrn(),
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              IncidentInfo.class));
    } else if (isIncidentStatusChanged(event)) {

      log.debug(
          String.format(
              "Found eligible incident status change event to notify. urn: %s",
              event.getEntityUrn().toString()));

      sendIncidentStatusChangeNotifications(
          event.getEntityUrn(),
          GenericRecordUtils.deserializeAspect(
              event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(),
              IncidentInfo.class),
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              IncidentInfo.class));

      // Enable or disable attempting to update the state of an incident we've already broadcasted!
      if (_featureFlags.isBroadcastNewIncidentUpdatesEnabled()) {
        sendBroadcastNewIncidentUpdateNotification(
            event.getEntityUrn(),
            GenericRecordUtils.deserializeAspect(
                event.getAspect().getValue(),
                event.getAspect().getContentType(),
                IncidentInfo.class));
      }
    }
  }

  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    if (event.getEntityUrn() == null) {
      return false;
    }
    return Constants.INCIDENT_INFO_ASPECT_NAME.equals(event.getAspectName())
        && (ChangeType.UPSERT.equals(event.getChangeType())
            || ChangeType.CREATE.equals(event.getChangeType()));
  }

  /** Sends a notification of template type "BROADCAST_NEW_INCIDENT" when an incident is created. */
  private void sendNewIncidentNotifications(
      @Nonnull final Urn urn, @Nonnull final IncidentInfo info) {

    log.debug(info.toString());

    final Urn entityUrn = info.getEntities().get(0);
    final Urn actorUrn = info.getStatus().getLastUpdated().getActor();

    EntityChangeType changeType = getEntityChangeType(info);

    Set<NotificationRecipient> recipients =
        new HashSet<>(
            buildRecipients(
                systemOpContext,
                NotificationScenarioType.NEW_INCIDENT,
                entityUrn,
                changeType,
                actorUrn));
    if (recipients.isEmpty()) {
      log.warn("Skipping incident notification generation - no recipients");
      return;
    }

    final Map<String, String> templateParams =
        buildBroadcastNewIncidentTemplateParams(urn, entityUrn, info);

    final NotificationRequest notificationRequest =
        buildNotificationRequest(
            NotificationTemplateType.BROADCAST_NEW_INCIDENT.name(), templateParams, recipients);

    log.info(String.format("Broadcasting new incident for entity %s...", entityUrn));
    sendNotificationRequest(notificationRequest);
  }

  /**
   * Sends a notification of template type "BROADCAST_INCIDENT_STATUS_CHANGE" when an incident's
   * status is changed.
   */
  private void sendIncidentStatusChangeNotifications(
      @Nonnull final Urn urn,
      @Nonnull final IncidentInfo prevInfo,
      @Nonnull final IncidentInfo newInfo) {

    // Notify a specific slack channel to alert the owners of the asset.
    final Urn entityUrn = newInfo.getEntities().get(0);
    final Urn actorUrn = newInfo.getStatus().getLastUpdated().getActor();
    EntityChangeType changeType = getEntityChangeType(newInfo);

    Set<NotificationRecipient> recipients =
        new HashSet<>(
            buildRecipients(
                systemOpContext,
                NotificationScenarioType.INCIDENT_STATUS_CHANGE,
                entityUrn,
                changeType,
                actorUrn));
    if (recipients.isEmpty()) {
      log.info("Skipping incident generation - no recipients");
      return;
    }

    final Ownership maybeOwnership = getEntityOwnership(entityUrn);
    final List<Urn> owners =
        maybeOwnership != null
            ? maybeOwnership.getOwners().stream().map(Owner::getOwner).collect(Collectors.toList())
            : Collections.emptyList();
    final DownstreamSummary downstreamSummary = getDownstreamSummary(entityUrn);
    final Map<String, String> templateParams = new HashMap<>();
    final String entityName = _entityNameProvider.getQualifiedName(systemOpContext, entityUrn);
    final String entityType = _entityNameProvider.getTypeName(systemOpContext, entityUrn);
    final String entityPlatform = _entityNameProvider.getPlatformName(systemOpContext, entityUrn);
    final String actorName =
        _entityNameProvider.getName(
            systemOpContext, newInfo.getStatus().getLastUpdated().getActor());
    templateParams.put("incidentUrn", urn.toString());
    templateParams.put("entityUrn", entityUrn.toString());
    templateParams.put("entityPath", generateEntityPath(entityUrn));
    templateParams.put("entityName", entityName);
    templateParams.put("entityType", entityType);
    templateParams.put("newStatus", newInfo.getStatus().getState().toString());
    templateParams.put("prevStatus", prevInfo.getStatus().getState().toString());
    templateParams.put("owners", listToJSON(owners));
    if (downstreamSummary != null) {
      final List<Urn> downstreamOwners = downstreamSummary.getOwnerUrns();
      final Integer downstreamAssetCount = downstreamSummary.getTotal();
      templateParams.put("downstreamOwners", listToJSON(downstreamOwners));
      templateParams.put("downstreamAssetCount", downstreamAssetCount.toString());
    }
    templateParams.put("actorUrn", newInfo.getStatus().getLastUpdated().getActor().toString());
    templateParams.put("actorName", actorName);
    if (entityPlatform != null) {
      templateParams.put("entityPlatform", entityPlatform);
    }
    if (newInfo.getStatus().hasMessage()) {
      templateParams.put("message", newInfo.getStatus().getMessage());
    }
    if (newInfo.hasTitle()) {
      templateParams.put("incidentTitle", newInfo.getTitle());
    }
    if (newInfo.hasDescription()) {
      templateParams.put("incidentDescription", newInfo.getDescription());
    }
    if (newInfo.hasPriority()) {
      templateParams.put("incidentPriority", newInfo.getPriority().toString());
    }
    if (newInfo.getStatus().hasStage()) {
      templateParams.put("incidentStage", newInfo.getStatus().getStage().toString());
    }
    if (newInfo.hasType()) {
      if (newInfo.hasCustomType()) {
        templateParams.put("incidentType", newInfo.getCustomType());
      } else {
        templateParams.put("incidentType", newInfo.getType().toString());
      }
    }
    if (newInfo.hasSource()
        && IncidentSourceType.ASSERTION_FAILURE.equals(newInfo.getSource().getType())) {
      Urn assertionUrn = newInfo.getSource().getSourceUrn();
      templateParams.put("assertionUrn", assertionUrn.toString());
      // Attempt to populate the assertion description.
      DataMap rawInfo = getAspectData(assertionUrn, ASSERTION_INFO_ASPECT_NAME);
      if (rawInfo != null) {
        AssertionInfo aspectInfo = new AssertionInfo(rawInfo);
        templateParams.put(
            "assertionDescription",
            AssertionUtils.buildAssertionDescription(assertionUrn, aspectInfo));
        templateParams.put("assertionType", aspectInfo.getType().toString());
        if (aspectInfo.getSource() != null) {
          templateParams.put("assertionSourceType", aspectInfo.getSource().getType().toString());
        }
      }
    }

    final NotificationRequest notificationRequest =
        buildNotificationRequest(
            NotificationTemplateType.BROADCAST_INCIDENT_STATUS_CHANGE.name(),
            templateParams,
            recipients);

    log.info(String.format("Broadcasting incident status change for entity %s...", entityUrn));
    sendNotificationRequest(notificationRequest);
  }

  /**
   * Sends a notification of template type "BROADCAST_NEW_INCIDENT_UPDATE" when an incident's status
   * is changed.
   *
   * <p>This is used to update the incident status in the destination, as opposed to sending a net
   * new notification.
   */
  private void sendBroadcastNewIncidentUpdateNotification(
      @Nonnull final Urn urn, @Nonnull final IncidentInfo newInfo) {

    // Notify a specific slack channel to alert the owners of the asset.
    final Urn entityUrn = newInfo.getEntities().get(0);
    final Urn actorUrn = newInfo.getStatus().getLastUpdated().getActor();
    EntityChangeType changeType = getEntityChangeType(newInfo);

    // Anyone who has received broadcast new incident may be interested in this update -
    // you do not need to specifically subscribe to receive this special broadcast type.
    Set<NotificationRecipient> recipients =
        new HashSet<>(
            buildRecipients(
                systemOpContext,
                NotificationScenarioType.NEW_INCIDENT,
                entityUrn,
                changeType,
                actorUrn));

    if (recipients.isEmpty()) {
      log.info("Skipping incident generation - no recipients");
      return;
    }

    final Map<String, String> templateParams =
        buildBroadcastNewIncidentTemplateParams(urn, entityUrn, newInfo);

    final NotificationRequest notificationRequest =
        buildNotificationRequest(
            NotificationTemplateType.BROADCAST_NEW_INCIDENT_UPDATE.name(),
            templateParams,
            recipients);

    log.info(String.format("Updating broadcasted incident for entity %s...", entityUrn));
    sendNotificationRequest(notificationRequest);
  }

  private Map<String, String> buildBroadcastNewIncidentTemplateParams(
      @Nonnull final Urn urn, @Nonnull Urn entityUrn, @Nonnull final IncidentInfo info) {
    final Map<String, String> templateParams = new HashMap<>();
    final Ownership maybeOwnership = getEntityOwnership(entityUrn);
    final List<Urn> owners =
        maybeOwnership != null
            ? maybeOwnership.getOwners().stream().map(Owner::getOwner).collect(Collectors.toList())
            : Collections.emptyList();
    final DownstreamSummary downstreamSummary = getDownstreamSummary(entityUrn);
    final String entityName = _entityNameProvider.getQualifiedName(systemOpContext, entityUrn);
    final String entityType = _entityNameProvider.getTypeName(systemOpContext, entityUrn);
    final String entityPlatform = _entityNameProvider.getPlatformName(systemOpContext, entityUrn);
    final String actorName =
        _entityNameProvider.getName(systemOpContext, info.getStatus().getLastUpdated().getActor());
    templateParams.put("incidentUrn", urn.toString());
    templateParams.put("entityUrn", entityUrn.toString());
    templateParams.put("entityName", entityName);
    templateParams.put("entityType", entityType);
    templateParams.put("entityPath", generateEntityPath(entityUrn));
    templateParams.put("newStatus", info.getStatus().getState().toString());
    templateParams.put("owners", listToJSON(owners));
    if (downstreamSummary != null) {
      final List<Urn> downstreamOwners = downstreamSummary.getOwnerUrns();
      final Integer downstreamAssetCount = downstreamSummary.getTotal();
      templateParams.put("downstreamOwners", listToJSON(downstreamOwners));
      templateParams.put("downstreamAssetCount", downstreamAssetCount.toString());
    }
    templateParams.put("actorUrn", info.getStatus().getLastUpdated().getActor().toString());
    templateParams.put("actorName", actorName);
    if (entityPlatform != null) {
      templateParams.put("entityPlatform", entityPlatform);
    }
    if (info.hasTitle()) {
      templateParams.put("incidentTitle", info.getTitle());
    }
    if (info.hasDescription()) {
      templateParams.put("incidentDescription", info.getDescription());
    }
    if (info.hasPriority()) {
      templateParams.put("incidentPriority", info.getPriority().toString());
    }
    if (info.getStatus().hasStage()) {
      templateParams.put("incidentStage", info.getStatus().getStage().toString());
    }
    if (info.hasType()) {
      if (info.hasCustomType()) {
        templateParams.put("incidentType", info.getCustomType());
      } else {
        templateParams.put("incidentType", info.getType().toString());
      }
    }
    if (info.hasSource()
        && IncidentSourceType.ASSERTION_FAILURE.equals(info.getSource().getType())) {
      Urn assertionUrn = info.getSource().getSourceUrn();
      templateParams.put("assertionUrn", assertionUrn.toString());
      // Attempt to populate the assertion description.
      DataMap rawInfo = getAspectData(assertionUrn, ASSERTION_INFO_ASPECT_NAME);
      if (rawInfo != null) {
        AssertionInfo aspectInfo = new AssertionInfo(rawInfo);
        templateParams.put(
            "assertionDescription",
            AssertionUtils.buildAssertionDescription(assertionUrn, aspectInfo));
        templateParams.put("assertionType", aspectInfo.getType().toString());
        if (aspectInfo.getSource() != null) {
          templateParams.put("assertionSourceType", aspectInfo.getSource().getType().toString());
        }
      }
    }
    return templateParams;
  }

  private EntityChangeType getEntityChangeType(@Nonnull final IncidentInfo info) {
    return info.getStatus().getState().equals(IncidentState.ACTIVE)
        ? EntityChangeType.INCIDENT_RAISED
        : EntityChangeType.INCIDENT_RESOLVED;
  }

  private boolean isNewIncident(final MetadataChangeLog event) {
    return event.getPreviousAspectValue() == null;
  }

  private boolean isIncidentStatusChanged(final MetadataChangeLog event) {
    if (event.getAspect() == null || event.getPreviousAspectValue() == null) {
      return false;
    }
    final IncidentInfo prevInfo =
        GenericRecordUtils.deserializeAspect(
            event.getPreviousAspectValue().getValue(),
            event.getPreviousAspectValue().getContentType(),
            IncidentInfo.class);
    final IncidentInfo newInfo =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(), event.getAspect().getContentType(), IncidentInfo.class);
    return !prevInfo.getStatus().getState().equals(newInfo.getStatus().getState());
  }

  private String listToJSON(final List<?> list) {
    ObjectMapper objectMapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    try {
      return objectMapper.writeValueAsString(
          list.stream().map(Object::toString).collect(Collectors.toList()));
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot convert list to JSON array", e);
    }
  }
}
