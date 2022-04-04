package com.linkedin.metadata.kafka.hook.notification.incident;

import com.datahub.authentication.Authentication;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.SettingsProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.BaseMclNotificationGenerator;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.settings.NotificationSettingValue;
import com.linkedin.settings.global.GlobalSettingsInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.kafka.hook.notification.NotificationUtils.*;


/**
 * An extension of {@link BaseMclNotificationGenerator} which generates notifications
 * on incident creation and status changes.
 */
@Slf4j
public class IncidentNotificationGenerator extends BaseMclNotificationGenerator {

  public IncidentNotificationGenerator(
      @Nonnull final EventProducer eventProducer,
      @Nonnull final EntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final SettingsProvider settingsProvider,
      @Nonnull final Authentication systemAuthentication) {
    super(eventProducer, entityClient, graphClient, settingsProvider, systemAuthentication);
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

      log.debug(String.format("Found eligible new incident event to notify. urn: %s", event.getEntityUrn().toString()));

      generateNewIncidentNotifications(
          event.getEntityUrn(),
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              IncidentInfo.class)
      );
    } else if (isIncidentStatusChanged(event)) {

      log.debug(String.format("Found eligible incident status change event to notify. urn: %s", event.getEntityUrn().toString()));

      generateUpdatedIncidentNotifications(
          event.getEntityUrn(),
          GenericRecordUtils.deserializeAspect(
              event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(),
              IncidentInfo.class),
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              IncidentInfo.class)
      );
    }
  }

  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    if (event.getEntityUrn() == null) {
      return false;
    }
    return Constants.INCIDENT_INFO_ASPECT_NAME.equals(event.getAspectName())
        && (ChangeType.UPSERT.equals(event.getChangeType()) || ChangeType.CREATE.equals(event.getChangeType()));
  }

  public void generateNewIncidentNotifications(
      @Nonnull final Urn urn,
      @Nonnull final IncidentInfo info) {
    // TODO: Support other notification types on new incidents - e.g. notify owners directly.
    if (shouldBroadcastNewIncident()) {
      // Broadcast new incident.
      broadcastNewIncident(urn, info);
    }
  }

  public void generateUpdatedIncidentNotifications(
      @Nonnull final Urn urn,
      @Nonnull final IncidentInfo prevInfo,
      @Nonnull final IncidentInfo newInfo) {
    // TODO: Support other notification types on updated incidents - e.g. notify owners directly.
      if (shouldBroadcastIncidentStatusChange()) {
        // Broadcast incident status change.
        broadcastIncidentStatusChange(urn, prevInfo, newInfo);
      }
  }

  /**
   * Sends a notification of template type "BROADCAST_NEW_INCIDENT" when an incident is created.
   */
  private void broadcastNewIncident(
      @Nonnull final Urn urn,
      @Nonnull final IncidentInfo info) {

    log.info(info.toString());

    // Notify a specific slack channel to alert the owners of the asset.
    final Urn entityUrn = info.getEntities().get(0);
    final Ownership maybeOwnership = getEntityOwnership(entityUrn);
    final List<Urn> owners = maybeOwnership != null
        ? maybeOwnership
        .getOwners()
        .stream()
        .map(Owner::getOwner)
        .collect(Collectors.toList())
        : Collections.emptyList();
    final List<Urn> downstreamOwners = getDownstreamOwners(entityUrn);

    final Map<String, String> templateParams = new HashMap<>();
    templateParams.put("incidentUrn", urn.toString());
    templateParams.put("entityUrn", entityUrn.toString());
    templateParams.put("entityType", entityUrn.getEntityType());
    templateParams.put("entityPath", generateEntityPath(entityUrn));
    templateParams.put("newStatus", info.getStatus().getState().toString());
    templateParams.put("owners", listToJSON(owners));
    templateParams.put("downstreamOwners", listToJSON(downstreamOwners));
    templateParams.put("actorUrn", info.getStatus().getLastUpdated().getActor().toString());
    if (info.hasTitle()) {
      templateParams.put("incidentTitle", info.getTitle());
    }
    if (info.hasDescription()) {
      templateParams.put("incidentDescription", info.getDescription());
    }

    final NotificationRequest notificationRequest = buildNotificationRequest(
        NotificationTemplateType.BROADCAST_NEW_INCIDENT.name(),
        templateParams,
        Collections.emptyList(),
        Collections.emptyList()
    );

    log.info(String.format("Broadcasting new incident for entity %s...", entityUrn));
    sendNotificationRequest(notificationRequest);
  }

  /**
   * Sends a notification of template type "BROADCAST_INCIDENT_STATUS_CHANGE" when an incident's status is changed.
   */
  private void broadcastIncidentStatusChange(
      @Nonnull final Urn urn,
      @Nonnull final IncidentInfo prevInfo,
      @Nonnull final IncidentInfo newInfo) {

    // Notify a specific slack channel to alert the owners of the asset.
    final Urn entityUrn = newInfo.getEntities().get(0);
    final Ownership maybeOwnership = getEntityOwnership(entityUrn);
    final List<Urn> owners = maybeOwnership != null
        ? maybeOwnership
        .getOwners()
        .stream()
        .map(Owner::getOwner)
        .collect(Collectors.toList())
        : Collections.emptyList();
    final List<Urn> downstreamOwners = getDownstreamOwners(entityUrn);

    final Map<String, String> templateParams = new HashMap<>();
    templateParams.put("incidentUrn", urn.toString());
    templateParams.put("entityUrn", entityUrn.toString());
    templateParams.put("entityPath", generateEntityPath(entityUrn));
    templateParams.put("newStatus", newInfo.getStatus().getState().toString());
    templateParams.put("prevStatus", prevInfo.getStatus().getState().toString());
    templateParams.put("owners", listToJSON(owners));
    templateParams.put("downstreamOwners", listToJSON(downstreamOwners));
    templateParams.put("actorUrn", newInfo.getStatus().getLastUpdated().getActor().toString());
    if (newInfo.getStatus().hasMessage()) {
      templateParams.put("message", newInfo.getStatus().getMessage());
    }
    if (newInfo.hasTitle()) {
      templateParams.put("incidentTitle", newInfo.getTitle());
    }
    if (newInfo.hasDescription()) {
      templateParams.put("incidentDescription", newInfo.getDescription());
    }

    final NotificationRequest notificationRequest = buildNotificationRequest(
        NotificationTemplateType.BROADCAST_INCIDENT_STATUS_CHANGE.name(),
        templateParams,
        Collections.emptyList(), // no users or groups targeted specifically since its broadcast
        Collections.emptyList()
    );

    log.info(String.format("Broadcasting incident status change for entity %s...", entityUrn));
    sendNotificationRequest(notificationRequest);

  }

  /**
   * Returns true if the incident notification of type "broadcast new incident" has been enabled in the settings.
   */
  private boolean shouldBroadcastNewIncident() {
    GlobalSettingsInfo globalSettingsInfo = _settingsProvider.getGlobalSettings();
    return globalSettingsInfo != null
        && globalSettingsInfo.getIntegrations().getSlackSettings().isEnabled()
        && globalSettingsInfo.getNotifications().hasIncidents()
        && globalSettingsInfo.getNotifications().getIncidents().hasBroadcastNewIncidentNotification()
        && NotificationSettingValue.ENABLED.equals(
            globalSettingsInfo.getNotifications().getIncidents().getBroadcastNewIncidentNotification().getValue());
  }

  /**
   * Returns true if the incident notification of type "broadcast incident status change" has been enabled in the settings.
   */
  private boolean shouldBroadcastIncidentStatusChange() {
    GlobalSettingsInfo globalSettingsInfo = _settingsProvider.getGlobalSettings();
    return globalSettingsInfo != null
        && globalSettingsInfo.getIntegrations().getSlackSettings().isEnabled()
        && globalSettingsInfo.getNotifications().hasIncidents()
        && globalSettingsInfo.getNotifications().getIncidents().hasBroadcastIncidentStatusChangeNotification()
        && NotificationSettingValue.ENABLED.equals(
            globalSettingsInfo.getNotifications().getIncidents().getBroadcastIncidentStatusChangeNotification().getValue());
  }

  private boolean isNewIncident(final MetadataChangeLog event) {
    return event.getPreviousAspectValue() == null;
  }

  private boolean isIncidentStatusChanged(final MetadataChangeLog event) {
    if (event.getAspect() == null || event.getPreviousAspectValue() == null) {
      return false;
    }
    final IncidentInfo prevInfo = GenericRecordUtils.deserializeAspect(
        event.getPreviousAspectValue().getValue(),
        event.getPreviousAspectValue().getContentType(),
        IncidentInfo.class);
    final IncidentInfo newInfo = GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(),
        event.getAspect().getContentType(),
        IncidentInfo.class);
    return !prevInfo.getStatus().getState().equals(newInfo.getStatus().getState());
  }

  private String listToJSON(final List<?> list) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.writeValueAsString(list.stream().map(Object::toString).collect(Collectors.toList()));
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot convert list to JSON array", e);
    }
  }
}