package com.linkedin.metadata.kafka.hook.notification.ingestion;

import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSourceType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.BaseMclNotificationGenerator;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * An extension of {@link BaseMclNotificationGenerator} which generates notifications on ingestion
 * run completion and creations.
 */
@Slf4j
public class IngestionNotificationGenerator extends BaseMclNotificationGenerator {

  public IngestionNotificationGenerator(
      @Nonnull OperationContext systemOpContext,
      @Nonnull final EventProducer eventProducer,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final SettingsService settingsService,
      @Nonnull final NotificationRecipientBuilders notificationRecipientBuilders) {
    super(
        systemOpContext,
        eventProducer,
        entityClient,
        graphClient,
        settingsService,
        notificationRecipientBuilders);
  }

  @Override
  public void generate(@Nonnull MetadataChangeLog event) {
    if (!isEligibleForProcessing(event)) {
      return;
    }

    log.debug("Found eligible ingestion Execution Request MCL. urn: {}", event.getEntityUrn());

    generateIngestionRunChangeNotifications(
        event.getEntityUrn(),
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            ExecutionRequestResult.class),
        event.getCreated() // Should always be present.
        );
  }

  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    if (event.getEntityUrn() == null) {
      return false;
    }
    return Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME.equals(event.getAspectName())
        && (ChangeType.UPSERT.equals(event.getChangeType())
            || ChangeType.CREATE.equals(event.getChangeType()))
        && !isRunProgressUpdate(
            event); // If there is a previous result, we've already sent a notification.
  }

  public void generateIngestionRunChangeNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ExecutionRequestResult result,
      @Nonnull final AuditStamp auditStamp) {

    final ExecutionRequestInput input = getExecutionRequestInput(urn);

    if (input == null) {
      // Just warn and return.
      log.warn(
          "Failed to generate ingestion started notification! Could not find input aspect for exec request {}",
          urn);
      return;
    }

    if (!input.hasSource() || !input.getSource().hasIngestionSource()) {
      // Just warn and return.
      log.warn(
          "Failed to generate ingestion started notification! Missing source or ingestion source urn for exec request {}",
          urn);
      return;
    }

    final Urn ingestionSourceUrn = input.getSource().getIngestionSource();
    final DataHubIngestionSourceInfo ingestionSourceInfo =
        getIngestionSourceInfo(ingestionSourceUrn);

    if (ingestionSourceInfo == null) {
      log.warn(
          "Failed to resolve ingestion source info for ingestion source with urn {}. Skipping notification..",
          ingestionSourceUrn);
      return;
    }

    if (ingestionSourceInfo.hasSource()
        && ingestionSourceInfo
            .getSource()
            .getType()
            .equals(DataHubIngestionSourceSourceType.SYSTEM)) {
      log.debug(
          "Skipping notifications for system ingestion source with urn {}", ingestionSourceUrn);
      return;
    }

    final Map<String, String> templateParams = new HashMap<>();
    templateParams.put("sourceName", ingestionSourceInfo.getName());
    templateParams.put("sourceType", ingestionSourceInfo.getType());
    templateParams.put("statusText", mapStatus(result.getStatus()));
    templateParams.put("executionRequestUrn", urn.toString()); // For future.
    templateParams.put("ingestionSourceUrn", ingestionSourceUrn.toString()); // For future.

    sendForIngestionRunChange(
        templateParams, ingestionSourceUrn, urn, NotificationScenarioType.INGESTION_RUN_CHANGE);
    if (Constants.EXECUTION_REQUEST_STATUS_FAILURE.equals(result.getStatus())) {
      sendForIngestionRunChange(
          templateParams, ingestionSourceUrn, urn, NotificationScenarioType.INGESTION_FAILURE);
    }
  }

  private void sendForIngestionRunChange(
      final Map<String, String> templateParams,
      Urn ingestionSourceUrn,
      Urn executionRequestUrn,
      NotificationScenarioType scenarioType) {
    Set<NotificationRecipient> recipients =
        new HashSet<>(buildRecipients(systemOpContext, scenarioType, ingestionSourceUrn, null));
    if (recipients.isEmpty()) {
      log.warn(
          "Found empty recipients for ingestion source notification for urn {}. Skipping sending..",
          ingestionSourceUrn);
      return;
    }
    final NotificationRequest notificationRequest =
        buildNotificationRequest(
            NotificationTemplateType.BROADCAST_INGESTION_RUN_CHANGE.name(),
            templateParams,
            recipients);
    log.debug(
        "Broadcasting ingestion run change for execution request {}, ingestion source {}...",
        executionRequestUrn,
        ingestionSourceUrn);
    sendNotificationRequest(notificationRequest);
  }

  public String mapStatus(@Nonnull final String status) {
    switch (status) {
      case Constants.EXECUTION_REQUEST_STATUS_RUNNING:
        return "started";
      case Constants.EXECUTION_REQUEST_STATUS_TIMEOUT:
        return "timed out";
      case Constants.EXECUTION_REQUEST_STATUS_CANCELLED:
        return "been cancelled";
      case Constants.EXECUTION_REQUEST_STATUS_FAILURE:
        return "failed";
      case Constants.EXECUTION_REQUEST_STATUS_SUCCESS:
        return "completed successfully";
      case Constants.EXECUTION_REQUEST_STATUS_ABORTED:
        return "aborted";
      case Constants.EXECUTION_REQUEST_STATUS_DUPLICATE:
        return "duplicate";
      default:
        throw new IllegalArgumentException(
            String.format(
                "Failed to map execution request status %s. Unrecognized source type.", status));
    }
  }

  private ExecutionRequestInput getExecutionRequestInput(final Urn executionRequestUrn) {
    DataMap data =
        getAspectData(executionRequestUrn, Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
    if (data != null) {
      return new ExecutionRequestInput(data);
    }
    return null;
  }

  private boolean isRunProgressUpdate(final MetadataChangeLog event) {
    final ExecutionRequestResult result =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            ExecutionRequestResult.class);
    // If the run is in RUNNING state and there has already been at least one "running" status
    // before,
    // then we are just dealing with a run progress update. We don't send notifications here since
    // this happens
    // every minute.
    return event.hasPreviousAspectValue()
        && Constants.EXECUTION_REQUEST_STATUS_RUNNING.equals(result.getStatus());
  }

  private DataHubIngestionSourceInfo getIngestionSourceInfo(final Urn ingestionSourceUrn) {
    DataMap data = getAspectData(ingestionSourceUrn, Constants.INGESTION_INFO_ASPECT_NAME);
    if (data != null) {
      return new DataHubIngestionSourceInfo(data);
    }
    return null;
  }
}
