package com.linkedin.metadata.kafka.hook.notification.ingestion;

import com.datahub.authentication.Authentication;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.provider.SettingsProvider;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.BaseMclNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.NotificationScenarioType;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * An extension of {@link BaseMclNotificationGenerator} which generates notifications
 * on ingestion run completion and creations.
 */
@Slf4j
public class IngestionNotificationGenerator extends BaseMclNotificationGenerator {

  public IngestionNotificationGenerator(
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

    log.debug(String.format("Found eligible Execution Request MCL. urn: %s", event.getEntityUrn()));

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
        && (ChangeType.UPSERT.equals(event.getChangeType()) || ChangeType.CREATE.equals(event.getChangeType()));
  }

  public void generateIngestionRunChangeNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ExecutionRequestResult result,
      @Nonnull final AuditStamp auditStamp) {

    final ExecutionRequestInput input = getExecutionRequestInput(urn);

    if (input == null) {
      // Just warn and return.
      log.warn("Failed to generate ingestion started notification! Could not find input aspect for exec request {}", urn);
      return;
    }

    if (!input.hasSource() || !input.getSource().hasIngestionSource()) {
      // Just warn and return.
      log.warn("Failed to generate ingestion started notification! Missing source or ingestion source urn for exec request {}", urn);
      return;
    }

    final Urn ingestionSourceUrn = input.getSource().getIngestionSource();
    final DataHubIngestionSourceInfo ingestionSourceInfo = getIngestionSourceInfo(ingestionSourceUrn);

    if (ingestionSourceInfo == null) {
      log.warn("Failed to resolve ingestion source info for ingestion source with urn {}. Skipping notification..", ingestionSourceUrn);
      return;
    }

    Set<NotificationRecipient> recipients = new HashSet<>(buildRecipients(NotificationScenarioType.INGESTION_RUN_CHANGE, ingestionSourceUrn));
    if (recipients.isEmpty()) {
      return;
    }

    final Map<String, String> templateParams = new HashMap<>();
    templateParams.put("sourceName", ingestionSourceInfo.getName());
    templateParams.put("sourceType", ingestionSourceInfo.getType());
    templateParams.put("statusText", mapStatus(result.getStatus()));
    templateParams.put("executionRequestUrn", urn.toString()); // For future.
    templateParams.put("ingestionSourceUrn", ingestionSourceUrn.toString()); // For future.

    final NotificationRequest notificationRequest = buildNotificationRequest(
        NotificationTemplateType.BROADCAST_INGESTION_RUN_CHANGE.name(),
        templateParams,
        recipients
    );

    log.info(String.format("Broadcasting ingestion run change for execution request %s, ingestion source %s...", urn, ingestionSourceUrn));
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
      default:
        throw new IllegalArgumentException(
            String.format("Failed to map execution request status %s. Unrecognized source type.", status));
    }
  }

  private ExecutionRequestInput getExecutionRequestInput(final Urn executionRequestUrn) {
    DataMap data = getAspectData(executionRequestUrn, Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
    if (data != null) {
      return new ExecutionRequestInput(data);
    }
    return null;
  }

  private DataHubIngestionSourceInfo getIngestionSourceInfo(final Urn ingestionSourceUrn) {
    DataMap data = getAspectData(ingestionSourceUrn, Constants.INGESTION_INFO_ASPECT_NAME);
    if (data != null) {
      return new DataHubIngestionSourceInfo(data);
    }
    return null;
  }
}