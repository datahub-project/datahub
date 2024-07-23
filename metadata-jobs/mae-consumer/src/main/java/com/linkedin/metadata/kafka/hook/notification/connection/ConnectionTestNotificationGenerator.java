package com.linkedin.metadata.kafka.hook.notification.connection;

import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.NotificationRecipientBuilder;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.BaseMclNotificationGenerator;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.integrations.invoker.JSON;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * An extension of {@link BaseMclNotificationGenerator} which generates notifications on
 * DataHubConnectionTestEvents
 */
@Slf4j
public class ConnectionTestNotificationGenerator extends BaseMclNotificationGenerator {

  private final Urn SLACK_CONNECTION_URN = UrnUtils.getUrn(Constants.SLACK_CONNECTION_ID);
  private final boolean isEnabled;

  public ConnectionTestNotificationGenerator(
      @Nonnull final OperationContext systemOpContext,
      @Nonnull final EventProducer eventProducer,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final SettingsProvider settingsProvider,
      @Nonnull final NotificationRecipientBuilders notificationRecipientBuilders,
      @Nonnull final Boolean isEnabled) {
    super(
        systemOpContext,
        eventProducer,
        entityClient,
        graphClient,
        settingsProvider,
        notificationRecipientBuilders);
    this.isEnabled = isEnabled;
  }

  @Override
  public void generate(@Nonnull MetadataChangeLog event) {
    if (!isEligibleForProcessing(event)) {
      log.debug(
          String.format(
              "Event is ineligible for processing by ConnectionTestNotificationGenerator, urn %s",
              event.getEntityUrn()));
      return;
    }
    // re-asserting here to avoid IDE warnings
    if (event.getEntityUrn() == null || event.getAspect() == null) {
      return;
    }

    log.debug(
        String.format("Found eligible dataHubExecutionRequest MCL. urn: %s", event.getEntityUrn()));

    sendNewTestEventNotifications(
        event.getEntityUrn(),
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            ExecutionRequestInput.class));
  }

  private void sendNewTestEventNotifications(
      @Nonnull final Urn requestUrn, @Nonnull final ExecutionRequestInput requestInput) {
    log.debug(requestInput.toString());

    final Urn connectionUrn = requestInput.getSource().getConnection();
    if (connectionUrn == null) {
      log.warn(
          String.format(
              "dataHubExecutionRequest with urn %s missing connectionSource", requestUrn));
      return;
    }

    if (connectionUrn.equals(SLACK_CONNECTION_URN)) {
      sendNewSlackTestEventNotification(requestUrn, requestInput);
    } else {
      log.warn(String.format("No test event handler for connection with urn %s", connectionUrn));
    }
  }

  private void sendNewSlackTestEventNotification(
      @Nonnull final Urn requestUrn, @Nonnull final ExecutionRequestInput requestInput) {
    StringMap args = requestInput.getArgs();
    String title = args.get("title");
    String body = args.get("body");
    String slackChannelsJSON = args.get("channelsJSON");
    StringArray slackChannels =
        slackChannelsJSON != null ? JSON.deserialize(slackChannelsJSON, StringArray.class) : null;
    String slackUserHandle = args.get("userHandle");
    if (Objects.isNull(title) || Objects.isNull(body)) {
      log.warn(
          String.format(
              "Execution request for slack connection missing title or body in the args, requestUrn=%s. Skipping notification generation.",
              requestUrn));
      return;
    }
    if (Objects.isNull(slackChannels) && Objects.isNull(slackUserHandle)) {
      log.warn(
          String.format(
              "Execution request for slack connection must have channel or userHandle defined in the args, requestUrn=%s. Skipping notification generation.",
              requestUrn));
      return;
    }

    final NotificationRecipientBuilder builder =
        _recipientBuilders.getBuilder(NotificationSinkType.SLACK);
    final Set<NotificationRecipient> notificationRecipients = new HashSet<>();
    if (!Objects.isNull(slackUserHandle)) {
      notificationRecipients.add(
          builder.buildRecipient(NotificationRecipientType.SLACK_DM, slackUserHandle, null));
    } else {
      slackChannels.forEach(
          channel ->
              notificationRecipients.add(
                  builder.buildRecipient(NotificationRecipientType.SLACK_CHANNEL, channel, null)));
    }

    final Map<String, String> templateParams = new HashMap<>();
    templateParams.put("title", title);
    templateParams.put("body", body);
    templateParams.put(
        Constants.NOTIFICATION_CONNECTION_TEST_EXECUTION_REQUEST_URN_PARAM_KEY,
        requestUrn.toString());
    templateParams.put("requestName", Constants.NOTIFICATION_CONNECTION_TEST_REQUEST_TEMPLATE_NAME);
    final NotificationRequest notificationRequest =
        buildNotificationRequest(
            NotificationTemplateType.CUSTOM.name(), templateParams, notificationRecipients);
    sendNotificationRequest(notificationRequest);
  }

  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    if (!isEnabled) {
      return false;
    }
    if (event.getEntityUrn() == null || event.getAspect() == null) {
      return false;
    }
    final boolean isValidAspect =
        Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME.equals(event.getAspectName())
            && (ChangeType.CREATE.equals(event.getChangeType())
                || ChangeType.UPSERT.equals(event.getChangeType()));
    if (!isValidAspect) {
      return false;
    }
    ExecutionRequestInput requestInput =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            ExecutionRequestInput.class);
    return requestInput
        .getTask()
        .equals(Constants.NOTIFICATION_CONNECTION_TEST_EXECUTION_REQUEST_TASK_NAME);
  }
}
