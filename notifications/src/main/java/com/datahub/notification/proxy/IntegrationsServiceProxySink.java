package com.datahub.notification.proxy;

import com.datahub.notification.NotificationContext;
import com.datahub.notification.NotificationSink;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationTemplateType;
import com.google.common.collect.ImmutableList;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.metadata.integration.IntegrationsService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

/**
 * This class routes notifications to the datahub-integrations-service python service for any sinks
 * that are defined there, currently email.
 */
@Slf4j
public abstract class IntegrationsServiceProxySink implements NotificationSink {

  /** A list of notification templates supported by this sink. */
  private static final List<NotificationTemplateType> SUPPORTED_TEMPLATES =
      ImmutableList.of(
          NotificationTemplateType.CUSTOM,
          NotificationTemplateType.BROADCAST_NEW_INCIDENT,
          NotificationTemplateType.BROADCAST_INCIDENT_STATUS_CHANGE,
          NotificationTemplateType.BROADCAST_NEW_PROPOSAL,
          NotificationTemplateType.BROADCAST_PROPOSAL_STATUS_CHANGE,
          NotificationTemplateType.BROADCAST_ENTITY_CHANGE,
          NotificationTemplateType.BROADCAST_INGESTION_RUN_CHANGE,
          NotificationTemplateType.BROADCAST_ASSERTION_STATUS_CHANGE);

  /** A list of recipient types that can be handled by the sink */
  private static final List<NotificationRecipientType> RECIPIENT_TYPES =
      ImmutableList.of(NotificationRecipientType.EMAIL, NotificationRecipientType.CUSTOM);

  private IntegrationsService integrationsService;

  @Override
  public NotificationSinkType type() {
    return NotificationSinkType.EMAIL;
  }

  @Override
  public Collection<NotificationTemplateType> templates() {
    return SUPPORTED_TEMPLATES;
  }

  @Override
  public Collection<NotificationRecipientType> recipientTypes() {
    return RECIPIENT_TYPES;
  }

  @Override
  public void init(@NotNull NotificationSinkConfig cfg) {
    log.info("Initializing IntegrationsServiceProxySink...");
    this.integrationsService = cfg.getIntegrationsService();
  }

  @Override
  public void send(
      @NotNull OperationContext opContext,
      @NotNull NotificationRequest request,
      @NotNull NotificationContext context)
      throws Exception {
    log.debug("Received request to proxy notification to integrations service: {}", request);
    this.integrationsService.sendNotification(request);
  }
}
