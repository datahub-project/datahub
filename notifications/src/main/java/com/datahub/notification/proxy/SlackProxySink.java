package com.datahub.notification.proxy;

import com.datahub.notification.NotificationTemplateType;
import com.google.common.collect.ImmutableList;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationSinkType;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * This class routes notifications to the datahub-integrations-service python service for any sinks
 * that are defined there, currently email.
 */
@Slf4j
public class SlackProxySink extends IntegrationsServiceProxySink {

  /** A list of notification templates supported by this sink. */
  private static final List<NotificationTemplateType> SUPPORTED_TEMPLATES =
      ImmutableList.of(
          NotificationTemplateType.BROADCAST_NEW_INCIDENT,
          NotificationTemplateType.BROADCAST_NEW_INCIDENT_UPDATE,
          NotificationTemplateType.BROADCAST_INCIDENT_STATUS_CHANGE,
          NotificationTemplateType.BROADCAST_COMPLIANCE_FORM_PUBLISH);

  /** A list of recipient types that can be handled by the sink */
  private static final List<NotificationRecipientType> RECIPIENT_TYPES =
      ImmutableList.of(NotificationRecipientType.SLACK_DM, NotificationRecipientType.SLACK_CHANNEL);

  @Override
  public NotificationSinkType type() {
    return NotificationSinkType.SLACK;
  }

  @Override
  public Collection<NotificationTemplateType> templates() {
    return SUPPORTED_TEMPLATES;
  }

  @Override
  public Collection<NotificationRecipientType> recipientTypes() {
    return RECIPIENT_TYPES;
  }
}
