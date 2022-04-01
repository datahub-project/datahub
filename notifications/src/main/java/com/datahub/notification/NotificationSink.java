package com.datahub.notification;

import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.NotificationSinkType;
import java.util.Collection;
import javax.annotation.Nonnull;


/**
 * A notification sink represents a destination to which notifications can be sent.
 *
 * Notification sinks are dumb components with a single job: send a notification to a single recipient (DataHub Actor or custom recipient)
 *
 * By the time a sink receives it, a message is presumed safe to send. A notification sink does not perform
 * any additional validation against a user's preferences.
 */
public interface NotificationSink {
  /**
   * Returns the {@link NotificationSinkType} corresponding to the sink.
   */
  NotificationSinkType type();

  /**
   * Returns the set of notification template types supported by the sink.
   */
  Collection<NotificationTemplateType> templates();

  /**
   * Initializes a notification sink.
   */
  void init(@Nonnull final NotificationSinkConfig cfg);

  /**
   * Sends a notification to one or more recipients based on a {@link NotificationRequest}.
   */
  void send(@Nonnull final NotificationRequest request, @Nonnull final NotificationContext context) throws Exception;
}
