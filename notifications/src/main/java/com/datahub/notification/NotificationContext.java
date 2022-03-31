package com.datahub.notification;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Context provided to a {@link NotificationSink} when a request to send a notification is made.
 */
@Data
@AllArgsConstructor
public class NotificationContext {
  // Currently unused.
}
