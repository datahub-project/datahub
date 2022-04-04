package com.linkedin.metadata.config.notification;

import java.util.Map;
import lombok.Data;

@Data
public class NotificationSinkConfiguration {
  /**
   * A fully-qualified class name for the {@link com.linkedin.event.notification.NotificationSink} implementation to be registered.
   */
  private String type;

  /**
   * Whether the sink should be created (is enabled)
   */
  private boolean enabled;

  /**
   * A set of notification-sink-specific configurations passed through during "init" of the sink.
   */
  private Map<String, Object> configs;
}
