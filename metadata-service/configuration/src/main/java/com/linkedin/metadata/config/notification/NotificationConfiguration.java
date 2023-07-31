package com.linkedin.metadata.config.notification;

import java.util.List;
import lombok.Data;

/**
 * POJO representing the "notifications" block in application.yml
 */
@Data
public class NotificationConfiguration {
  /**
   * Whether sinking notifications is enabled
   */
  public boolean enabled;
  /**
   * List of configurations for {@link com.linkedin.event.notification.NotificationSink}s to be registered
   */
  private List<NotificationSinkConfiguration> sinks;
}