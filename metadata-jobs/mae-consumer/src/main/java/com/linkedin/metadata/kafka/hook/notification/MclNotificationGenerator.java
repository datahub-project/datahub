package com.linkedin.metadata.kafka.hook.notification;

import com.linkedin.mxe.MetadataChangeLog;
import javax.annotation.Nonnull;


/**
 * A component which generates notifications based on a {@link MetadataChangeLog} event.
 */
public interface MclNotificationGenerator {
  /**
   * Generate notifications based on a {@link com.linkedin.mxe.MetadataChangeLog} event.
   */
  void generate(@Nonnull MetadataChangeLog event);
}
