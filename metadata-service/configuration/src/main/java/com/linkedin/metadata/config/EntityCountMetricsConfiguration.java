package com.linkedin.metadata.config;

import lombok.Data;

/** Micrometer export of key-aspect entity counts from system metadata. */
@Data
public class EntityCountMetricsConfiguration {
  /** Enable periodic entity count gauge refresh on each GMS replica. */
  private boolean enabled = true;

  /**
   * Interval between refresh cycles (seconds). Zero runs a single refresh after the initial delay.
   */
  private long updateIntervalSeconds = 3600;

  /** Delay before the first refresh after startup (seconds). */
  private long initialDelaySeconds = 60;

  /** When true, bypass the key-aspect entity count cache on each refresh. */
  private boolean skipCache = false;
}
