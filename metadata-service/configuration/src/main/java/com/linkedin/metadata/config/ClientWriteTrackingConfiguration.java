package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Settings for counting GMS write requests by the client that sent them. Observation only: nothing
 * is ever rejected, so it is safe to leave on everywhere.
 */
@Data
public class ClientWriteTrackingConfiguration {
  /** Count write requests by client type, client version and actor. */
  private boolean enabled = true;

  /** How often per-actor totals are written to the GMS log. */
  private int summaryIntervalSeconds = 3600;

  /**
   * Upper bound on distinct actor and client combinations held between two summaries. Writes for
   * combinations beyond it are still counted in the metric, just not attributed in the summary.
   */
  private int maxTrackedKeys = 5000;
}
