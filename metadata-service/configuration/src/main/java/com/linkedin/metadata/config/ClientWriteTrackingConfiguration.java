package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Settings for counting accepted GMS metadata writes by the client that sent them. Observation
 * only: nothing is ever rejected, so it is safe to leave on everywhere.
 */
@Data
public class ClientWriteTrackingConfiguration {
  /** Count accepted writes by client type, client version and actor. */
  private boolean enabled = true;

  /**
   * How often a background thread writes per-actor totals to the GMS log. Zero or less turns the
   * summary off; the metric is still recorded.
   */
  private int summaryIntervalSeconds = 3600;

  /**
   * Upper bound on distinct actor and client combinations held between two summaries. Writes for
   * combinations beyond it are still counted in the metric, just not attributed in the summary.
   * Zero or less turns the summary off.
   */
  private int maxTrackedKeys = 5000;

  /**
   * Upper bound on distinct {@code client_version} metric tag values. Each pod keeps the first
   * versions it sees; later ones are tagged {@code other}.
   */
  private int maxVersionTags = 20;
}
