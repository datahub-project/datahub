package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class UsageEventsConfiguration {

  /**
   * {@code elasticsearch} (search index) or {@code postgres} (pgAnalytics). When {@code postgres},
   * requires {@code postgres.pgAnalytics.enabled=true} and SqlSetup.
   */
  private String implementation;

  /**
   * Bounds usage-based recommendations to events no older than this many days (PostgreSQL backend).
   */
  private int recommendationLookbackDays;

  public boolean usePostgresql() {
    return "postgres".equalsIgnoreCase(implementation);
  }
}
