package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class UsageEventsConfiguration {

  /**
   * {@code elasticsearch} (search index) or {@code postgres} (pgAnalytics). Must be {@code
   * postgres} when {@code postgres.pgAnalytics.enabled=true}; dual-write is not supported. {@code
   * postgres} also requires SqlSetup.
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
