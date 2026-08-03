package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class UsageEventsConfiguration {

  /**
   * {@code elasticsearch} (legacy OpenSearch-backed index) or {@code postgres} (partitioned
   * Postgres table plus SQL analytics). When {@code postgres}, reads/writes use the dedicated
   * {@code postgres.pgTimeseries.pool} ({@code pgTimeseriesEbeanServer}), shared with SqlSetup
   * pgTimeseries runtime traffic — not {@code ebeanServer}.
   */
  private String implementation;

  /** How many calendar months of usage events to retain (partitions fully older are dropped). */
  private int retentionMonths;

  /** Pre-create empty monthly partitions this many months ahead of the current month. */
  private int partitionsAheadMonths;

  /**
   * Bounds usage-based recommendations (popular / recently edited) to events no older than this
   * many days (PostgreSQL backend only; avoids scanning full history).
   */
  private int recommendationLookbackDays;

  private PartitionMaintenanceConfiguration partitionMaintenance;

  public boolean usePostgresql() {
    return "postgres".equalsIgnoreCase(implementation);
  }

  @Data
  public static class PartitionMaintenanceConfiguration {

    /** When true, runs a scheduled task (GMS) to pre-create partitions and enforce retention. */
    private boolean enabled;

    /** Spring cron expression; default daily at 03:00. */
    private String cron;
  }
}
