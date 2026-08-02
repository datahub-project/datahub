package com.linkedin.metadata.config.postgres;

import lombok.Value;

/**
 * Resolved when {@code postgres.pgTimeseries.enabled} is true; see {@link
 * PostgresSqlSetupProperties#buildPgTimeseriesOptions()}.
 */
@Value
public class PgTimeseriesSetupOptions {
  /**
   * Shared {@code postgres.schema} where pgTimeseries tables live; see {@link
   * PostgresSqlSetupProperties#normalizedPostgresSchema()}.
   */
  String schema;

  /**
   * Normalized {@code postgres.pgTimeseries.tablePrefix}; aspect table is {@code tablePrefix +
   * "_aspect"}.
   */
  String tablePrefix;

  /** Lower-cased allowlisted pg_partman interval on {@code event_time} ({@code timestamptz}). */
  String partmanPartitionInterval;

  int partmanPremake;

  /**
   * When true, SqlSetup updates {@code part_config.partition_interval} / {@code premake} even if
   * the parent is already registered. Default false leaves those sticky after first {@code
   * create_parent}.
   */
  boolean forceOverwritePartmanConfig;

  /**
   * When {@code > 0}, sets partman {@code retention}. When {@code 0}, clears {@code
   * part_config.retention} (stops partition drops).
   */
  int retentionMaxAgeSeconds;

  boolean maintenanceCronEnabled;
  int maintenanceIntervalSeconds;
}
