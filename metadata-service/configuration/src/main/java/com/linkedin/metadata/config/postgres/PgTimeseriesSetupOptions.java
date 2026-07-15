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
   * "_aspect_row"}.
   */
  String tablePrefix;

  /** Lower-cased allowlisted pg_partman interval on {@code event_time} ({@code timestamptz}). */
  String partmanPartitionInterval;

  int partmanPremake;

  /** 0 = do not set partman {@code retention} from SqlSetup. */
  int retentionMaxAgeSeconds;

  boolean maintenanceCronEnabled;
  int maintenanceIntervalSeconds;
}
