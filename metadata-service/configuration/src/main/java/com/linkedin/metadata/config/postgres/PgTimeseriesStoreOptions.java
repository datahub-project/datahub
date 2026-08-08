package com.linkedin.metadata.config.postgres;

import lombok.Builder;
import lombok.Value;
import org.springframework.lang.Nullable;

/**
 * Resolved options for one pgTimeseries store (one partman parent table + JDBC pool). See {@link
 * PostgresSqlSetupProperties#buildPgTimeseriesOptions()}.
 */
@Value
@Builder
public class PgTimeseriesStoreOptions {
  /** Store name (e.g. {@code default}, {@code long}). */
  String name;

  /** PostgreSQL schema for this store's tables. */
  String schema;

  /** Normalized table prefix; aspect table is {@code tablePrefix + "_aspect"}. */
  String tablePrefix;

  /** Lower-cased allowlisted pg_partman interval on {@code event_time}. */
  String partmanPartitionInterval;

  int partmanPremake;

  /**
   * When true, SqlSetup updates {@code part_config.partition_interval} / {@code premake} even if
   * the parent is already registered.
   */
  boolean forceOverwritePartmanConfig;

  /**
   * When {@code > 0}, sets partman {@code retention}. When {@code 0}, clears {@code
   * part_config.retention} (stops partition drops).
   */
  int retentionMaxAgeSeconds;

  boolean maintenanceCronEnabled;
  int maintenanceIntervalSeconds;

  /**
   * JDBC URL for this store; may be null/blank to fall through to the main Ebean URL at runtime.
   */
  @Nullable String poolUrl;

  @Nullable String poolDriver;
  @Nullable String poolUsername;
  @Nullable String poolPassword;

  int poolMinConnections;
  int poolMaxConnections;
  int poolMaxInactiveTimeSeconds;
  int poolMaxAgeMinutes;
  int poolLeakTimeMinutes;
  int poolWaitTimeoutMillis;

  /** Qualified aspect table: {@code schema.prefix_aspect}. */
  public String qualifiedAspectTable() {
    return schema + "." + tablePrefix + "_aspect";
  }
}
