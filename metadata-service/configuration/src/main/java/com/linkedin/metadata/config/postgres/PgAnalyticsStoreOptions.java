package com.linkedin.metadata.config.postgres;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import org.springframework.lang.Nullable;

/**
 * Resolved options for one pgAnalytics store (event/rollup/distinct/watermark parents + JDBC pool).
 */
@Value
@Builder
@ToString(exclude = "poolPassword")
public class PgAnalyticsStoreOptions {
  String name;
  String schema;
  String tablePrefix;
  String partmanPartitionInterval;
  int partmanPremake;
  boolean forceOverwritePartmanConfig;

  int rawMaxAgeSeconds;
  int hourlyMaxAgeSeconds;
  int dailyMaxAgeSeconds;
  int monthlyMaxAgeSeconds;

  /** Seal lag after hour_end before an hour may be watermarked (seconds). */
  int inputLagSeconds;

  boolean maintenanceCronEnabled;
  int maintenanceIntervalSeconds;

  boolean apiUsageFlushEnabled;
  boolean entityCountEnabled;

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

  public String qualifiedEventTable() {
    return schema + "." + tablePrefix + "_event";
  }

  public String qualifiedRollupTable() {
    return schema + "." + tablePrefix + "_rollup";
  }

  public String qualifiedDistinctSetTable() {
    return schema + "." + tablePrefix + "_distinct_set";
  }

  public String qualifiedWatermarkTable() {
    return schema + "." + tablePrefix + "_watermark";
  }
}
