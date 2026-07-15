package com.linkedin.metadata.datahubusage.postgres;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Partitioned usage event table (PostgreSQL); distinct from the OpenSearch index name. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PostgresUsageEventsTable {

  public static final String TABLE_NAME = "datahub_usage_events";

  /** Suffix for partition table names, e.g. {@code datahub_usage_events_p202501}. */
  public static final String PARTITION_SUFFIX_PREFIX = "_p";

  public static String partitionTableName(String baseTable, int year, int month) {
    return baseTable + PARTITION_SUFFIX_PREFIX + yearMonthToken(year, month);
  }

  public static String yearMonthToken(int year, int month) {
    return String.format("%04d%02d", year, month);
  }
}
