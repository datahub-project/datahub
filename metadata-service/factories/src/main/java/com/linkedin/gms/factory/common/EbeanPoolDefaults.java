package com.linkedin.gms.factory.common;

import io.ebean.datasource.DataSourceConfig;
import java.sql.Connection;

/**
 * Default JDBC transaction isolation for DataHub Ebean connection pools.
 *
 * <p>Each pool applies this explicitly at {@link DataSourceConfig} build time so defaults stay
 * aligned across the main metadata DB, pgGraph, pgQueue, pgTimeseries, and embedded consumers — and
 * match {@link com.linkedin.metadata.entity.ebean.EbeanAspectDao}'s typical {@code READ_COMMITTED}
 * usage, reducing isolation churn when connections are borrowed from the pool.
 */
public final class EbeanPoolDefaults {

  /** Matches {@link Connection#TRANSACTION_READ_COMMITTED}. */
  public static final int DEFAULT_TRANSACTION_ISOLATION = Connection.TRANSACTION_READ_COMMITTED;

  private EbeanPoolDefaults() {}

  public static void applyDefaultTransactionIsolation(DataSourceConfig dataSourceConfig) {
    dataSourceConfig.setIsolationLevel(DEFAULT_TRANSACTION_ISOLATION);
  }
}
