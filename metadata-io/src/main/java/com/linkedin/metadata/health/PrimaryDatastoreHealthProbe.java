package com.linkedin.metadata.health;

import javax.annotation.Nonnull;

/**
 * Probes reachability of the configured primary entity store: JDBC (Ebean) for MySQL, PostgreSQL,
 * etc., or Cassandra when {@code entityService.impl=cassandra}.
 */
public interface PrimaryDatastoreHealthProbe {

  @Nonnull
  DatastoreHealthStatus probe();
}
