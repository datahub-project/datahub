package com.linkedin.metadata.entity.coordinator.concurrency;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import io.ebean.Database;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Coordinated-ingest concurrency IT against a real PostgreSQL (READ COMMITTED + {@code FOR
 * UPDATE}). All scenarios live in {@link AbstractCoordinatedIngestConcurrencyIT}.
 */
public class CoordinatedIngestConcurrencyPostgresIT extends AbstractCoordinatedIngestConcurrencyIT {

  @Override
  protected Database startEngineDatabase() {
    PostgreSQLContainer<?> container = PostgresTestUtils.startPostgres();
    return PostgresTestUtils.createEbeanPrimaryDatabase(
        container, PostgresTestUtils.uniqueServerName("coord_concurrency_pg"));
  }

  @Override
  protected void stopEngineDatabase(Database database) {
    // The PostgreSQL container is JVM-wide shared (and reused); only the per-class pool is closed.
    EbeanTestUtils.shutdownDatabase(database);
  }

  @Override
  protected String engineLabel() {
    return "postgres";
  }
}
