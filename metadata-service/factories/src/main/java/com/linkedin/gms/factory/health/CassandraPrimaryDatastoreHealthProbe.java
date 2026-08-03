package com.linkedin.gms.factory.health;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.linkedin.metadata.health.DatastoreHealthStatus;
import com.linkedin.metadata.health.PrimaryDatastoreHealthProbe;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Session reachability check for Cassandra-backed entity storage. */
@RequiredArgsConstructor
@Slf4j
public class CassandraPrimaryDatastoreHealthProbe implements PrimaryDatastoreHealthProbe {

  private final CqlSession session;

  @Override
  @Nonnull
  public DatastoreHealthStatus probe() {
    try {
      Row row =
          session
              .execute(SimpleStatement.newInstance("SELECT release_version FROM system.local"))
              .one();
      String version = row != null ? row.getString("release_version") : "unknown";
      return DatastoreHealthStatus.ok("Cassandra reachable (version " + version + ")");
    } catch (Exception e) {
      log.debug("Cassandra health check failed", e);
      return DatastoreHealthStatus.unhealthy(
          e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
    }
  }
}
