package com.linkedin.gms.factory.health;

import com.linkedin.metadata.health.DatastoreHealthStatus;
import com.linkedin.metadata.health.PrimaryDatastoreHealthProbe;
import io.ebean.Database;
import java.sql.Connection;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** JDBC connectivity check for Ebean-backed deployments (MySQL, PostgreSQL, H2, etc.). */
@RequiredArgsConstructor
@Slf4j
public class EbeanPrimaryDatastoreHealthProbe implements PrimaryDatastoreHealthProbe {

  private final Database database;

  @Override
  @Nonnull
  public DatastoreHealthStatus probe() {
    try {
      database.sqlQuery("SELECT 1 AS health_check").findOne();
      try (Connection c = database.dataSource().getConnection()) {
        String url = c.getMetaData().getURL();
        String summary = url != null ? sanitizeJdbcUrl(url) : "unknown URL";
        return DatastoreHealthStatus.ok("JDBC reachable (" + summary + ")");
      }
    } catch (Exception e) {
      log.debug("Primary datastore JDBC health check failed", e);
      return DatastoreHealthStatus.unhealthy(
          e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
    }
  }

  static String sanitizeJdbcUrl(@Nonnull String url) {
    return url.replaceAll("(?i)(password=)[^;&]*", "$1***");
  }
}
