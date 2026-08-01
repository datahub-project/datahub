package com.linkedin.metadata;

import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import java.time.Duration;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainers PostgreSQL helpers for metadata-io integration tests.
 *
 * <p><b>Concurrency and reuse:</b> {@link #startPostgres()} returns one JVM-wide shared {@link
 * PostgreSQLContainer} (started lazily, {@link
 * org.testcontainers.containers.PostgreSQLContainer#withReuse(boolean) reuse} enabled for faster
 * reruns when {@code testcontainers.reuse.enable=true}). Trimmed backport of master's helper: only
 * the pieces needed by {@link com.linkedin.metadata.entity.ebean.EbeanAspectDaoLockingPostgresIT}
 * exist on this branch (no split read pool, no per-test schema namespaces).
 */
public final class PostgresTestUtils {

  private PostgresTestUtils() {}

  private static PostgreSQLContainer<?> sharedPostgres;

  @Nonnull
  public static DockerImageName datahubPostgresDockerImageName() {
    String repo =
        firstNonBlank(
            System.getenv("DATAHUB_POSTGRES_IMAGE"),
            System.getProperty("datahub.testcontainers.postgres.image"));
    String tag =
        firstNonBlank(
            System.getenv("DATAHUB_POSTGRES_VERSION"),
            System.getProperty("datahub.testcontainers.postgres.version"));
    if (repo == null) {
      repo = "acryldata/datahub-postgres";
    }
    if (tag == null) {
      tag = "17.5-extensions-v1";
    }
    return DockerImageName.parse(repo + ":" + tag).asCompatibleSubstituteFor("postgres");
  }

  @Nullable
  private static String firstNonBlank(@Nullable String a, @Nullable String b) {
    if (a != null && !a.isBlank()) {
      return a.trim();
    }
    if (b != null && !b.isBlank()) {
      return b.trim();
    }
    return null;
  }

  @Nonnull
  public static synchronized PostgreSQLContainer<?> startPostgres() {
    if (sharedPostgres != null && sharedPostgres.isRunning()) {
      return sharedPostgres;
    }
    PostgreSQLContainer<?> container =
        new PostgreSQLContainer<>(datahubPostgresDockerImageName())
            .withStartupTimeout(Duration.ofMinutes(2))
            .withReuse(true);
    container.start();
    sharedPostgres = container;
    return container;
  }

  /**
   * Primary Ebean pool for integration tests: runs aspect DDL against the shared Testcontainers
   * PostgreSQL instance.
   */
  @Nonnull
  public static Database createEbeanPrimaryDatabase(
      @Nonnull PostgreSQLContainer<?> container, @Nonnull String serverName) {
    DataSourceConfig dsc = new DataSourceConfig();
    dsc.setUrl(container.getJdbcUrl());
    dsc.setUsername(container.getUsername());
    dsc.setPassword(container.getPassword());
    dsc.setDriver("org.postgresql.Driver");

    DatabaseConfig cfg = new DatabaseConfig();
    cfg.setName(serverName);
    cfg.setDataSourceConfig(dsc);
    cfg.setDefaultServer(false);
    cfg.setDdlGenerate(true);
    cfg.setDdlRun(true);
    cfg.addPackage("com.linkedin.metadata.entity.ebean");
    return DatabaseFactory.create(cfg);
  }

  @Nonnull
  public static String uniqueServerName(@Nonnull String base) {
    return base + "_" + UUID.randomUUID().toString().replace("-", "");
  }
}
