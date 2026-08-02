package com.linkedin.metadata;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
 * reruns when {@code testcontainers.reuse.enable=true}). Integration tests must use <b>isolated
 * PostgreSQL namespaces</b> per test class: call {@link #newIntegrationNamespace(String)} once in
 * {@code @BeforeClass} and pass the returned schema + table prefix into properties. Do not
 * hard-code a single schema — parallel classes or reuse would truncate or drop each other's tables.
 */
public final class PostgresTestUtils {

  private PostgresTestUtils() {}

  private static PostgreSQLContainer<?> sharedPostgres;

  /**
   * Pair of {@code postgres.schema} and table-prefix segment for one integration test class.
   * Together they isolate DDL/DML from other tests on the same PostgreSQL instance.
   */
  public static final class IntegrationNamespace {
    private final String schema;
    private final String tablePrefix;

    private IntegrationNamespace(String schema, String tablePrefix) {
      this.schema = schema;
      this.tablePrefix = tablePrefix;
    }

    @Nonnull
    public String getSchema() {
      return schema;
    }

    @Nonnull
    public String getTablePrefix() {
      return tablePrefix;
    }
  }

  @Nonnull
  public static IntegrationNamespace newIntegrationNamespace(@Nonnull String shortLabel) {
    String sanitized = shortLabel.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
    if (sanitized.isEmpty()) {
      sanitized = "it";
    }
    if (sanitized.length() > 20) {
      sanitized = sanitized.substring(0, 20);
    }
    String id = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    String schema = "it_" + sanitized + "_" + id;
    String prefix = "mio_" + id;
    return new IntegrationNamespace(schema, prefix);
  }

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

  @Nonnull
  public static Database createEbeanDatabase(
      @Nonnull PostgreSQLContainer<?> container, @Nonnull String serverName) {
    return createEbeanDatabase(container, serverName, false, false);
  }

  /**
   * Primary Ebean pool for integration tests: runs aspect DDL against the shared Testcontainers
   * PostgreSQL instance.
   */
  @Nonnull
  public static Database createEbeanPrimaryDatabase(
      @Nonnull PostgreSQLContainer<?> container, @Nonnull String serverName) {
    return createEbeanDatabase(container, serverName, true, false);
  }

  /**
   * Read-pool Ebean instance (split-pool): same JDBC URL as primary, connections marked read-only.
   */
  @Nonnull
  public static Database createEbeanReadPoolDatabase(
      @Nonnull PostgreSQLContainer<?> container, @Nonnull String serverName) {
    return createEbeanDatabase(container, serverName, false, true);
  }

  @Nonnull
  private static Database createEbeanDatabase(
      @Nonnull PostgreSQLContainer<?> container,
      @Nonnull String serverName,
      boolean runAspectDdl,
      boolean readOnlyPool) {
    DataSourceConfig dsc = new DataSourceConfig();
    dsc.setUrl(container.getJdbcUrl());
    dsc.setUsername(container.getUsername());
    dsc.setPassword(container.getPassword());
    dsc.setDriver("org.postgresql.Driver");
    if (readOnlyPool) {
      dsc.setReadOnly(true);
      dsc.setAutoCommit(true);
    }

    DatabaseConfig cfg = new DatabaseConfig();
    cfg.setName(serverName);
    cfg.setDataSourceConfig(dsc);
    cfg.setDefaultServer(false);
    cfg.setDdlGenerate(runAspectDdl);
    cfg.setDdlRun(runAspectDdl);
    cfg.addPackage("com.linkedin.metadata.entity.ebean");
    cfg.addPackage("com.linkedin.metadata.queue.ebean");
    return DatabaseFactory.create(cfg);
  }

  @Nonnull
  public static String uniqueServerName(@Nonnull String base) {
    return base + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  /**
   * Properties for pgTimeseries JDBC tests: schema + table prefix only (other SqlSetup features
   * off).
   */
  @Nonnull
  public static PostgresSqlSetupProperties testPgTimeseriesProperties(
      @Nonnull String schema, @Nonnull String tablePrefix) {
    PostgresSqlSetupProperties p = new PostgresSqlSetupProperties();
    p.setSchema(schema);
    p.getPgQueue().setEnabled(false);
    p.getPgTimeseries().setEnabled(false);
    p.getPgTimeseries().setTablePrefix(tablePrefix);
    return p;
  }

  /**
   * Creates a non-partitioned {@code {prefix}_aspect} table (same columns as SqlSetup) for
   * Testcontainers tests without pg_partman.
   */
  public static void applyPgTimeseriesAspectTable(
      @Nonnull Connection connection, @Nonnull PostgresSqlSetupProperties props)
      throws SQLException {
    String schema = props.normalizedPostgresSchema();
    String prefix = props.normalizedPgTimeseriesTablePrefix();
    String qualified = schema + "." + prefix + "_aspect";
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
      st.execute("DROP TABLE IF EXISTS " + qualified + " CASCADE");
      st.execute(
          "CREATE TABLE "
              + qualified
              + " ("
              + "entity_name text NOT NULL, "
              + "aspect_name text NOT NULL, "
              + "urn text NOT NULL, "
              + "message_id text NOT NULL, "
              + "event_time timestamptz NOT NULL, "
              + "run_id text, "
              + "event_granularity text, "
              + "partition_spec jsonb, "
              + "event jsonb, "
              + "system_metadata jsonb, "
              + "document jsonb, "
              + "PRIMARY KEY (entity_name, aspect_name, message_id, event_time))");
      st.execute(
          "CREATE INDEX IF NOT EXISTS idx_"
              + prefix
              + "_aspect_lookup ON "
              + qualified
              + " (entity_name, aspect_name, urn, event_time DESC)");
      st.execute(
          "CREATE INDEX IF NOT EXISTS idx_"
              + prefix
              + "_aspect_truncate ON "
              + qualified
              + " (entity_name, aspect_name, event_time)");
      connection.commit();
    }
  }

  public static void truncatePgTimeseriesAspect(
      @Nonnull Connection connection, @Nonnull PostgresSqlSetupProperties props)
      throws SQLException {
    String schema = props.normalizedPostgresSchema();
    String prefix = props.normalizedPgTimeseriesTablePrefix();
    try (Statement st = connection.createStatement()) {
      st.execute("TRUNCATE TABLE " + schema + "." + prefix + "_aspect");
      connection.commit();
    }
  }
}
