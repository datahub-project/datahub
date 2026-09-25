package com.linkedin.metadata;

import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import java.sql.Connection;
import java.sql.DriverManager;
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
    return createEbeanDatabase(container, serverName, false, false, null);
  }

  /**
   * Primary Ebean pool for integration tests: runs aspect DDL against the shared Testcontainers
   * PostgreSQL instance.
   *
   * <p><b>Not schema-isolated.</b> Only safe for a test class that is the sole user of Ebean entity
   * DDL ({@code com.linkedin.metadata.entity.ebean}) on the shared container, since {@code ddlRun}
   * drops and recreates those tables in the default schema. Prefer the {@link
   * #createEbeanPrimaryDatabase(PostgreSQLContainer, String, IntegrationNamespace)} overload for
   * any class that runs alongside other Ebean-DDL Postgres IT classes under {@code
   * parallel="classes"}.
   */
  @Nonnull
  public static Database createEbeanPrimaryDatabase(
      @Nonnull PostgreSQLContainer<?> container, @Nonnull String serverName) {
    return createEbeanDatabase(container, serverName, true, false, null);
  }

  /**
   * Read-pool Ebean instance (split-pool): same JDBC URL as primary, connections marked read-only.
   */
  @Nonnull
  public static Database createEbeanReadPoolDatabase(
      @Nonnull PostgreSQLContainer<?> container, @Nonnull String serverName) {
    return createEbeanDatabase(container, serverName, false, true, null);
  }

  /**
   * Schema-isolated primary Ebean pool: runs aspect DDL inside {@code namespace}'s own PostgreSQL
   * schema instead of the shared container's default schema. Use this (paired with the read-pool
   * overload below on the same namespace) for any test class that runs concurrently with other
   * Ebean-DDL Postgres IT classes, so one class's DDL can't drop/recreate tables mid-query in
   * another.
   */
  @Nonnull
  public static Database createEbeanPrimaryDatabase(
      @Nonnull PostgreSQLContainer<?> container,
      @Nonnull String serverName,
      @Nonnull IntegrationNamespace namespace) {
    return createEbeanDatabase(container, serverName, true, false, namespace.getSchema());
  }

  /** Schema-isolated read-pool counterpart of {@link #createEbeanPrimaryDatabase}. */
  @Nonnull
  public static Database createEbeanReadPoolDatabase(
      @Nonnull PostgreSQLContainer<?> container,
      @Nonnull String serverName,
      @Nonnull IntegrationNamespace namespace) {
    return createEbeanDatabase(container, serverName, false, true, namespace.getSchema());
  }

  @Nonnull
  private static Database createEbeanDatabase(
      @Nonnull PostgreSQLContainer<?> container,
      @Nonnull String serverName,
      boolean runAspectDdl,
      boolean readOnlyPool,
      @Nullable String schema) {
    String jdbcUrl = container.getJdbcUrl();
    if (schema != null) {
      ensureSchemaExists(container, schema);
      jdbcUrl = jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?") + "currentSchema=" + schema;
    }

    DataSourceConfig dsc = new DataSourceConfig();
    dsc.setUrl(jdbcUrl);
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

  private static void ensureSchemaExists(
      @Nonnull PostgreSQLContainer<?> container, @Nonnull String schema) {
    try (Connection c =
            DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
        Statement st = c.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
    } catch (java.sql.SQLException e) {
      throw new RuntimeException("Failed to create isolated schema " + schema, e);
    }
  }

  @Nonnull
  public static String uniqueServerName(@Nonnull String base) {
    return base + "_" + UUID.randomUUID().toString().replace("-", "");
  }
}
