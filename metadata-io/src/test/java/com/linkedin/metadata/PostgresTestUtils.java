package com.linkedin.metadata;

import com.linkedin.metadata.config.postgres.PgSystemMetadataSetupOptions;
import com.linkedin.metadata.config.postgres.PgUsageEventsSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner;
import com.linkedin.metadata.sqlsetup.postgres.pg_system_metadata.PgSystemMetadataSqlMigrationModules;
import com.linkedin.metadata.sqlsetup.postgres.pg_system_metadata.PgSystemMetadataSqlMigrationTokens;
import com.linkedin.metadata.sqlsetup.postgres.usage_events.PgUsageEventsSqlMigrationModules;
import com.linkedin.metadata.sqlsetup.postgres.usage_events.PgUsageEventsSqlMigrationTokens;
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
 * {@code @BeforeClass} and pass the returned schema + table prefix into {@link
 * PostgresSqlSetupProperties}. Do not hard-code a single schema like {@code dh_pg_it} — parallel
 * classes or reuse would truncate or drop each other's tables.
 *
 * <p>Do not call {@link PostgreSQLContainer#stop()} on the container returned from {@link
 * #startPostgres()} from test {@code @AfterClass} — other classes in the same suite still need it.
 * Shut down only Ebean {@link Database} instances via {@link com.linkedin.metadata.EbeanTestUtils}.
 *
 * <p>The {@code testng-postgresql.xml} suite runs {@code parallel="classes"} with a bounded thread
 * count; {@code Gradle testPostgresql} uses a single fork so the shared container is one per worker
 * JVM.
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

  /**
   * Allocates a unique schema name and table-prefix fragment for JDBC integration tests (valid
   * PostgreSQL identifiers, normalized lowercase).
   *
   * <p>Use one namespace per test class so parallel execution or Testcontainers reuse cannot mix
   * data between tests.
   */
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

  /**
   * Docker image for PostgreSQL Testcontainers, aligned with {@code
   * docker/profiles/docker-compose.prerequisites.yml}: {@code
   * ${DATAHUB_POSTGRES_IMAGE}:${DATAHUB_POSTGRES_VERSION}}.
   *
   * <p>Use the repo-built {@code :docker:postgres} image (PostGIS, pgRouting, pgvector, pg_partman,
   * pg_cron) so SqlSetup-style tests match production. After {@code ./gradlew :docker:postgres},
   * Gradle sets {@code DATAHUB_POSTGRES_IMAGE} / {@code DATAHUB_POSTGRES_VERSION} when unset;
   * override with env or {@code -Ddatahub.testcontainers.postgres.image} / {@code .version}.
   *
   * <p>Declared as a substitute for the official {@code postgres} image so Testcontainers wait
   * strategies accept custom registry paths.
   */
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
      tag = "debug";
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

  /**
   * Returns a single shared running PostgreSQL container for this JVM (metadata-io postgres IT
   * suite). Synchronized so parallel {@code @BeforeClass} methods do not start two containers.
   */
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
   * Same as {@link #startPostgres()}: the DataHub {@code datahub-postgres} image already includes
   * pgvector for {@code embedding_tier*} columns.
   */
  @Nonnull
  public static PostgreSQLContainer<?> startPostgresWithPgvector() {
    return startPostgres();
  }

  /**
   * Builds {@link PostgresSqlSetupProperties} aligned with {@link
   * #applyPgSearchEntityTables(Connection, PostgresSqlSetupProperties)} (vector off, tier columns
   * from fulltext config).
   */
  @Nonnull
  public static PostgresSqlSetupProperties testPgSearchProperties(
      @Nonnull String schema, @Nonnull String tablePrefix) {
    PostgresSqlSetupProperties p = new PostgresSqlSetupProperties();
    p.setSchema(schema);
    p.getPgGraph().setEnabled(false);
    p.getPgTimeseries().setEnabled(false);
    p.getPgQueue().setEnabled(false);
    p.getPgSearch().getEntity().setEnabled(true);
    p.getPgSearch().getEntity().setTablePrefix(tablePrefix);
    p.getPgSearch().getEntity().getVector().setEnabled(false);
    p.getPgSearch().getEntity().getFulltext().setDefaultLanguage("english");
    if (p.getPgSearch().getEntity().getFulltext().getTierTsvectorColumnCount() < 1) {
      p.getPgSearch().getEntity().getFulltext().setTierTsvectorColumnCount(4);
    }
    return p;
  }

  /**
   * Creates {@code {schema}.{prefix}_group_registry} and {@code {schema}.{prefix}_search_row}
   * matching SqlSetup layout (no pgvector columns when vector is disabled).
   */
  public static void applyPgSearchEntityTables(
      @Nonnull Connection connection, @Nonnull PostgresSqlSetupProperties props)
      throws SQLException {
    String schema = props.normalizedPostgresSchema();
    String prefix = props.normalizedPgSearchEntityTablePrefix();
    int tierCols = props.getPgSearch().getEntity().getFulltext().getTierTsvectorColumnCount();
    String tierText = buildTierTextColumnDefinitions(tierCols);
    String tierVector = buildTierTsvectorColumnDefinitions(tierCols);
    boolean vectorEnabled = props.getPgSearch().getEntity().getVector().isEnabled();
    int embeddingDims = props.getPgSearch().getEntity().getVector().getEmbeddingDimensions();
    String tierEmbeddingCreate =
        vectorEnabled
            ? PostgresSqlSetupProperties.buildTierEmbeddingVectorColumnDefinitionsForCreateTable(
                tierCols, embeddingDims)
            : "";
    String qualifiedRegistry = schema + "." + prefix + "_group_registry";
    String qualifiedRow = schema + "." + prefix + "_search_row";
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
      st.execute("DROP TABLE IF EXISTS " + qualifiedRow + " CASCADE");
      st.execute("DROP TABLE IF EXISTS " + qualifiedRegistry + " CASCADE");
      if (vectorEnabled) {
        st.execute("CREATE EXTENSION IF NOT EXISTS vector");
      }
      st.execute(
          "CREATE TABLE "
              + qualifiedRegistry
              + " (search_group TEXT PRIMARY KEY, physical_table_name TEXT NOT NULL)");
      st.execute(
          "CREATE TABLE "
              + qualifiedRow
              + " (\n"
              + "    urn TEXT NOT NULL,\n"
              + "    search_group TEXT NOT NULL REFERENCES "
              + qualifiedRegistry
              + " (search_group),\n"
              + "    entity_type TEXT NOT NULL,\n"
              + "    document JSONB,\n"
              + "    search_extras JSONB,\n"
              + "    systemmetadata JSONB,\n"
              + tierText
              + tierVector
              + tierEmbeddingCreate
              + "    PRIMARY KEY (urn)\n"
              + ")");
      // Must match EntitySpec#getSearchGroup() for entities used in ITs (e.g. dataset =
      // searchGroup: primary in entity-registry.yml). Legacy test label "dataset" kept for FK
      // compatibility with older tests that still reference it.
      st.execute(
          "INSERT INTO "
              + qualifiedRegistry
              + " (search_group, physical_table_name) VALUES "
              + "('primary', '"
              + prefix
              + "_search_row'), "
              + "('dataset', '"
              + prefix
              + "_search_row') ON CONFLICT (search_group) DO NOTHING");
      connection.commit();
    }
  }

  public static void truncateSearchRow(
      @Nonnull Connection connection, @Nonnull PostgresSqlSetupProperties props)
      throws SQLException {
    String schema = props.normalizedPostgresSchema();
    String prefix = props.normalizedPgSearchEntityTablePrefix();
    try (Statement st = connection.createStatement()) {
      st.execute("TRUNCATE TABLE " + schema + "." + prefix + "_search_row");
      connection.commit();
    }
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
  private static String buildTierTextColumnDefinitions(int tierColumnCount) {
    if (tierColumnCount < 1) {
      throw new IllegalArgumentException("tierColumnCount must be >= 1");
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= tierColumnCount; i++) {
      sb.append("    ")
          .append(PostgresSqlSetupProperties.searchTextTierColumnName(i))
          .append(" TEXT,\n");
    }
    return sb.toString();
  }

  @Nonnull
  private static String buildTierTsvectorColumnDefinitions(int tierColumnCount) {
    if (tierColumnCount < 1) {
      throw new IllegalArgumentException("tierColumnCount must be >= 1");
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= tierColumnCount; i++) {
      sb.append("    ")
          .append(PostgresSqlSetupProperties.searchVectorTierColumnName(i))
          .append(" tsvector,\n");
    }
    return sb.toString();
  }

  /** Unique Ebean server name per JVM to avoid clashes when multiple suites reuse containers. */
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
    p.getPgGraph().setEnabled(false);
    p.getPgQueue().setEnabled(false);
    p.getPgSearch().getEntity().setEnabled(false);
    p.getPgTimeseries().setEnabled(false);
    p.getPgTimeseries().setTablePrefix(tablePrefix);
    return p;
  }

  /**
   * SqlSetup-aligned properties for PostgreSQL-backed system metadata only (other postgres.* DDL
   * features off).
   */
  @Nonnull
  public static PostgresSqlSetupProperties testPgSystemMetadataProperties(@Nonnull String schema) {
    PostgresSqlSetupProperties p = new PostgresSqlSetupProperties();
    p.setSchema(schema);
    p.getPgGraph().setEnabled(false);
    p.getPgQueue().setEnabled(false);
    p.getPgSearch().getEntity().setEnabled(false);
    p.getPgTimeseries().setEnabled(false);
    p.getPgSystemMetadata().setEnabled(true);
    return p;
  }

  /**
   * Applies system metadata table via versioned SqlSetup migrations in {@link
   * PostgresSqlSetupProperties#schema}.
   */
  public static void applyPgSystemMetadataTables(
      @Nonnull Connection connection, @Nonnull PostgresSqlSetupProperties props) throws Exception {
    PgSystemMetadataSetupOptions options = props.buildPgSystemMetadataOptions();
    if (options == null) {
      props.getPgSystemMetadata().setEnabled(true);
      options = props.buildPgSystemMetadataOptions();
    }
    PgSystemMetadataSqlMigrationTokens tokens =
        PgSystemMetadataSqlMigrationTokens.builder().tableName(options.getTableName()).build();
    PostgresSqlMigrationRunner.migrate(
        connection, PgSystemMetadataSqlMigrationModules.from(options, tokens));
    connection.commit();
  }

  public static void applyPgUsageEventsTables(
      @Nonnull Connection connection, @Nonnull PostgresSqlSetupProperties props) throws Exception {
    PgUsageEventsSetupOptions options = props.buildPgUsageEventsOptions();
    if (options == null) {
      props.getPgUsageEvents().setEnabled(true);
      options = props.buildPgUsageEventsOptions();
    }
    PgUsageEventsSqlMigrationTokens tokens =
        PgUsageEventsSqlMigrationTokens.builder()
            .parentTableName(options.getParentTableName())
            .build();
    PostgresSqlMigrationRunner.migrate(
        connection, PgUsageEventsSqlMigrationModules.from(options, tokens));
    connection.commit();
  }

  public static void truncatePgSystemMetadata(
      @Nonnull Connection connection, @Nonnull PostgresSqlSetupProperties props)
      throws SQLException {
    String schema = props.normalizedPostgresSchema();
    String table = props.normalizedPgSystemMetadataTableName();
    try (Statement st = connection.createStatement()) {
      st.execute("TRUNCATE TABLE " + schema + "." + table);
      connection.commit();
    }
  }

  /**
   * Creates a non-partitioned {@code {prefix}_aspect_row} table (same columns as SqlSetup) for
   * Testcontainers tests without pg_partman.
   */
  public static void applyPgTimeseriesAspectRowTable(
      @Nonnull Connection connection, @Nonnull PostgresSqlSetupProperties props)
      throws SQLException {
    String schema = props.normalizedPostgresSchema();
    String prefix = props.normalizedPgTimeseriesTablePrefix();
    String qualified = schema + "." + prefix + "_aspect_row";
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
              + "_aspect_row_lookup ON "
              + qualified
              + " (entity_name, aspect_name, urn, event_time DESC)");
      connection.commit();
    }
  }

  public static void truncatePgTimeseriesAspectRow(
      @Nonnull Connection connection, @Nonnull PostgresSqlSetupProperties props)
      throws SQLException {
    String schema = props.normalizedPostgresSchema();
    String prefix = props.normalizedPgTimeseriesTablePrefix();
    try (Statement st = connection.createStatement()) {
      st.execute("TRUNCATE TABLE " + schema + "." + prefix + "_aspect_row");
      connection.commit();
    }
  }

  /**
   * {@link PostgresSqlSetupProperties} for pgGraph integration tests: schema, table prefix, other
   * SqlSetup features off.
   */
  @Nonnull
  public static PostgresSqlSetupProperties testPgGraphProperties(
      @Nonnull String schema, @Nonnull String tablePrefix) {
    PostgresSqlSetupProperties p = new PostgresSqlSetupProperties();
    p.setSchema(schema);
    p.getPgGraph().setEnabled(true);
    p.getPgGraph().setTablePrefix(tablePrefix);
    p.getPgGraph().setIdHashAlgo("XXHASH64");
    p.getPgGraph().setMaxEdgeWriteBatchSize(1000);
    p.getPgSearch().getEntity().setEnabled(false);
    p.getPgTimeseries().setEnabled(false);
    p.getPgQueue().setEnabled(false);
    return p;
  }

  /**
   * Creates SqlSetup-style pgGraph tables, the {@code pgrouting_network} view, and {@code
   * public.dh_create_edge_type_if_not_exists} (required by {@code
   * com.linkedin.metadata.graph.postgres.PostgresGraphWriteSink} when a relationship name is not
   * yet in {@code edge_types}).
   *
   * <p>Uses a non-partitioned {@code edges} table (sufficient for Testcontainers). Requires {@code
   * postgis} and {@code pgrouting} (DataHub {@code datahub-postgres} image).
   */
  public static void applyPgGraphSchema(
      @Nonnull Connection connection, @Nonnull PostgresSqlSetupProperties props)
      throws SQLException {
    String schema = props.normalizedPostgresSchema();
    String prefix = props.normalizedPgGraphTablePrefix();
    String qEdgeTypes = schema + "." + prefix + "_edge_types";
    String qVertices = schema + "." + prefix + "_vertices";
    String qEdges = schema + "." + prefix + "_edges";
    String qNet = schema + "." + prefix + "_pgrouting_network";

    boolean ac = connection.getAutoCommit();
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE EXTENSION IF NOT EXISTS postgis");
      st.execute("CREATE EXTENSION IF NOT EXISTS pgrouting");
    }
    connection.setAutoCommit(false);
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
      st.execute("DROP VIEW IF EXISTS " + qNet + " CASCADE");
      st.execute("DROP TABLE IF EXISTS " + qEdges + " CASCADE");
      st.execute("DROP TABLE IF EXISTS " + qVertices + " CASCADE");
      st.execute("DROP TABLE IF EXISTS " + qEdgeTypes + " CASCADE");

      st.execute(
          "CREATE TABLE "
              + qEdgeTypes
              + " (id SMALLINT PRIMARY KEY, type_name VARCHAR(200) UNIQUE NOT NULL)");
      st.execute(
          "CREATE TABLE "
              + qVertices
              + " (xxhash64_id BIGINT PRIMARY KEY, urn VARCHAR(1024) UNIQUE NOT NULL,"
              + " removed BOOLEAN DEFAULT FALSE, properties JSONB DEFAULT '{}'::jsonb)");
      st.execute(
          "CREATE INDEX IF NOT EXISTS idx_"
              + prefix
              + "_vertices_urn_hash ON "
              + qVertices
              + " USING HASH (urn)");
      st.execute(
          "CREATE INDEX IF NOT EXISTS idx_"
              + prefix
              + "_vertices_urn_btree ON "
              + qVertices
              + "(urn varchar_pattern_ops) WHERE removed = FALSE");

      st.execute(
          "CREATE TABLE "
              + qEdges
              + " (source_id BIGINT NOT NULL REFERENCES "
              + qVertices
              + "(xxhash64_id) ON DELETE CASCADE,"
              + " target_id BIGINT NOT NULL REFERENCES "
              + qVertices
              + "(xxhash64_id) ON DELETE CASCADE,"
              + " edge_type SMALLINT NOT NULL REFERENCES "
              + qEdgeTypes
              + "(id) ON DELETE CASCADE,"
              + " owner_id BIGINT NOT NULL DEFAULT 0,"
              + " removed BOOLEAN DEFAULT FALSE,"
              + " created_at BIGINT NOT NULL,"
              + " updated_at BIGINT NOT NULL,"
              + " properties JSONB NOT NULL DEFAULT '{}'::jsonb,"
              + " PRIMARY KEY (source_id, edge_type, target_id, owner_id))");
      st.execute(
          "CREATE INDEX IF NOT EXISTS idx_"
              + prefix
              + "_edges_active_source ON "
              + qEdges
              + "(source_id, edge_type) WHERE removed = FALSE");
      st.execute(
          "CREATE INDEX IF NOT EXISTS idx_"
              + prefix
              + "_edges_active_target ON "
              + qEdges
              + "(target_id, edge_type) WHERE removed = FALSE");

      st.execute(
          "CREATE OR REPLACE VIEW "
              + qNet
              + " AS SELECT "
              + "(abs(('x' || substr(md5(source_id::text || '|' || target_id::text || '|' ||"
              + " edge_type::text || '|' || owner_id::text), 1, 16))::bit(64)::bigint)) AS id,"
              + " source_id AS source, target_id AS target,"
              + " 1.0::double precision AS cost, 1.0::double precision AS reverse_cost,"
              + " edge_type, owner_id, properties FROM "
              + qEdges
              + " WHERE removed = FALSE");

      String fnBody =
          "CREATE OR REPLACE FUNCTION public.dh_create_edge_type_if_not_exists(\n"
              + "    edge_type_id SMALLINT,\n"
              + "    type_name VARCHAR(200)\n"
              + ") RETURNS SMALLINT AS $fn$\n"
              + "DECLARE\n"
              + "    existing_id SMALLINT;\n"
              + "BEGIN\n"
              + "    SELECT et.id INTO existing_id FROM "
              + qEdgeTypes
              + " et\n"
              + "    WHERE et.type_name = dh_create_edge_type_if_not_exists.type_name;\n"
              + "    IF existing_id IS NOT NULL THEN\n"
              + "        RETURN existing_id;\n"
              + "    END IF;\n"
              + "    IF EXISTS (SELECT 1 FROM "
              + qEdgeTypes
              + " WHERE id = edge_type_id) THEN\n"
              + "        RAISE EXCEPTION 'Edge type ID % is already taken by a different edge"
              + " type', edge_type_id;\n"
              + "    END IF;\n"
              + "    INSERT INTO "
              + qEdgeTypes
              + " (id, type_name) VALUES (edge_type_id, dh_create_edge_type_if_not_exists.type_name);\n"
              + "    RETURN edge_type_id;\n"
              + "END;\n"
              + "$fn$ LANGUAGE plpgsql";
      st.execute(fnBody);

      connection.commit();
    } finally {
      connection.setAutoCommit(ac);
    }
  }
}
