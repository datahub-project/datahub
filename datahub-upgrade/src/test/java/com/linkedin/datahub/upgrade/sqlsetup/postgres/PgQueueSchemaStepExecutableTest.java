package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgQueueResolvedTopicCatalogEntry;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import com.linkedin.metadata.sqlsetup.postgres.pgqueue.PgQueueSqlSetupSupport;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgQueueSchemaStepExecutableTest {

  private Database database;
  private Connection connection;
  private Statement statement;
  private PostgresSqlSetupProperties postgresProperties;
  private UpgradeContext context;
  private UpgradeReport report;

  private boolean partmanAvailable = true;
  private boolean partmanInstalled = true;

  @BeforeMethod
  public void setUp() throws SQLException {
    database = mock(Database.class);
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    postgresProperties = mock(PostgresSqlSetupProperties.class);
    context = mock(UpgradeContext.class);
    report = mock(UpgradeReport.class);
    partmanAvailable = true;
    partmanInstalled = true;

    DataSource dataSource = mock(DataSource.class);
    when(database.dataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    when(connection.getCatalog()).thenReturn("datahub");
    when(context.report()).thenReturn(report);
    when(statement.execute(anyString())).thenReturn(false);
    when(statement.getUpdateCount()).thenReturn(-1);
    when(statement.executeQuery(anyString()))
        .thenAnswer(
            inv -> {
              String sql = inv.getArgument(0, String.class);
              ResultSet rs = mock(ResultSet.class);
              if (sql.contains("pg_available_extensions")) {
                when(rs.next()).thenReturn(partmanAvailable);
              } else if (sql.contains("FROM pg_extension WHERE extname")) {
                when(rs.next()).thenReturn(partmanInstalled);
              } else if (sql.contains("pg_namespace") && sql.contains("pg_partman")) {
                when(rs.next()).thenReturn(partmanInstalled);
                when(rs.getString(1)).thenReturn("partman");
              } else if (sql.contains("MAX(retention_max_age_seconds)")) {
                when(rs.next()).thenReturn(true);
                when(rs.getInt(1)).thenReturn(86400);
              }
              return rs;
            });

    when(connection.prepareStatement(anyString()))
        .thenAnswer(
            inv -> {
              String sql = inv.getArgument(0, String.class);
              PreparedStatement ps = mock(PreparedStatement.class);
              if (sql.contains("pg_advisory_xact_lock")) {
                ResultSet rs = mock(ResultSet.class);
                when(rs.next()).thenReturn(true);
                when(ps.executeQuery()).thenReturn(rs);
              } else if (sql.contains("schema_migration") && sql.trim().startsWith("SELECT")) {
                ResultSet rs = mock(ResultSet.class);
                when(rs.next()).thenReturn(false);
                when(ps.executeQuery()).thenReturn(rs);
              } else {
                when(ps.executeUpdate()).thenReturn(1);
              }
              return ps;
            });
  }

  @Test
  public void executable_failsWhenPgQueueOptionsNull() {
    when(postgresProperties.buildPgQueueOptions()).thenReturn(null);

    PgQueueSchemaStep step = new PgQueueSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void executable_failsWhenPgPartmanNotInstalled() throws SQLException {
    partmanInstalled = false;
    when(postgresProperties.buildPgQueueOptions()).thenReturn(sampleOptions(false));

    PgQueueSchemaStep step = new PgQueueSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void executable_failsWhenPartmanSchemaUnreadable() throws SQLException {
    when(postgresProperties.buildPgQueueOptions()).thenReturn(sampleOptions(false));

    when(statement.executeQuery(anyString()))
        .thenAnswer(
            inv -> {
              String sql = inv.getArgument(0, String.class);
              ResultSet rs = mock(ResultSet.class);
              if (sql.contains("pg_available_extensions")) {
                when(rs.next()).thenReturn(true);
              } else if (sql.contains("FROM pg_extension WHERE extname = 'pg_partman'")) {
                when(rs.next()).thenReturn(true);
              } else if (sql.contains("pg_namespace") && sql.contains("pg_partman")) {
                when(rs.next()).thenReturn(false);
              } else if (sql.contains("MAX(retention_max_age_seconds)")) {
                when(rs.next()).thenReturn(true);
                when(rs.getInt(1)).thenReturn(0);
              }
              return rs;
            });

    PgQueueSchemaStep step = new PgQueueSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void executable_succeedsWithoutCronOrCatalog() throws SQLException {
    when(postgresProperties.buildPgQueueOptions()).thenReturn(sampleOptions(false));
    when(postgresProperties.normalizedPgCronSchema()).thenReturn("cron");
    PgQueueSchemaStep step = new PgQueueSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(connection).setAutoCommit(true);
    verify(connection).commit();
  }

  @Test
  public void executable_upsertsTopicCatalog() throws SQLException {
    PgQueueSetupOptions options = sampleOptions(true);
    when(postgresProperties.buildPgQueueOptions()).thenReturn(options);
    when(postgresProperties.normalizedPgCronSchema()).thenReturn("cron");

    PreparedStatement catalogPs = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString()))
        .thenAnswer(
            inv -> {
              String sql = inv.getArgument(0, String.class);
              if (sql.contains("INSERT INTO") && sql.contains("_topic")) {
                return catalogPs;
              }
              PreparedStatement ps = mock(PreparedStatement.class);
              if (sql.contains("pg_advisory_xact_lock")) {
                ResultSet rs = mock(ResultSet.class);
                when(rs.next()).thenReturn(true);
                when(ps.executeQuery()).thenReturn(rs);
              } else if (sql.contains("schema_migration") && sql.trim().startsWith("SELECT")) {
                ResultSet rs = mock(ResultSet.class);
                when(rs.next()).thenReturn(false);
                when(ps.executeQuery()).thenReturn(rs);
              } else {
                when(ps.executeUpdate()).thenReturn(1);
              }
              return ps;
            });

    PgQueueSchemaStep step = new PgQueueSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(catalogPs).setString(1, "MetadataChangeProposal_v1");
    verify(catalogPs).executeUpdate();
  }

  @Test
  public void queryMaxTopicRetentionMaxAgeSeconds_readsMax() throws SQLException {
    int max =
        PgQueueSqlSetupSupport.queryMaxTopicRetentionMaxAgeSeconds(
            connection, "queue", "metadata_queue");
    assertEquals(max, 86400);
  }

  @Test
  public void resolvePgPartmanExtensionSchema_returnsNamespace() throws SQLException {
    assertEquals(PgQueueSqlSetupSupport.resolvePgPartmanExtensionSchema(connection), "partman");
  }

  @Test
  public void partmanRetentionUpdateSql_updatesPartConfig() {
    String sql =
        PgQueueSqlSetupSupport.partmanRetentionUpdateSql(
            "partman", "queue", "7 days", "metadata_queue");
    assertTrue(sql.contains("UPDATE \"partman\".part_config"));
    assertTrue(sql.contains("retention = '7 days'"));
    assertTrue(sql.contains("queue.metadata_queue_message"));
  }

  @Test
  public void quotePgIdentifier_rejectsEmpty() {
    try {
      PostgresSqlUtils.quotePgIdentifier("");
      throw new AssertionError("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("required"));
    }
  }

  private static PgQueueSetupOptions sampleOptions(boolean withCatalog) {
    List<PgQueueResolvedTopicCatalogEntry> catalog =
        withCatalog
            ? List.of(
                new PgQueueResolvedTopicCatalogEntry(
                    "mcp", "MetadataChangeProposal_v1", 3, "0-9", 604800, 0L, 0L, false, 1))
            : List.of();
    return new PgQueueSetupOptions(
        "queue",
        "metadata_queue",
        3,
        30,
        "0-9",
        604800,
        0L,
        0L,
        "application/json",
        "1 day",
        4,
        false,
        3600,
        1000,
        false,
        1,
        catalog);
  }
}
