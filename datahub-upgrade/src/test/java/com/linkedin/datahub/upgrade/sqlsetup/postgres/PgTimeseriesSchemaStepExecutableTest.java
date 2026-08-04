package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgTimeseriesSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgTimeseriesSchemaStepExecutableTest {

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
  public void executable_failsWhenPgTimeseriesOptionsNull() {
    when(postgresProperties.buildPgTimeseriesOptions()).thenReturn(null);

    PgTimeseriesSchemaStep step = new PgTimeseriesSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void executable_failsWhenPgPartmanNotInstalled() {
    partmanInstalled = false;
    when(postgresProperties.buildPgTimeseriesOptions()).thenReturn(sampleOptions(false, 604800));

    PgTimeseriesSchemaStep step = new PgTimeseriesSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void executable_failsWhenPartmanSchemaUnreadable() throws SQLException {
    when(postgresProperties.buildPgTimeseriesOptions()).thenReturn(sampleOptions(false, 604800));

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
              }
              return rs;
            });

    PgTimeseriesSchemaStep step = new PgTimeseriesSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void executable_succeedsWithoutCron_withRetentionUpdate() {
    when(postgresProperties.buildPgTimeseriesOptions()).thenReturn(sampleOptions(false, 604800));
    when(postgresProperties.normalizedPgCronSchema()).thenReturn("cron");

    PgTimeseriesSchemaStep step = new PgTimeseriesSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(connection).setAutoCommit(true);
  }

  @Test
  public void executable_succeedsWithRetentionClearWhenMaxAgeZero() {
    when(postgresProperties.buildPgTimeseriesOptions()).thenReturn(sampleOptions(false, 0));
    when(postgresProperties.normalizedPgCronSchema()).thenReturn("cron");

    PgTimeseriesSchemaStep step = new PgTimeseriesSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void executable_softSkipsCronWhenPgCronMissing() throws SQLException {
    when(postgresProperties.buildPgTimeseriesOptions()).thenReturn(sampleOptions(true, 604800));
    when(postgresProperties.normalizedPgCronSchema()).thenReturn("cron");

    Connection cronConn = mock(Connection.class);
    Statement cronStatement = mock(Statement.class);
    when(cronConn.createStatement()).thenReturn(cronStatement);
    when(cronStatement.executeQuery(anyString()))
        .thenAnswer(
            inv -> {
              String sql = inv.getArgument(0, String.class);
              ResultSet rs = mock(ResultSet.class);
              if (sql.contains("pg_available_extensions")) {
                when(rs.next()).thenReturn(false);
              } else if (sql.contains("FROM pg_extension WHERE extname")) {
                when(rs.next()).thenReturn(false);
              }
              return rs;
            });

    long skippedBefore = PgTimeseriesSchemaStep.CRON_REGISTRATION_SKIPPED.get();
    try (MockedStatic<PgCronAdminConnections> cronAdmin =
        mockStatic(PgCronAdminConnections.class)) {
      cronAdmin.when(() -> PgCronAdminConnections.open(postgresProperties)).thenReturn(cronConn);

      PgTimeseriesSchemaStep step = new PgTimeseriesSchemaStep(database, postgresProperties);
      UpgradeStepResult result = step.executable().apply(context);

      assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
      assertTrue(PgTimeseriesSchemaStep.CRON_REGISTRATION_SKIPPED.get() > skippedBefore);
    }
  }

  @Test
  public void executable_failsOnUnexpectedException() throws SQLException {
    when(postgresProperties.buildPgTimeseriesOptions()).thenReturn(sampleOptions(false, 604800));
    DataSource dataSource = mock(DataSource.class);
    when(database.dataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenThrow(new SQLException("boom"));

    PgTimeseriesSchemaStep step = new PgTimeseriesSchemaStep(database, postgresProperties);
    UpgradeStepResult result = step.executable().apply(context);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  private static PgTimeseriesSetupOptions sampleOptions(boolean cronEnabled, int retentionSeconds) {
    return new PgTimeseriesSetupOptions(
        "public", "metadata_timeseries", "1 day", 4, false, retentionSeconds, cronEnabled, 3600);
  }
}
