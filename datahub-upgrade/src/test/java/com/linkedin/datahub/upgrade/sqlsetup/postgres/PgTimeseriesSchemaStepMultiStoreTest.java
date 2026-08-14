package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgTimeseriesSetupOptions;
import com.linkedin.metadata.config.postgres.PgTimeseriesStoreOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.sql.DataSource;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

public class PgTimeseriesSchemaStepMultiStoreTest {

  @Test
  public void executable_opensConnectionPerStore() throws Exception {
    Database database = mock(Database.class);
    DataSource dataSource = mock(DataSource.class);
    when(database.dataSource()).thenReturn(dataSource);

    Connection defaultConn = mockConnection("datahub");
    Connection longConn = mockConnection("datahub_long");
    when(dataSource.getConnection()).thenReturn(defaultConn);

    PostgresSqlSetupProperties props = mock(PostgresSqlSetupProperties.class);
    when(props.buildPgTimeseriesOptions()).thenReturn(twoStores());
    when(props.normalizedPgCronSchema()).thenReturn("cron");

    UpgradeContext context = mock(UpgradeContext.class);
    UpgradeReport report = mock(UpgradeReport.class);
    when(context.report()).thenReturn(report);

    try (MockedStatic<PgTimeseriesStoreConnections> storeConns =
        mockStatic(PgTimeseriesStoreConnections.class)) {
      storeConns
          .when(() -> PgTimeseriesStoreConnections.open(any(), eq(database), eq(props), any()))
          .thenAnswer(
              inv -> {
                PgTimeseriesStoreOptions store = inv.getArgument(0);
                return "long".equals(store.getName()) ? longConn : defaultConn;
              });

      PgTimeseriesSchemaStep step = new PgTimeseriesSchemaStep(database, props);
      UpgradeStepResult result = step.executable().apply(context);

      assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
      storeConns.verify(
          () -> PgTimeseriesStoreConnections.open(any(), eq(database), eq(props), any()), times(2));
    }
  }

  private static PgTimeseriesSetupOptions twoStores() {
    return new PgTimeseriesSetupOptions(
        "default",
        Map.of(
            "default", store("default", "metadata_timeseries", null),
            "long", store("long", "metadata_timeseries_long", "jdbc:postgresql://other/db")),
        Map.of());
  }

  private static PgTimeseriesStoreOptions store(String name, String prefix, String poolUrl) {
    return PgTimeseriesStoreOptions.builder()
        .name(name)
        .schema("public")
        .tablePrefix(prefix)
        .partmanPartitionInterval("1 day")
        .partmanPremake(4)
        .retentionMaxAgeSeconds(604800)
        .maintenanceCronEnabled(false)
        .maintenanceIntervalSeconds(3600)
        .poolUrl(poolUrl)
        .poolMinConnections(1)
        .poolMaxConnections(12)
        .poolMaxInactiveTimeSeconds(120)
        .poolMaxAgeMinutes(120)
        .poolLeakTimeMinutes(15)
        .poolWaitTimeoutMillis(1000)
        .build();
  }

  private static Connection mockConnection(String catalog) throws SQLException {
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    when(connection.getCatalog()).thenReturn(catalog);
    when(statement.execute(anyString())).thenReturn(false);
    when(statement.getUpdateCount()).thenReturn(-1);
    when(statement.executeQuery(anyString()))
        .thenAnswer(
            inv -> {
              String sql = inv.getArgument(0, String.class);
              ResultSet rs = mock(ResultSet.class);
              if (sql.contains("pg_available_extensions")) {
                when(rs.next()).thenReturn(true);
              } else if (sql.contains("FROM pg_extension WHERE extname")) {
                when(rs.next()).thenReturn(true);
              } else if (sql.contains("pg_namespace") && sql.contains("pg_partman")) {
                when(rs.next()).thenReturn(true);
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
    return connection;
  }
}
