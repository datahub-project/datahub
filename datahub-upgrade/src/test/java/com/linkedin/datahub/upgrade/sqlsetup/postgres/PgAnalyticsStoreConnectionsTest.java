package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import io.ebean.Database;
import io.ebean.datasource.DataSourceConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class PgAnalyticsStoreConnectionsTest {

  @Test
  public void open_customUrl_ebeanIam_setsWrapperPluginsOnConnection() throws Exception {
    PgAnalyticsStoreOptions store = baseStore();
    Database fallback = mock(Database.class);
    DataSourceConfig ebeanDs = new DataSourceConfig();
    ebeanDs.setUrl("jdbc:postgresql://localhost:5432/analytics");
    ebeanDs.setUsername("ebean_user");
    ebeanDs.setPassword("ebean_pass");
    ebeanDs.setCustomProperties(Map.of("wrapperPlugins", "iam"));
    Connection expected = mock(Connection.class);

    try (MockedStatic<DriverManager> dm = Mockito.mockStatic(DriverManager.class)) {
      dm.when(
              () ->
                  DriverManager.getConnection(
                      eq("jdbc:postgresql://localhost:5432/analytics"), any(Properties.class)))
          .thenAnswer(
              inv -> {
                Properties props = inv.getArgument(1);
                assertEquals(props.getProperty("wrapperPlugins"), "iam");
                assertEquals(props.getProperty("user"), "ebean_user");
                return expected;
              });
      Connection got =
          PgAnalyticsStoreConnections.open(
              store, fallback, new PostgresSqlSetupProperties(), ebeanDs);
      assertEquals(got, expected);
    }
  }

  private static PgAnalyticsStoreOptions baseStore() {
    return PgAnalyticsStoreOptions.builder()
        .name("default")
        .schema("public")
        .tablePrefix("metadata_analytics")
        .partmanPartitionInterval("1 day")
        .partmanPremake(4)
        .forceOverwritePartmanConfig(false)
        .rawMaxAgeSeconds(1)
        .hourlyMaxAgeSeconds(1)
        .dailyMaxAgeSeconds(1)
        .monthlyMaxAgeSeconds(1)
        .inputLagSeconds(0)
        .maintenanceCronEnabled(false)
        .maintenanceIntervalSeconds(3600)
        .apiUsageFlushEnabled(false)
        .entityCountEnabled(false)
        .poolUrl("jdbc:postgresql://localhost:5432/analytics")
        .poolMinConnections(1)
        .poolMaxConnections(12)
        .poolMaxInactiveTimeSeconds(120)
        .poolMaxAgeMinutes(120)
        .poolLeakTimeMinutes(15)
        .poolWaitTimeoutMillis(1000)
        .build();
  }
}
