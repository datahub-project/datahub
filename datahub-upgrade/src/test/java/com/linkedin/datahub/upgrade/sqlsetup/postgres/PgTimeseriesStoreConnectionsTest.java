package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PgTimeseriesStoreOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties.PgCron.Iam;
import io.ebean.Database;
import io.ebean.datasource.DataSourceConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class PgTimeseriesStoreConnectionsTest {

  @Test
  public void open_customUrl_usesEbeanDataSourceConfigCredentials() throws Exception {
    PgTimeseriesStoreOptions store =
        baseStore().toBuilder().poolUrl("jdbc:postgresql://localhost:5432/ts").build();
    Database fallback = mock(Database.class);
    DataSourceConfig ebeanDs = new DataSourceConfig();
    ebeanDs.setUsername("ebean_user");
    ebeanDs.setPassword("ebean_pass");
    Connection expected = mock(Connection.class);

    try (MockedStatic<DriverManager> dm = Mockito.mockStatic(DriverManager.class)) {
      dm.when(
              () ->
                  DriverManager.getConnection(
                      "jdbc:postgresql://localhost:5432/ts", "ebean_user", "ebean_pass"))
          .thenReturn(expected);
      Connection got =
          PgTimeseriesStoreConnections.open(
              store, fallback, new PostgresSqlSetupProperties(), ebeanDs);
      assertEquals(got, expected);
    }
  }

  @Test
  public void open_customUrl_missingEbeanConfig_usesEmptyCredentials() throws Exception {
    PgTimeseriesStoreOptions store =
        baseStore().toBuilder().poolUrl("jdbc:postgresql://localhost:5432/ts").build();
    Database fallback = mock(Database.class);
    Connection expected = mock(Connection.class);

    try (MockedStatic<DriverManager> dm = Mockito.mockStatic(DriverManager.class)) {
      dm.when(() -> DriverManager.getConnection("jdbc:postgresql://localhost:5432/ts", "", ""))
          .thenReturn(expected);
      Connection got =
          PgTimeseriesStoreConnections.open(
              store, fallback, new PostgresSqlSetupProperties(), null);
      assertEquals(got, expected);
    }
  }

  @Test
  public void shouldUseIam_followsEbeanPoolWhenPgCronIamUnset() {
    DataSourceConfig ebeanDs = new DataSourceConfig();
    ebeanDs.setCustomProperties(Map.of("wrapperPlugins", "failover, iam"));
    assertTrue(PgTimeseriesStoreConnections.shouldUseIam(null, ebeanDs));
    assertFalse(PgTimeseriesStoreConnections.shouldUseIam(null, new DataSourceConfig()));
    Iam iam = new Iam();
    iam.setUseIamAuth(true);
    assertTrue(PgTimeseriesStoreConnections.shouldUseIam(iam, null));
  }

  @Test
  public void inferCloudProvider_wrapperPluginsIam_isAws() {
    DataSourceConfig ebeanDs = new DataSourceConfig();
    ebeanDs.setCustomProperties(Map.of("wrapperPlugins", "failover, iam"));
    assertEquals(
        PgTimeseriesStoreConnections.inferCloudProvider(ebeanDs, "jdbc:postgresql://localhost/ts"),
        "aws");
  }

  @Test
  public void open_customUrl_ebeanIam_setsWrapperPluginsOnConnection() throws Exception {
    PgTimeseriesStoreOptions store =
        baseStore().toBuilder().poolUrl("jdbc:postgresql://localhost:5432/ts").build();
    Database fallback = mock(Database.class);
    DataSourceConfig ebeanDs = new DataSourceConfig();
    ebeanDs.setUsername("ebean_user");
    ebeanDs.setPassword("ebean_pass");
    ebeanDs.setCustomProperties(Map.of("wrapperPlugins", "iam"));
    Connection expected = mock(Connection.class);

    try (MockedStatic<DriverManager> dm = Mockito.mockStatic(DriverManager.class)) {
      dm.when(
              () ->
                  DriverManager.getConnection(
                      eq("jdbc:postgresql://localhost:5432/ts"), any(Properties.class)))
          .thenAnswer(
              inv -> {
                Properties props = inv.getArgument(1);
                assertEquals(props.getProperty("wrapperPlugins"), "iam");
                assertEquals(props.getProperty("user"), "ebean_user");
                return expected;
              });
      Connection got =
          PgTimeseriesStoreConnections.open(
              store, fallback, new PostgresSqlSetupProperties(), ebeanDs);
      assertEquals(got, expected);
    }
  }

  @Test
  public void inferCloudProvider_enableIamAuth_isGcp() {
    DataSourceConfig ebeanDs = new DataSourceConfig();
    ebeanDs.setCustomProperties(Map.of("enableIamAuth", "true"));
    assertEquals(
        PgTimeseriesStoreConnections.inferCloudProvider(ebeanDs, "jdbc:postgresql://localhost/ts"),
        "gcp");
  }

  @Test
  public void open_customUrl_ebeanIam_usesPropertiesConnection() throws Exception {
    PgTimeseriesStoreOptions store =
        baseStore().toBuilder().poolUrl("jdbc:postgresql://localhost:5432/ts").build();
    Database fallback = mock(Database.class);
    DataSourceConfig ebeanDs = new DataSourceConfig();
    ebeanDs.setUsername("ebean_user");
    ebeanDs.setPassword("ebean_pass");
    ebeanDs.setCustomProperties(Map.of("wrapperPlugins", "iam"));
    Connection expected = mock(Connection.class);

    try (MockedStatic<DriverManager> dm = Mockito.mockStatic(DriverManager.class)) {
      dm.when(
              () ->
                  DriverManager.getConnection(
                      eq("jdbc:postgresql://localhost:5432/ts"), any(Properties.class)))
          .thenReturn(expected);
      Connection got =
          PgTimeseriesStoreConnections.open(
              store, fallback, new PostgresSqlSetupProperties(), ebeanDs);
      assertEquals(got, expected);
    }
  }

  @Test
  public void open_blankPoolUrl_usesFallbackServerConnection() throws Exception {
    PgTimeseriesStoreOptions store = baseStore();
    Database fallback = mock(Database.class);
    DataSource ds = mock(DataSource.class);
    Connection expected = mock(Connection.class);
    when(fallback.dataSource()).thenReturn(ds);
    when(ds.getConnection()).thenReturn(expected);

    Connection got =
        PgTimeseriesStoreConnections.open(store, fallback, new PostgresSqlSetupProperties(), null);
    assertNotNull(got);
    assertEquals(got, expected);
  }

  private static PgTimeseriesStoreOptions baseStore() {
    return PgTimeseriesStoreOptions.builder()
        .name("default")
        .schema("public")
        .tablePrefix("metadata_timeseries")
        .partmanPartitionInterval("1 day")
        .partmanPremake(4)
        .retentionMaxAgeSeconds(7776000)
        .maintenanceIntervalSeconds(3600)
        .poolMinConnections(1)
        .poolMaxConnections(12)
        .poolMaxInactiveTimeSeconds(120)
        .poolMaxAgeMinutes(120)
        .poolLeakTimeMinutes(15)
        .poolWaitTimeoutMillis(1000)
        .build();
  }
}
