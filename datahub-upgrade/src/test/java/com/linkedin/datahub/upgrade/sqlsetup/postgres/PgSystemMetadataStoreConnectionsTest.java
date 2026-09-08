package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.postgres.PgSystemMetadataSetupOptions;
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

public class PgSystemMetadataStoreConnectionsTest {

  @Test
  public void open_customUrl_ebeanIam_setsWrapperPluginsOnConnection() throws Exception {
    PgSystemMetadataSetupOptions options =
        PgSystemMetadataSetupOptions.builder()
            .schema("public")
            .tablePrefix("metadata_system_metadata")
            .tableName("system_metadata_v2")
            .poolUrl("jdbc:postgresql://localhost:5432/sysmeta")
            .build();
    Database fallback = mock(Database.class);
    DataSourceConfig ebeanDs = new DataSourceConfig();
    ebeanDs.setUrl("jdbc:postgresql://localhost:5432/sysmeta");
    ebeanDs.setUsername("ebean_user");
    ebeanDs.setPassword("ebean_pass");
    ebeanDs.setCustomProperties(Map.of("wrapperPlugins", "iam"));
    Connection expected = mock(Connection.class);

    try (MockedStatic<DriverManager> dm = Mockito.mockStatic(DriverManager.class)) {
      dm.when(
              () ->
                  DriverManager.getConnection(
                      eq("jdbc:postgresql://localhost:5432/sysmeta"), any(Properties.class)))
          .thenAnswer(
              inv -> {
                Properties props = inv.getArgument(1);
                assertEquals(props.getProperty("wrapperPlugins"), "iam");
                assertEquals(props.getProperty("user"), "ebean_user");
                return expected;
              });
      Connection got =
          PgSystemMetadataStoreConnections.open(
              options, fallback, new PostgresSqlSetupProperties(), ebeanDs);
      assertEquals(got, expected);
    }
  }
}
