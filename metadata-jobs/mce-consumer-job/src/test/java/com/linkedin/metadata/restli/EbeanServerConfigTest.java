package com.linkedin.metadata.restli;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.ebean.datasource.DataSourceConfig;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

public class EbeanServerConfigTest {

  @Test
  public void buildDataSourceConfig_appliesPostgresMetadataSchema() {
    EbeanServerConfig config = postgresConfig("dhub");

    DataSourceConfig dataSource =
        config.buildDataSourceConfig("jdbc:postgresql://localhost:5432/datahub", null);

    assertEquals(
        dataSource.getUrl(), "jdbc:postgresql://localhost:5432/datahub?currentSchema=dhub");
  }

  @Test
  public void buildDataSourceConfig_leavesPublicSchemaUrlUnchanged() {
    EbeanServerConfig config = postgresConfig("public");
    String url = "jdbc:postgresql://localhost:5432/datahub";

    DataSourceConfig dataSource = config.buildDataSourceConfig(url, null);

    assertEquals(dataSource.getUrl(), url);
  }

  @Test
  public void buildDataSourceConfig_rejectsConflictingCurrentSchema() {
    EbeanServerConfig config = postgresConfig("dhub");

    try {
      config.buildDataSourceConfig(
          "jdbc:postgresql://localhost:5432/datahub?currentSchema=other", null);
      throw new AssertionError("expected conflicting currentSchema to fail");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("currentSchema"));
    }
  }

  private static EbeanServerConfig postgresConfig(String schema) {
    EbeanServerConfig config = new EbeanServerConfig();
    ReflectionTestUtils.setField(config, "ebeanDatasourceUsername", "datahub");
    ReflectionTestUtils.setField(config, "ebeanDatasourcePassword", "datahub");
    ReflectionTestUtils.setField(config, "ebeanDatasourceDriver", "org.postgresql.Driver");
    ReflectionTestUtils.setField(config, "ebeanMinConnections", 1);
    ReflectionTestUtils.setField(config, "ebeanMaxConnections", 2);
    ReflectionTestUtils.setField(config, "ebeanMaxInactiveTimeSecs", 120);
    ReflectionTestUtils.setField(config, "ebeanMaxAgeMinutes", 120);
    ReflectionTestUtils.setField(config, "ebeanLeakTimeMinutes", 15);
    ReflectionTestUtils.setField(config, "ebeanWaitTimeoutMillis", 1000);
    ReflectionTestUtils.setField(config, "useIamAuth", false);
    ReflectionTestUtils.setField(config, "postgresUseIamAuth", false);
    ReflectionTestUtils.setField(config, "cloudProvider", "auto");
    ReflectionTestUtils.setField(config, "postgresMetadataSchema", schema);
    return config;
  }
}
