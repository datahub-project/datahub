package com.linkedin.metadata;

import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import javax.annotation.Nonnull;

public class EbeanTestUtils {

  private EbeanTestUtils() {}

  @Nonnull
  public static Database createTestServer(String instanceId) {
    return DatabaseFactory.create(createTestingH2ServerConfig(instanceId));
  }

  @Nonnull
  private static DatabaseConfig createTestingH2ServerConfig(String instanceId) {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    dataSourceConfig.setUrl(
        String.format("jdbc:h2:mem:%s;IGNORECASE=TRUE;mode=mysql;", instanceId));
    dataSourceConfig.setDriver("org.h2.Driver");

    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(true);
    serverConfig.setDdlRun(true);

    return serverConfig;
  }
}
