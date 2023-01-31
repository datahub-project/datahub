package com.linkedin.metadata;

import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;

import javax.annotation.Nonnull;

public class EbeanTestUtils {

  private EbeanTestUtils() {
  }

  @Nonnull
  public static Database createTestServer() {
    return DatabaseFactory.create(createTestingH2ServerConfig());
  }

  @Nonnull
  private static DatabaseConfig createTestingH2ServerConfig() {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    dataSourceConfig.setUrl("jdbc:h2:mem:;IGNORECASE=TRUE;");
    dataSourceConfig.setDriver("org.h2.Driver");

    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(true);
    serverConfig.setDdlRun(true);

    return serverConfig;
  }
}
