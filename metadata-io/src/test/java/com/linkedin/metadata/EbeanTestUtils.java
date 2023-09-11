package com.linkedin.metadata;

import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.ServerConfig;
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
  private static ServerConfig createTestingH2ServerConfig() {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    dataSourceConfig.setUrl("jdbc:h2:mem:test;IGNORECASE=TRUE;mode=mysql;");
    dataSourceConfig.setDriver("org.h2.Driver");

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(true);
    serverConfig.setDdlRun(true);

    return serverConfig;
  }
}
