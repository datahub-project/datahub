package com.linkedin.gms.factory.common;

import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class LocalEbeanServerConfigFactory {

  @Value("${EBEAN_DATASOURCE_USERNAME:datahub}")
  private String ebeanDatasourceUsername;

  @Value("${EBEAN_DATASOURCE_PASSWORD:datahub}")
  private String ebeanDatasourcePassword;

  @Value("${EBEAN_DATASOURCE_URL:jdbc:mysql://localhost:3306/datahub}")
  private String ebeanDatasourceUrl;

  @Value("${EBEAN_DATASOURCE_DRIVER:com.mysql.jdbc.Driver}")
  private String ebeanDatasourceDriver;

  @Value("${EBEAN_MIN_CONNECTIONS:2}")
  private Integer ebeanMinConnections;

  @Value("${EBEAN_MAX_CONNECTIONS:50}")
  private Integer ebeanMaxConnections;

  @Value("${EBEAN_MAX_INACTIVE_TIME_IN_SECS:120}")
  private Integer ebeanMaxInactiveTimeSecs;

  @Value("${EBEAN_MAX_AGE_MINUTES:120}")
  private Integer ebeanMaxAgeMinutes;

  @Value("${EBEAN_LEAK_TIME_MINUTES:15}")
  private Integer ebeanLeakTimeMinutes;

  @Value("${EBEAN_AUTOCREATE:false}")
  private Boolean ebeanAutoCreate;

  @Bean(name = "gmsEbeanServiceConfig")
  protected ServerConfig createInstance() {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername(ebeanDatasourceUsername);
    dataSourceConfig.setPassword(ebeanDatasourcePassword);
    dataSourceConfig.setUrl(ebeanDatasourceUrl);
    dataSourceConfig.setDriver(ebeanDatasourceDriver);
    dataSourceConfig.setMinConnections(ebeanMinConnections);
    dataSourceConfig.setMaxConnections(ebeanMaxConnections);
    dataSourceConfig.setMaxInactiveTimeSecs(ebeanMaxInactiveTimeSecs);
    dataSourceConfig.setMaxAgeMinutes(ebeanMaxAgeMinutes);
    dataSourceConfig.setLeakTimeMinutes(ebeanLeakTimeMinutes);

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gmsEbeanServiceConfig");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(ebeanAutoCreate);
    serverConfig.setDdlRun(ebeanAutoCreate);
    return serverConfig;
  }
}
