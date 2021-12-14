package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class LocalEbeanServerConfigFactory {

  @Value("${ebean.username}")
  private String ebeanDatasourceUsername;

  @Value("${ebean.password}")
  private String ebeanDatasourcePassword;

  @Value("${ebean.url}")
  private String ebeanDatasourceUrl;

  @Value("${ebean.driver}")
  private String ebeanDatasourceDriver;

  @Value("${ebean.minConnections:2}")
  private Integer ebeanMinConnections;

  @Value("${ebean.maxConnections:50}")
  private Integer ebeanMaxConnections;

  @Value("${ebean.maxInactiveTimeSeconds:120}")
  private Integer ebeanMaxInactiveTimeSecs;

  @Value("${ebean.maxAgeMinutes:120}")
  private Integer ebeanMaxAgeMinutes;

  @Value("${ebean.leakTimeMinutes:15}")
  private Integer ebeanLeakTimeMinutes;

  @Value("${ebean.autoCreateDdl:false}")
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
