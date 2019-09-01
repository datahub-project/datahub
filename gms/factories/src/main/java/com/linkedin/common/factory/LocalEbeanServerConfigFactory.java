package com.linkedin.common.factory;

import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Slf4j
@Configuration
@PropertySource("classpath:gms.properties")
public class LocalEbeanServerConfigFactory {

  @Autowired
  private Environment env;

  @Bean(name = "gmsEbeanServiceConfig")
  protected ServerConfig createInstance() {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername(env.getRequiredProperty("ebean.datasourceUsername"));
    dataSourceConfig.setPassword(env.getRequiredProperty("ebean.datasourcePassword"));
    dataSourceConfig.setUrl(env.getRequiredProperty("ebean.datasourceUrl"));
    dataSourceConfig.setDriver(env.getRequiredProperty("ebean.datasourceDriver"));
    dataSourceConfig.setMinConnections(Integer.valueOf(env.getProperty("ebean.minConnections", "2")));
    dataSourceConfig.setMaxConnections(Integer.valueOf(env.getProperty("ebean.maxConnections", "50")));
    dataSourceConfig.setMaxInactiveTimeSecs(Integer.valueOf(env.getProperty("ebean.maxInactiveTimeSecs", "120")));
    dataSourceConfig.setMaxAgeMinutes(Integer.valueOf(env.getProperty("ebean.maxAgeMinutes", "120")));
    dataSourceConfig.setLeakTimeMinutes(Integer.valueOf(env.getProperty("ebean.leakTimeMinutes", "15")));

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gmsEbeanServiceConfig");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(Boolean.valueOf(env.getProperty("ebean.autoCreate", "false")));
    serverConfig.setDdlRun(Boolean.valueOf(env.getProperty("ebean.autoCreate", "false")));

    return serverConfig;
  }
}
