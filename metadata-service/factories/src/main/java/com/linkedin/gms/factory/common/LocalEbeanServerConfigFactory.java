package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.config.ServerConfig;
import io.ebean.datasource.DataSourceConfig;
import io.ebean.datasource.DataSourcePoolListener;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
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

  @Value("${ebean.waitTimeoutMillis:1000}")
  private Integer ebeanWaitTimeoutMillis;

  @Value("${ebean.autoCreateDdl:false}")
  private Boolean ebeanAutoCreate;

  @Value("${ebean.postgresUseIamAuth:false}")
  private Boolean postgresUseIamAuth;

  private DataSourcePoolListener getListenerToTrackCounts(String metricName) {
    final String counterName = "ebeans_connection_pool_size_" + metricName;
    return new DataSourcePoolListener() {
      @Override
      public void onAfterBorrowConnection(Connection connection) {
        MetricUtils.counter(counterName).inc();
      }

      @Override
      public void onBeforeReturnConnection(Connection connection) {
        MetricUtils.counter(counterName).dec();
      }
    };
  }

  private DataSourceConfig buildDataSourceConfig(String dataSourceUrl, String dataSourceType) {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername(ebeanDatasourceUsername);
    dataSourceConfig.setPassword(ebeanDatasourcePassword);
    dataSourceConfig.setUrl(dataSourceUrl);
    dataSourceConfig.setDriver(ebeanDatasourceDriver);
    dataSourceConfig.setMinConnections(ebeanMinConnections);
    dataSourceConfig.setMaxConnections(ebeanMaxConnections);
    dataSourceConfig.setMaxInactiveTimeSecs(ebeanMaxInactiveTimeSecs);
    dataSourceConfig.setMaxAgeMinutes(ebeanMaxAgeMinutes);
    dataSourceConfig.setLeakTimeMinutes(ebeanLeakTimeMinutes);
    dataSourceConfig.setWaitTimeoutMillis(ebeanWaitTimeoutMillis);
    dataSourceConfig.setListener(getListenerToTrackCounts(dataSourceType));
    // Adding IAM auth access for AWS Postgres
    if (postgresUseIamAuth) {
      Map<String, String> custom = new HashMap<>();
      custom.put("wrapperPlugins", "iam");
      dataSourceConfig.setCustomProperties(custom);
    }
    return dataSourceConfig;
  }

  @Bean(name = "gmsEbeanServiceConfig")
  protected ServerConfig createInstance() {
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.setName("gmsEbeanServiceConfig");
    serverConfig.setDataSourceConfig(buildDataSourceConfig(ebeanDatasourceUrl, "main"));
    serverConfig.setDdlGenerate(ebeanAutoCreate);
    serverConfig.setDdlRun(ebeanAutoCreate);
    return serverConfig;
  }
}
