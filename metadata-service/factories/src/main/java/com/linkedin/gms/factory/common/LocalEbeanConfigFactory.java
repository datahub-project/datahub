package com.linkedin.gms.factory.common;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import io.ebean.datasource.DataSourcePoolListener;
import java.sql.Connection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class LocalEbeanConfigFactory {

  @Value("${ebean.username}")
  private String ebeanDatasourceUsername;

  @Value("${ebean.password}")
  private String ebeanDatasourcePassword;

  @Value("${ebean.driver}")
  private String ebeanDatasourceDriver;

  @Value("${ebean.url}")
  private String ebeanDatasourceUrl;

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

  @Value("${ebean.useIamAuth:false}")
  private Boolean useIamAuth;

  @Value("${ebean.cloudProvider:auto}")
  private String cloudProvider;

  // Environment variable properties for cloud detection
  @Value("${AWS_REGION:#{null}}")
  private String awsRegion;

  @Value("${AWS_ACCESS_KEY_ID:#{null}}")
  private String awsAccessKeyId;

  @Value("${AWS_SECRET_ACCESS_KEY:#{null}}")
  private String awsSecretAccessKey;

  @Value("${AWS_SESSION_TOKEN:#{null}}")
  private String awsSessionToken;

  @Value("${GOOGLE_APPLICATION_CREDENTIALS:#{null}}")
  private String googleApplicationCredentials;

  @Value("${GCP_PROJECT:#{null}}")
  private String gcpProject;

  @Value("${INSTANCE_CONNECTION_NAME:#{null}}")
  private String instanceConnectionName;

  public static DataSourcePoolListener getListenerToTrackCounts(
      MetricUtils metricUtils, String metricName) {
    final String counterName = "ebeans_connection_pool_size_" + metricName;
    return new DataSourcePoolListener() {
      @Override
      public void onAfterBorrowConnection(Connection connection) {
        if (metricUtils != null) metricUtils.increment(counterName, 1);
      }

      @Override
      public void onBeforeReturnConnection(Connection connection) {
        if (metricUtils != null) metricUtils.increment(counterName, -1);
      }
    };
  }

  @Bean("ebeanDataSourceConfig")
  public DataSourceConfig buildDataSourceConfig(MetricUtils metricUtils) {
    return buildDataSourceConfig(ebeanDatasourceUrl, metricUtils);
  }

  public DataSourceConfig buildDataSourceConfig(String dataSourceUrl, MetricUtils metricUtils) {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();

    // Configure cross-cloud IAM authentication
    boolean shouldUseIam = useIamAuth || postgresUseIamAuth;

    CrossCloudIamUtils.CrossCloudConfig crossCloudConfig =
        CrossCloudIamUtils.configureCrossCloudIam(
            dataSourceUrl,
            ebeanDatasourceDriver,
            shouldUseIam,
            cloudProvider,
            awsRegion,
            awsAccessKeyId,
            awsSecretAccessKey,
            awsSessionToken,
            googleApplicationCredentials,
            gcpProject,
            instanceConnectionName);

    dataSourceConfig.setUsername(ebeanDatasourceUsername);
    dataSourceConfig.setPassword(ebeanDatasourcePassword);
    dataSourceConfig.setUrl(crossCloudConfig.url);
    dataSourceConfig.setDriver(crossCloudConfig.driver);
    dataSourceConfig.setMinConnections(ebeanMinConnections);
    dataSourceConfig.setMaxConnections(ebeanMaxConnections);
    dataSourceConfig.setMaxInactiveTimeSecs(ebeanMaxInactiveTimeSecs);
    dataSourceConfig.setMaxAgeMinutes(ebeanMaxAgeMinutes);
    dataSourceConfig.setLeakTimeMinutes(ebeanLeakTimeMinutes);
    dataSourceConfig.setWaitTimeoutMillis(ebeanWaitTimeoutMillis);
    dataSourceConfig.setListener(getListenerToTrackCounts(metricUtils, "main"));

    // Set custom properties for IAM authentication
    if (crossCloudConfig.customProperties != null) {
      dataSourceConfig.setCustomProperties(crossCloudConfig.customProperties);
    }

    return dataSourceConfig;
  }

  @Bean(name = "gmsEbeanDatabaseConfig")
  protected DatabaseConfig createInstance(
      @Qualifier("ebeanDataSourceConfig") DataSourceConfig config) {
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("gmsEbeanDatabaseConfig");
    serverConfig.setDataSourceConfig(config);
    serverConfig.setDdlGenerate(ebeanAutoCreate);
    serverConfig.setDdlRun(ebeanAutoCreate);
    return serverConfig;
  }
}
