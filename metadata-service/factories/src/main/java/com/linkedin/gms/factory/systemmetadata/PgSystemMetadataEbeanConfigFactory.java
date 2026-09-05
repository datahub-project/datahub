package com.linkedin.gms.factory.systemmetadata;

import static com.linkedin.gms.factory.common.LocalEbeanConfigFactory.getListenerToTrackCounts;

import com.linkedin.gms.factory.common.CrossCloudIamUtils;
import com.linkedin.gms.factory.common.EbeanPoolDefaults;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.Database;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Dedicated Ebean {@link Database} for the PostgreSQL system-metadata store. DDL is applied by
 * {@code PgSystemMetadataSchemaStep} (main Ebean unless {@code pool.url} is overridden); runtime
 * ops bind here.
 */
@Slf4j
@Configuration
@Conditional(PgSystemMetadataRuntimePoolEnabledCondition.class)
public class PgSystemMetadataEbeanConfigFactory {

  @Value("${postgres.pgSystemMetadata.pool.username}")
  private String username;

  @Value("${postgres.pgSystemMetadata.pool.password}")
  private String password;

  @Value("${postgres.pgSystemMetadata.pool.driver}")
  private String driver;

  @Value("${postgres.pgSystemMetadata.pool.url}")
  private String url;

  @Value("${postgres.pgSystemMetadata.pool.minConnections:1}")
  private Integer minConnections;

  @Value("${postgres.pgSystemMetadata.pool.maxConnections:8}")
  private Integer maxConnections;

  @Value("${postgres.pgSystemMetadata.pool.maxInactiveTimeSeconds:120}")
  private Integer maxInactiveTimeSecs;

  @Value("${postgres.pgSystemMetadata.pool.maxAgeMinutes:120}")
  private Integer maxAgeMinutes;

  @Value("${postgres.pgSystemMetadata.pool.leakTimeMinutes:15}")
  private Integer leakTimeMinutes;

  @Value("${postgres.pgSystemMetadata.pool.waitTimeoutMillis:1000}")
  private Integer waitTimeoutMillis;

  @Value("${ebean.postgresUseIamAuth:false}")
  private Boolean postgresUseIamAuth;

  @Value("${ebean.useIamAuth:false}")
  private Boolean useIamAuth;

  @Value("${ebean.cloudProvider:auto}")
  private String cloudProvider;

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

  @Bean("pgSystemMetadataDataSourceConfig")
  public DataSourceConfig buildDataSourceConfig(MetricUtils metricUtils) {
    return buildDataSourceConfig(url, metricUtils);
  }

  public DataSourceConfig buildDataSourceConfig(String dataSourceUrl, MetricUtils metricUtils) {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();

    boolean shouldUseIam = useIamAuth || postgresUseIamAuth;

    CrossCloudIamUtils.CrossCloudConfig crossCloudConfig =
        CrossCloudIamUtils.configureCrossCloudIam(
            dataSourceUrl,
            driver,
            shouldUseIam,
            cloudProvider,
            awsRegion,
            awsAccessKeyId,
            awsSecretAccessKey,
            awsSessionToken,
            googleApplicationCredentials,
            gcpProject,
            instanceConnectionName);

    dataSourceConfig.setUsername(username);
    dataSourceConfig.setPassword(password);
    dataSourceConfig.setUrl(crossCloudConfig.url);
    dataSourceConfig.setDriver(crossCloudConfig.driver);
    dataSourceConfig.setMinConnections(minConnections);
    dataSourceConfig.setMaxConnections(maxConnections);
    dataSourceConfig.setMaxInactiveTimeSecs(maxInactiveTimeSecs);
    dataSourceConfig.setMaxAgeMinutes(maxAgeMinutes);
    dataSourceConfig.setLeakTimeMinutes(leakTimeMinutes);
    dataSourceConfig.setWaitTimeoutMillis(waitTimeoutMillis);
    dataSourceConfig.setListener(getListenerToTrackCounts(metricUtils, "pgsystemmetadata"));
    EbeanPoolDefaults.applyDefaultTransactionIsolation(dataSourceConfig);

    if (crossCloudConfig.customProperties != null) {
      dataSourceConfig.setCustomProperties(crossCloudConfig.customProperties);
    }

    return dataSourceConfig;
  }

  @Bean(name = "pgSystemMetadataEbeanDatabaseConfig")
  protected DatabaseConfig pgSystemMetadataEbeanDatabaseConfig(
      @Qualifier("pgSystemMetadataDataSourceConfig") DataSourceConfig config) {
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("pgSystemMetadataEbeanDatabaseConfig");
    serverConfig.setDataSourceConfig(config);
    serverConfig.setDefaultServer(false);
    serverConfig.setRegister(false);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    return serverConfig;
  }

  @Bean("pgSystemMetadataEbeanServer")
  @Nonnull
  protected Database pgSystemMetadataEbeanServer(
      @Qualifier("pgSystemMetadataEbeanDatabaseConfig") DatabaseConfig serverConfig) {
    try {
      return io.ebean.DatabaseFactory.create(serverConfig);
    } catch (NullPointerException ne) {
      log.error("Failed to connect to the pgSystemMetadata server. Is Postgres up?");
      throw ne;
    }
  }
}
