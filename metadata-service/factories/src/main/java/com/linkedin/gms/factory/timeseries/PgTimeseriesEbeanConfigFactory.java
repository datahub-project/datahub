package com.linkedin.gms.factory.timeseries;

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
 * Dedicated Ebean {@link Database} for PostgreSQL timeseries-style runtime traffic: SqlSetup {@code
 * pgTimeseries} aspect rows and {@link com.linkedin.metadata.datahubusage.postgres
 * .PostgresUsageEventsStore platform analytics usage events} when {@code implementation} is {@code
 * postgres}. Uses {@code postgres.pgTimeseries.pool.*} (same pattern as {@code postgres.pgGraph} /
 * {@code postgres.pgQueue}) so these workloads share one pool and sizing, separate from the main
 * entity-store {@code ebeanServer}.
 *
 * <p>Defaults fall through to {@code ebean.url} / {@code ebean.username} / {@code ebean.password}.
 * SqlSetup DDL ({@code PgTimeseriesSchemaStep}, usage-events parent table ensure) still runs on the
 * main upgrade/Ebean datasource where applicable; runtime reads/writes bind here.
 */
@Slf4j
@Configuration
@Conditional(PgTimeseriesRuntimePoolEnabledCondition.class)
public class PgTimeseriesEbeanConfigFactory {

  @Value("${postgres.pgTimeseries.pool.username}")
  private String username;

  @Value("${postgres.pgTimeseries.pool.password}")
  private String password;

  @Value("${postgres.pgTimeseries.pool.driver}")
  private String driver;

  @Value("${postgres.pgTimeseries.pool.url}")
  private String url;

  @Value("${postgres.pgTimeseries.pool.minConnections:1}")
  private Integer minConnections;

  @Value("${postgres.pgTimeseries.pool.maxConnections:12}")
  private Integer maxConnections;

  @Value("${postgres.pgTimeseries.pool.maxInactiveTimeSeconds:120}")
  private Integer maxInactiveTimeSecs;

  @Value("${postgres.pgTimeseries.pool.maxAgeMinutes:120}")
  private Integer maxAgeMinutes;

  @Value("${postgres.pgTimeseries.pool.leakTimeMinutes:15}")
  private Integer leakTimeMinutes;

  @Value("${postgres.pgTimeseries.pool.waitTimeoutMillis:1000}")
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

  @Bean("pgTimeseriesDataSourceConfig")
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
    dataSourceConfig.setListener(getListenerToTrackCounts(metricUtils, "pgtimeseries"));
    EbeanPoolDefaults.applyDefaultTransactionIsolation(dataSourceConfig);

    if (crossCloudConfig.customProperties != null) {
      dataSourceConfig.setCustomProperties(crossCloudConfig.customProperties);
    }

    return dataSourceConfig;
  }

  @Bean(name = "pgTimeseriesEbeanDatabaseConfig")
  protected DatabaseConfig pgTimeseriesEbeanDatabaseConfig(
      @Qualifier("pgTimeseriesDataSourceConfig") DataSourceConfig config) {
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("pgTimeseriesEbeanDatabaseConfig");
    serverConfig.setDataSourceConfig(config);
    serverConfig.setDefaultServer(false);
    serverConfig.setRegister(false);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    return serverConfig;
  }

  @Bean("pgTimeseriesEbeanServer")
  @Nonnull
  protected Database pgTimeseriesEbeanServer(
      @Qualifier("pgTimeseriesEbeanDatabaseConfig") DatabaseConfig serverConfig) {
    try {
      return io.ebean.DatabaseFactory.create(serverConfig);
    } catch (NullPointerException ne) {
      log.error("Failed to connect to the pgTimeseries pool. Is Postgres up?");
      throw ne;
    }
  }
}
