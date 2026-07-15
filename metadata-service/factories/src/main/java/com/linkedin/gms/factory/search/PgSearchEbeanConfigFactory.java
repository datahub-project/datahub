package com.linkedin.gms.factory.search;

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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Dedicated Ebean {@link Database} for PostgreSQL entity search runtime traffic. Lives on its own
 * connection pool ({@code postgres.pgSearch.entity.pool.*}) so search reads/writes can be sized
 * independently of the main entity-store pool.
 *
 * <p>DDL setup ({@code PgSearchEntitySchemaStep} during {@code system-update}) intentionally still
 * uses the main {@code ebeanServer}; only runtime search ops bind here.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "postgres.pgSearch.entity.enabled", havingValue = "true")
public class PgSearchEbeanConfigFactory {

  @Value("${postgres.pgSearch.entity.pool.username}")
  private String username;

  @Value("${postgres.pgSearch.entity.pool.password}")
  private String password;

  @Value("${postgres.pgSearch.entity.pool.driver}")
  private String driver;

  @Value("${postgres.pgSearch.entity.pool.url}")
  private String url;

  @Value("${postgres.pgSearch.entity.pool.minConnections:1}")
  private Integer minConnections;

  @Value("${postgres.pgSearch.entity.pool.maxConnections:12}")
  private Integer maxConnections;

  @Value("${postgres.pgSearch.entity.pool.maxInactiveTimeSeconds:120}")
  private Integer maxInactiveTimeSeconds;

  @Value("${postgres.pgSearch.entity.pool.maxAgeMinutes:120}")
  private Integer maxAgeMinutes;

  @Value("${postgres.pgSearch.entity.pool.leakTimeMinutes:15}")
  private Integer leakTimeMinutes;

  @Value("${postgres.pgSearch.entity.pool.waitTimeoutMillis:1000}")
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

  @Bean("pgSearchDataSourceConfig")
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
    dataSourceConfig.setMaxInactiveTimeSecs(maxInactiveTimeSeconds);
    dataSourceConfig.setMaxAgeMinutes(maxAgeMinutes);
    dataSourceConfig.setLeakTimeMinutes(leakTimeMinutes);
    dataSourceConfig.setWaitTimeoutMillis(waitTimeoutMillis);
    dataSourceConfig.setListener(getListenerToTrackCounts(metricUtils, "pgsearch"));
    EbeanPoolDefaults.applyDefaultTransactionIsolation(dataSourceConfig);

    if (crossCloudConfig.customProperties != null) {
      dataSourceConfig.setCustomProperties(crossCloudConfig.customProperties);
    }

    return dataSourceConfig;
  }

  @Bean(name = "pgSearchEbeanDatabaseConfig")
  protected DatabaseConfig pgSearchEbeanDatabaseConfig(
      @Qualifier("pgSearchDataSourceConfig") DataSourceConfig config) {
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("pgSearchEbeanDatabaseConfig");
    serverConfig.setDataSourceConfig(config);
    serverConfig.setDefaultServer(false);
    serverConfig.setRegister(false);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    return serverConfig;
  }

  @Bean("pgSearchEbeanServer")
  @Nonnull
  protected Database pgSearchEbeanServer(
      @Qualifier("pgSearchEbeanDatabaseConfig") DatabaseConfig serverConfig) {
    try {
      return io.ebean.DatabaseFactory.create(serverConfig);
    } catch (NullPointerException ne) {
      log.error("Failed to connect to the pgSearch entity server. Is Postgres up?");
      throw ne;
    }
  }
}
