package com.linkedin.gms.factory.graph;

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
 * Dedicated Ebean {@link Database} for PostgreSQL graph runtime traffic. Lives on its own
 * connection pool ({@code postgres.pgGraph.pool.*}) so the graph service / write sink can:
 *
 * <ul>
 *   <li>be sized independently of the main entity-store pool (graph reads/writes have a different
 *       latency profile from aspect transactions);
 *   <li>pin default transaction isolation via {@link EbeanPoolDefaults} (same as other DataHub
 *       Ebean pools; graph DAOs are the sole consumers of this pool);
 *   <li>fail in isolation without exhausting connections that the entity DAO needs.
 * </ul>
 *
 * <p>Defaults fall through to {@code ebean.url} / {@code ebean.username} / {@code ebean.password}
 * so a single-Postgres deployment works with zero extra config; production deployments can point
 * pgGraph at a different role/database via {@code DATAHUB_PGGRAPH_DATASOURCE_*}.
 *
 * <p>DDL setup ({@code PgRoutingGraphSchemaStep} during {@code system-update}) intentionally still
 * uses the main {@code ebeanServer}; only runtime graph ops bind here.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "postgres.pgGraph.enabled", havingValue = "true")
public class PgGraphEbeanConfigFactory {

  @Value("${postgres.pgGraph.pool.username}")
  private String username;

  @Value("${postgres.pgGraph.pool.password}")
  private String password;

  @Value("${postgres.pgGraph.pool.driver}")
  private String driver;

  @Value("${postgres.pgGraph.pool.url}")
  private String url;

  @Value("${postgres.pgGraph.pool.minConnections:1}")
  private Integer minConnections;

  @Value("${postgres.pgGraph.pool.maxConnections:12}")
  private Integer maxConnections;

  @Value("${postgres.pgGraph.pool.maxInactiveTimeSeconds:120}")
  private Integer maxInactiveTimeSecs;

  @Value("${postgres.pgGraph.pool.maxAgeMinutes:120}")
  private Integer maxAgeMinutes;

  @Value("${postgres.pgGraph.pool.leakTimeMinutes:15}")
  private Integer leakTimeMinutes;

  @Value("${postgres.pgGraph.pool.waitTimeoutMillis:1000}")
  private Integer waitTimeoutMillis;

  // IAM toggles intentionally reuse the main ebean.* values: a separate pgGraph role would still
  // authenticate against the same cloud IAM provider as the main entity store.
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

  @Bean("pgGraphDataSourceConfig")
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
    dataSourceConfig.setListener(getListenerToTrackCounts(metricUtils, "pggraph"));
    EbeanPoolDefaults.applyDefaultTransactionIsolation(dataSourceConfig);

    if (crossCloudConfig.customProperties != null) {
      dataSourceConfig.setCustomProperties(crossCloudConfig.customProperties);
    }

    return dataSourceConfig;
  }

  @Bean(name = "pgGraphEbeanDatabaseConfig")
  protected DatabaseConfig pgGraphEbeanDatabaseConfig(
      @Qualifier("pgGraphDataSourceConfig") DataSourceConfig config) {
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("pgGraphEbeanDatabaseConfig");
    serverConfig.setDataSourceConfig(config);
    // Only the main gmsEbeanDatabaseConfig is allowed to register as the JVM-wide default Ebean
    // server; pgGraph is a named secondary, accessed only via its @Qualifier.
    serverConfig.setDefaultServer(false);
    serverConfig.setRegister(false);
    // Graph DAOs use raw JDBC (PostgresGraphWriteSink, PostgresGraphOneHopDao); no Ebean entity
    // models to register. DDL is applied by PgRoutingGraphSchemaStep on the main server.
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    return serverConfig;
  }

  @Bean("pgGraphEbeanServer")
  @Nonnull
  protected Database pgGraphEbeanServer(
      @Qualifier("pgGraphEbeanDatabaseConfig") DatabaseConfig serverConfig) {
    try {
      return io.ebean.DatabaseFactory.create(serverConfig);
    } catch (NullPointerException ne) {
      log.error("Failed to connect to the pgGraph server. Is Postgres up?");
      throw ne;
    }
  }
}
