package com.linkedin.gms.factory.event;

import static com.linkedin.gms.factory.common.LocalEbeanConfigFactory.getListenerToTrackCounts;

import com.linkedin.gms.factory.common.CrossCloudIamUtils;
import com.linkedin.gms.factory.common.EbeanPoolDefaults;
import com.linkedin.metadata.config.messaging.KafkaMessagingDisabled;
import com.linkedin.metadata.queue.ebean.EbeanPgQueueTopic;
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
 * Dedicated Ebean {@link Database} for the PostgreSQL queue store. Lives on its own connection pool
 * ({@code postgres.pgQueue.pool.*}) so the queue store can:
 *
 * <ul>
 *   <li>be sized independently of the main entity-store pool (queue traffic is bursty);
 *   <li>pin default transaction isolation via {@link EbeanPoolDefaults} (same as other DataHub
 *       Ebean pools; the queue store is the sole consumer);
 *   <li>fail in isolation without exhausting connections that the entity DAO needs.
 * </ul>
 *
 * <p>Defaults fall through to {@code ebean.url} / {@code ebean.username} / {@code ebean.password}
 * so a single-Postgres deployment works with zero extra config; production deployments can point
 * pgQueue at a different role/database via {@code DATAHUB_PGQUEUE_DATASOURCE_*}.
 *
 * <p>DDL setup ({@code PgQueueSchemaStep} during {@code system-update}) intentionally still uses
 * the main {@code ebeanServer}; only runtime queue ops bind here.
 */
@Slf4j
@Configuration
@KafkaMessagingDisabled
@ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
public class PgQueueEbeanConfigFactory {

  @Value("${postgres.pgQueue.pool.username}")
  private String username;

  @Value("${postgres.pgQueue.pool.password}")
  private String password;

  @Value("${postgres.pgQueue.pool.driver}")
  private String driver;

  @Value("${postgres.pgQueue.pool.url}")
  private String url;

  @Value("${postgres.pgQueue.pool.minConnections:1}")
  private Integer minConnections;

  @Value("${postgres.pgQueue.pool.maxConnections:8}")
  private Integer maxConnections;

  @Value("${postgres.pgQueue.pool.maxInactiveTimeSeconds:120}")
  private Integer maxInactiveTimeSecs;

  @Value("${postgres.pgQueue.pool.maxAgeMinutes:120}")
  private Integer maxAgeMinutes;

  @Value("${postgres.pgQueue.pool.leakTimeMinutes:15}")
  private Integer leakTimeMinutes;

  @Value("${postgres.pgQueue.pool.waitTimeoutMillis:1000}")
  private Integer waitTimeoutMillis;

  // IAM toggles intentionally reuse the main ebean.* values: a separate pgQueue role would still
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

  @Bean("pgQueueDataSourceConfig")
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
    dataSourceConfig.setListener(getListenerToTrackCounts(metricUtils, "pgqueue"));
    EbeanPoolDefaults.applyDefaultTransactionIsolation(dataSourceConfig);

    if (crossCloudConfig.customProperties != null) {
      dataSourceConfig.setCustomProperties(crossCloudConfig.customProperties);
    }

    return dataSourceConfig;
  }

  @Bean(name = "pgQueueEbeanDatabaseConfig")
  protected DatabaseConfig pgQueueEbeanDatabaseConfig(
      @Qualifier("pgQueueDataSourceConfig") DataSourceConfig config) {
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("pgQueueEbeanDatabaseConfig");
    serverConfig.setDataSourceConfig(config);
    // Only the main gmsEbeanDatabaseConfig is allowed to register as the JVM-wide default Ebean
    // server; pgQueue is a named secondary, accessed only via its @Qualifier.
    serverConfig.setDefaultServer(false);
    serverConfig.setRegister(false);
    // Queue Ebean entities are read-only models; DDL is applied by PgQueueSchemaStep on the main
    // server, so we do not enable autoCreate here.
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    return serverConfig;
  }

  @Bean("pgQueueEbeanServer")
  @Nonnull
  protected Database pgQueueEbeanServer(
      @Qualifier("pgQueueEbeanDatabaseConfig") DatabaseConfig serverConfig) {
    String queueModelPackage = EbeanPgQueueTopic.class.getPackage().getName();
    if (!serverConfig.getPackages().contains(queueModelPackage)) {
      serverConfig.getPackages().add(queueModelPackage);
    }
    try {
      return io.ebean.DatabaseFactory.create(serverConfig);
    } catch (NullPointerException ne) {
      log.error("Failed to connect to the pgQueue server. Is Postgres up?");
      throw ne;
    }
  }
}
