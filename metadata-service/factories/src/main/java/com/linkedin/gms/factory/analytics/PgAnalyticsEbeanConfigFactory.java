package com.linkedin.gms.factory.analytics;

import static com.linkedin.gms.factory.common.LocalEbeanConfigFactory.getListenerToTrackCounts;

import com.linkedin.gms.factory.common.CrossCloudIamUtils;
import com.linkedin.gms.factory.common.EbeanPoolDefaults;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry.StoreHandle;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.JdbcUrlParser;
import com.linkedin.metadata.config.postgres.PgAnalyticsSetupOptions;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.Database;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Slf4j
@Configuration
@Conditional(PgAnalyticsRuntimePoolEnabledCondition.class)
public class PgAnalyticsEbeanConfigFactory {

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

  @Value("${ebean.url:}")
  private String ebeanUrl;

  @Value("${ebean.driver:org.postgresql.Driver}")
  private String ebeanDriver;

  @Value("${ebean.username:}")
  private String ebeanUsername;

  @Value("${ebean.password:}")
  private String ebeanPassword;

  @Bean
  @Nonnull
  public PgAnalyticsStoreRegistry pgAnalyticsStoreRegistry(
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      MetricUtils metricUtils,
      Environment environment) {
    PgAnalyticsConfigOverlay.warnIfConfigFileMissingResourcePrefix(environment);
    postgresSqlSetupProperties.validateForUse(DatabaseType.POSTGRES);
    PgAnalyticsSetupOptions options = postgresSqlSetupProperties.buildPgAnalyticsOptions();
    if (options == null) {
      throw new IllegalStateException(
          "postgres.pgAnalytics.enabled but buildPgAnalyticsOptions() returned null");
    }

    Map<String, Database> databasesByUrl = new LinkedHashMap<>();
    Map<String, StoreHandle> handles = new LinkedHashMap<>();
    for (PgAnalyticsStoreOptions store : options.getStores().values()) {
      String url = resolvePoolUrl(store);
      if (url == null || url.isBlank()) {
        throw new IllegalStateException(
            "pgAnalytics store '"
                + store.getName()
                + "' has an empty pool URL (set postgres.pgAnalytics.pool.url or ebean.url)");
      }
      JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url.trim());
      if (info.databaseType != DatabaseType.POSTGRES) {
        throw new IllegalStateException(
            "pgAnalytics store '" + store.getName() + "' pool URL is not PostgreSQL: " + url);
      }
      Database database =
          databasesByUrl.computeIfAbsent(url.trim(), u -> createDatabase(store, u, metricUtils));
      handles.put(
          store.getName(),
          new StoreHandle(store, database, new PostgresAnalyticsStore(database, store)));
    }
    return new PgAnalyticsStoreRegistry(options, handles);
  }

  @Bean("pgAnalyticsEbeanServer")
  @Nonnull
  protected Database pgAnalyticsEbeanServer(@Nonnull PgAnalyticsStoreRegistry registry) {
    return registry.getDefault().getDatabase();
  }

  @Nonnull
  private Database createDatabase(
      PgAnalyticsStoreOptions store, String url, MetricUtils metricUtils) {
    DataSourceConfig dataSourceConfig = buildDataSourceConfig(store, url, metricUtils);
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("pgAnalytics-" + store.getName());
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDefaultServer(false);
    serverConfig.setRegister(false);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    try {
      return io.ebean.DatabaseFactory.create(serverConfig);
    } catch (NullPointerException ne) {
      log.error(
          "Failed to connect to the pgAnalytics pool for store '{}'. Is Postgres up?",
          store.getName());
      throw ne;
    }
  }

  @Nonnull
  DataSourceConfig buildDataSourceConfig(
      PgAnalyticsStoreOptions store, String dataSourceUrl, MetricUtils metricUtils) {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();

    boolean shouldUseIam =
        Boolean.TRUE.equals(useIamAuth) || Boolean.TRUE.equals(postgresUseIamAuth);
    String driver =
        store.getPoolDriver() != null && !store.getPoolDriver().isBlank()
            ? store.getPoolDriver()
            : ebeanDriver;
    String username = store.getPoolUsername() != null ? store.getPoolUsername() : ebeanUsername;
    String password = store.getPoolPassword() != null ? store.getPoolPassword() : ebeanPassword;

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
    dataSourceConfig.setMinConnections(store.getPoolMinConnections());
    dataSourceConfig.setMaxConnections(store.getPoolMaxConnections());
    dataSourceConfig.setMaxInactiveTimeSecs(store.getPoolMaxInactiveTimeSeconds());
    dataSourceConfig.setMaxAgeMinutes(store.getPoolMaxAgeMinutes());
    dataSourceConfig.setLeakTimeMinutes(store.getPoolLeakTimeMinutes());
    dataSourceConfig.setWaitTimeoutMillis(store.getPoolWaitTimeoutMillis());
    dataSourceConfig.setListener(
        getListenerToTrackCounts(metricUtils, "pganalytics-" + store.getName()));
    EbeanPoolDefaults.applyDefaultTransactionIsolation(dataSourceConfig);

    if (crossCloudConfig.customProperties != null) {
      dataSourceConfig.setCustomProperties(crossCloudConfig.customProperties);
    }

    return dataSourceConfig;
  }

  @Nullable
  private String resolvePoolUrl(PgAnalyticsStoreOptions store) {
    if (store.getPoolUrl() != null && !store.getPoolUrl().isBlank()) {
      return store.getPoolUrl().trim();
    }
    if (ebeanUrl != null && !ebeanUrl.isBlank()) {
      return ebeanUrl.trim();
    }
    return null;
  }
}
