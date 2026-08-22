package com.linkedin.gms.factory.timeseries;

import static com.linkedin.gms.factory.common.LocalEbeanConfigFactory.getListenerToTrackCounts;

import com.linkedin.gms.factory.common.CrossCloudIamUtils;
import com.linkedin.gms.factory.common.EbeanPoolDefaults;
import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.JdbcUrlParser;
import com.linkedin.metadata.config.postgres.PgTimeseriesSetupOptions;
import com.linkedin.metadata.config.postgres.PgTimeseriesStoreOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.timeseries.postgres.PgTimeseriesStoreRegistry;
import com.linkedin.metadata.timeseries.postgres.PgTimeseriesStoreRegistry.StoreHandle;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectDao;
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

/**
 * Builds a {@link PgTimeseriesStoreRegistry} with one Ebean {@link Database} per configured store
 * (deduped by JDBC URL so identical URLs share a pool). Defaults fall through to {@code ebean.*}.
 *
 * <p>SqlSetup DDL ({@code PgTimeseriesSchemaStep}) opens connections from each store's pool URL;
 * runtime reads/writes bind here.
 */
@Slf4j
@Configuration
@Conditional(PgTimeseriesRuntimePoolEnabledCondition.class)
public class PgTimeseriesEbeanConfigFactory {

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
  public PgTimeseriesStoreRegistry pgTimeseriesStoreRegistry(
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      MetricUtils metricUtils,
      Environment environment) {
    PgTimeseriesConfigOverlay.warnIfConfigFileMissingResourcePrefix(environment);
    postgresSqlSetupProperties.validateForUse(DatabaseType.POSTGRES);
    PgTimeseriesSetupOptions options = postgresSqlSetupProperties.buildPgTimeseriesOptions();
    if (options == null) {
      throw new IllegalStateException(
          "postgres.pgTimeseries.enabled but buildPgTimeseriesOptions() returned null");
    }

    Map<String, Database> databasesByUrl = new LinkedHashMap<>();
    Map<String, PgTimeseriesStoreOptions> storeByUrl = new LinkedHashMap<>();
    Map<String, StoreHandle> handles = new LinkedHashMap<>();
    for (PgTimeseriesStoreOptions store : options.getStores().values()) {
      String url = resolvePoolUrl(store);
      if (url == null || url.isBlank()) {
        throw new IllegalStateException(
            "pgTimeseries store '"
                + store.getName()
                + "' has an empty pool URL (set postgres.pgTimeseries.pool.url or the store's"
                + " pool.url, or ebean.url)");
      }
      String urlKey = url.trim();
      JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(urlKey);
      if (info.databaseType != DatabaseType.POSTGRES) {
        throw new IllegalStateException(
            "pgTimeseries store '" + store.getName() + "' pool URL is not PostgreSQL: " + url);
      }
      PgTimeseriesStoreOptions existing = storeByUrl.putIfAbsent(urlKey, store);
      if (existing != null && !poolIdentityEquals(existing, store)) {
        throw new IllegalStateException(
            "pgTimeseries stores '"
                + existing.getName()
                + "' and '"
                + store.getName()
                + "' share JDBC URL "
                + urlKey
                + " but differ in credentials or pool settings; use distinct URLs or identical"
                + " pool configuration");
      }
      Database database =
          databasesByUrl.computeIfAbsent(urlKey, u -> createDatabase(store, u, metricUtils));
      handles.put(
          store.getName(),
          new StoreHandle(store, database, new PostgresTimeseriesAspectDao(database, store)));
    }
    return new PgTimeseriesStoreRegistry(options, handles);
  }

  /** Same URL may be reused only when effective credentials and pool sizing match. */
  boolean poolIdentityEquals(
      @Nonnull PgTimeseriesStoreOptions a, @Nonnull PgTimeseriesStoreOptions b) {
    return java.util.Objects.equals(effectiveDriver(a), effectiveDriver(b))
        && java.util.Objects.equals(effectiveUsername(a), effectiveUsername(b))
        && java.util.Objects.equals(effectivePassword(a), effectivePassword(b))
        && a.getPoolMinConnections() == b.getPoolMinConnections()
        && a.getPoolMaxConnections() == b.getPoolMaxConnections()
        && a.getPoolMaxInactiveTimeSeconds() == b.getPoolMaxInactiveTimeSeconds()
        && a.getPoolMaxAgeMinutes() == b.getPoolMaxAgeMinutes()
        && a.getPoolLeakTimeMinutes() == b.getPoolLeakTimeMinutes()
        && a.getPoolWaitTimeoutMillis() == b.getPoolWaitTimeoutMillis();
  }

  @Nonnull
  private String effectiveDriver(@Nonnull PgTimeseriesStoreOptions store) {
    return store.getPoolDriver() != null && !store.getPoolDriver().isBlank()
        ? store.getPoolDriver()
        : ebeanDriver;
  }

  @Nonnull
  private String effectiveUsername(@Nonnull PgTimeseriesStoreOptions store) {
    return store.getPoolUsername() != null && !store.getPoolUsername().isBlank()
        ? store.getPoolUsername()
        : ebeanUsername;
  }

  @Nonnull
  private String effectivePassword(@Nonnull PgTimeseriesStoreOptions store) {
    return store.getPoolPassword() != null && !store.getPoolPassword().isBlank()
        ? store.getPoolPassword()
        : ebeanPassword;
  }

  /**
   * Back-compat bean for callers that still inject the default-store Database. Prefer {@link
   * #pgTimeseriesStoreRegistry}.
   */
  @Bean("pgTimeseriesEbeanServer")
  @Nonnull
  protected Database pgTimeseriesEbeanServer(
      @Nonnull PgTimeseriesStoreRegistry pgTimeseriesStoreRegistry) {
    return pgTimeseriesStoreRegistry.getDefault().getDatabase();
  }

  @Nonnull
  private Database createDatabase(
      PgTimeseriesStoreOptions store, String url, MetricUtils metricUtils) {
    DataSourceConfig dataSourceConfig = buildDataSourceConfig(store, url, metricUtils);
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("pgTimeseries-" + store.getName());
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDefaultServer(false);
    serverConfig.setRegister(false);
    serverConfig.setDdlGenerate(false);
    serverConfig.setDdlRun(false);
    try {
      return io.ebean.DatabaseFactory.create(serverConfig);
    } catch (NullPointerException ne) {
      log.error(
          "Failed to connect to the pgTimeseries pool for store '{}'. Is Postgres up?",
          store.getName());
      throw ne;
    }
  }

  @Nonnull
  DataSourceConfig buildDataSourceConfig(
      PgTimeseriesStoreOptions store, String dataSourceUrl, MetricUtils metricUtils) {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();

    boolean shouldUseIam =
        Boolean.TRUE.equals(useIamAuth) || Boolean.TRUE.equals(postgresUseIamAuth);
    String driver =
        store.getPoolDriver() != null && !store.getPoolDriver().isBlank()
            ? store.getPoolDriver()
            : ebeanDriver;
    String username =
        store.getPoolUsername() != null && !store.getPoolUsername().isBlank()
            ? store.getPoolUsername()
            : ebeanUsername;
    String password =
        store.getPoolPassword() != null && !store.getPoolPassword().isBlank()
            ? store.getPoolPassword()
            : ebeanPassword;

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
        getListenerToTrackCounts(metricUtils, "pgtimeseries-" + store.getName()));
    EbeanPoolDefaults.applyDefaultTransactionIsolation(dataSourceConfig);

    if (crossCloudConfig.customProperties != null) {
      dataSourceConfig.setCustomProperties(crossCloudConfig.customProperties);
    }

    return dataSourceConfig;
  }

  @Nullable
  private String resolvePoolUrl(PgTimeseriesStoreOptions store) {
    if (store.getPoolUrl() != null && !store.getPoolUrl().isBlank()) {
      return store.getPoolUrl().trim();
    }
    if (ebeanUrl != null && !ebeanUrl.isBlank()) {
      return ebeanUrl.trim();
    }
    return null;
  }
}
