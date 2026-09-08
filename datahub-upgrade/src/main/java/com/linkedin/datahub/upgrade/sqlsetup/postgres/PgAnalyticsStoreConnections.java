package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.gms.factory.common.CrossCloudIamUtils;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties.PgCron.Iam;
import io.ebean.Database;
import io.ebean.datasource.DataSourceBuilder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Opens JDBC connections for pgAnalytics SqlSetup per store. When the store has no pool URL, falls
 * back to the upgrade/Ebean {@link Database} connection. When only the URL is overridden,
 * blank/null username or password fall back to the Ebean pool credentials (same as runtime {@code
 * PgAnalyticsEbeanConfigFactory}).
 */
public final class PgAnalyticsStoreConnections {

  private PgAnalyticsStoreConnections() {}

  @Nonnull
  public static Connection open(
      @Nonnull PgAnalyticsStoreOptions store,
      @Nonnull Database fallbackServer,
      @Nonnull PostgresSqlSetupProperties props)
      throws SQLException {
    return open(store, fallbackServer, props, null);
  }

  @Nonnull
  public static Connection open(
      @Nonnull PgAnalyticsStoreOptions store,
      @Nonnull Database fallbackServer,
      @Nonnull PostgresSqlSetupProperties props,
      @Nullable DataSourceBuilder.Settings ebeanDataSourceConfig)
      throws SQLException {
    String url = store.getPoolUrl();
    if (url == null || url.isBlank()) {
      return fallbackServer.dataSource().getConnection();
    }

    String user = PgTimeseriesStoreConnections.blankToNull(store.getPoolUsername());
    String pass = PgTimeseriesStoreConnections.blankToNull(store.getPoolPassword());
    if (user == null || pass == null) {
      if (ebeanDataSourceConfig != null) {
        if (user == null) {
          user = PgTimeseriesStoreConnections.blankToNull(ebeanDataSourceConfig.getUsername());
        }
        if (pass == null) {
          pass = PgTimeseriesStoreConnections.blankToNull(ebeanDataSourceConfig.getPassword());
        }
      }
      if (user == null || pass == null) {
        String[] ebeanCreds = ebeanCredentials(fallbackServer);
        if (user == null) {
          user = PgTimeseriesStoreConnections.blankToNull(ebeanCreds[0]);
        }
        if (pass == null) {
          pass = PgTimeseriesStoreConnections.blankToNull(ebeanCreds[1]);
        }
      }
    }
    if (user == null) {
      user = "";
    }
    if (pass == null) {
      pass = "";
    }

    String defaultDriver =
        store.getPoolDriver() != null && !store.getPoolDriver().isBlank()
            ? store.getPoolDriver().trim()
            : "org.postgresql.Driver";

    Iam iam = props.getPgCron() != null ? props.getPgCron().getIam() : null;
    boolean shouldUseIam = PgTimeseriesStoreConnections.shouldUseIam(iam, ebeanDataSourceConfig);
    if (!shouldUseIam) {
      return DriverManager.getConnection(url.trim(), user, pass);
    }

    String jdbcUrl = url.trim();
    boolean sharesEbeanPool =
        PgTimeseriesStoreConnections.sharesEbeanPoolUrl(jdbcUrl, ebeanDataSourceConfig);
    String cloudProvider =
        PgTimeseriesStoreConnections.firstNonBlank(
            iam == null ? null : PgTimeseriesStoreConnections.emptyToNull(iam.getCloudProvider()),
            PgTimeseriesStoreConnections.inferCloudProvider(null, jdbcUrl),
            sharesEbeanPool
                ? PgTimeseriesStoreConnections.inferCloudProvider(ebeanDataSourceConfig, jdbcUrl)
                : null,
            "auto");

    CrossCloudIamUtils.CrossCloudConfig cfg =
        CrossCloudIamUtils.configureCrossCloudIam(
            jdbcUrl,
            defaultDriver,
            true,
            cloudProvider,
            iam == null ? null : PgTimeseriesStoreConnections.emptyToNull(iam.getAwsRegion()),
            iam == null ? null : PgTimeseriesStoreConnections.emptyToNull(iam.getAwsAccessKeyId()),
            iam == null
                ? null
                : PgTimeseriesStoreConnections.emptyToNull(iam.getAwsSecretAccessKey()),
            iam == null ? null : PgTimeseriesStoreConnections.emptyToNull(iam.getAwsSessionToken()),
            iam == null
                ? null
                : PgTimeseriesStoreConnections.emptyToNull(iam.getGoogleApplicationCredentials()),
            iam == null ? null : PgTimeseriesStoreConnections.emptyToNull(iam.getGcpProject()),
            iam == null
                ? null
                : PgTimeseriesStoreConnections.emptyToNull(iam.getInstanceConnectionName()));

    String driver = cfg.driver;
    if (sharesEbeanPool
        && ebeanDataSourceConfig != null
        && ebeanDataSourceConfig.getDriver() != null
        && ebeanDataSourceConfig.getDriver().contains("cloud.sql")
        && (driver == null || !driver.contains("cloud.sql"))) {
      driver = ebeanDataSourceConfig.getDriver();
    }

    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      throw new SQLException("JDBC driver not found: " + driver, e);
    }

    Properties connProps = new Properties();
    if (!user.isEmpty()) {
      connProps.setProperty("user", user);
    }
    if (!pass.isEmpty()) {
      connProps.setProperty("password", pass);
    }
    if (sharesEbeanPool) {
      PgTimeseriesStoreConnections.mergeNonBlank(
          connProps, PgTimeseriesStoreConnections.ebeanCustomProperties(ebeanDataSourceConfig));
    }
    PgTimeseriesStoreConnections.mergeNonBlank(connProps, cfg.customProperties);
    return DriverManager.getConnection(cfg.url, connProps);
  }

  @Nonnull
  private static String[] ebeanCredentials(@Nonnull Database fallbackServer) {
    try {
      DataSourceBuilder.Settings dsc = fallbackServer.pluginApi().config().getDataSourceConfig();
      if (dsc == null) {
        return new String[] {"", ""};
      }
      String user = dsc.getUsername() != null ? dsc.getUsername() : "";
      String pass = dsc.getPassword() != null ? dsc.getPassword() : "";
      return new String[] {user, pass};
    } catch (RuntimeException e) {
      return new String[] {"", ""};
    }
  }
}
