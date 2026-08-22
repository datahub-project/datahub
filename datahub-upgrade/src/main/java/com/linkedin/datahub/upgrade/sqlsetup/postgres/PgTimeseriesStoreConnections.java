package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.gms.factory.common.CrossCloudIamUtils;
import com.linkedin.metadata.config.postgres.PgTimeseriesStoreOptions;
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
 * Opens JDBC connections for pgTimeseries SqlSetup per store. When the store has no pool URL, falls
 * back to the upgrade/Ebean {@link Database} connection (default-store single-DB case).
 */
public final class PgTimeseriesStoreConnections {

  private PgTimeseriesStoreConnections() {}

  @Nonnull
  public static Connection open(
      @Nonnull PgTimeseriesStoreOptions store,
      @Nonnull Database fallbackServer,
      @Nonnull PostgresSqlSetupProperties props)
      throws SQLException {
    return open(store, fallbackServer, props, null);
  }

  @Nonnull
  public static Connection open(
      @Nonnull PgTimeseriesStoreOptions store,
      @Nonnull Database fallbackServer,
      @Nonnull PostgresSqlSetupProperties props,
      @Nullable DataSourceBuilder.Settings ebeanDataSourceConfig)
      throws SQLException {
    String url = store.getPoolUrl();
    if (url == null || url.isBlank()) {
      return fallbackServer.dataSource().getConnection();
    }

    String user = blankToNull(store.getPoolUsername());
    String pass = blankToNull(store.getPoolPassword());
    if (user == null || pass == null) {
      if (ebeanDataSourceConfig != null) {
        if (user == null) {
          user = blankToNull(ebeanDataSourceConfig.getUsername());
        }
        if (pass == null) {
          pass = blankToNull(ebeanDataSourceConfig.getPassword());
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
    boolean shouldUseIam = iam != null && (iam.isUseIamAuth() || iam.isPostgresUseIamAuth());
    if (!shouldUseIam) {
      // Prefer ebean IAM toggles when pgCron IAM block is unset — match runtime pool behavior via
      // DriverManager for non-IAM deployments.
      return DriverManager.getConnection(url.trim(), user, pass);
    }

    CrossCloudIamUtils.CrossCloudConfig cfg =
        CrossCloudIamUtils.configureCrossCloudIam(
            url.trim(),
            defaultDriver,
            true,
            emptyToNull(iam.getCloudProvider()),
            emptyToNull(iam.getAwsRegion()),
            emptyToNull(iam.getAwsAccessKeyId()),
            emptyToNull(iam.getAwsSecretAccessKey()),
            emptyToNull(iam.getAwsSessionToken()),
            emptyToNull(iam.getGoogleApplicationCredentials()),
            emptyToNull(iam.getGcpProject()),
            emptyToNull(iam.getInstanceConnectionName()));

    try {
      Class.forName(cfg.driver);
    } catch (ClassNotFoundException e) {
      throw new SQLException("JDBC driver not found: " + cfg.driver, e);
    }

    Properties connProps = new Properties();
    if (!user.isEmpty()) {
      connProps.setProperty("user", user);
    }
    if (!pass.isEmpty()) {
      connProps.setProperty("password", pass);
    }
    if (cfg.customProperties != null) {
      cfg.customProperties.forEach(connProps::setProperty);
    }
    return DriverManager.getConnection(cfg.url, connProps);
  }

  @Nullable
  private static String emptyToNull(String s) {
    if (s == null || s.isBlank()) {
      return null;
    }
    return s.trim();
  }

  @Nullable
  private static String blankToNull(@Nullable String s) {
    return emptyToNull(s);
  }
}
