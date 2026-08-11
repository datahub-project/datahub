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
    String url = store.getPoolUrl();
    if (url == null || url.isBlank()) {
      return fallbackServer.dataSource().getConnection();
    }

    String user = store.getPoolUsername();
    String pass = store.getPoolPassword();
    if (isBlank(user) || isBlank(pass)) {
      String[] ebeanCreds = ebeanCredentials(fallbackServer);
      if (isBlank(user)) {
        user = ebeanCreds[0];
      }
      if (isBlank(pass)) {
        pass = ebeanCreds[1];
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

  private static boolean isBlank(@Nullable String s) {
    return s == null || s.isBlank();
  }

  @Nullable
  private static String emptyToNull(String s) {
    if (s == null || s.isBlank()) {
      return null;
    }
    return s.trim();
  }
}
