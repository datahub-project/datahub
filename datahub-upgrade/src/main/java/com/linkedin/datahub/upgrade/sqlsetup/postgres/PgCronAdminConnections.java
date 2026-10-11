package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.gms.factory.common.CrossCloudIamUtils;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties.PgCron.Admin;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties.PgCron.Iam;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Opens JDBC connections to the PostgreSQL database that hosts pg_cron metadata ({@code
 * cron.database_name}), typically the {@code postgres} database while application DDL uses {@code
 * ebean.url}.
 */
public final class PgCronAdminConnections {

  private PgCronAdminConnections() {}

  /** Connects using {@link Admin} JDBC settings from Spring config. */
  public static Connection open(PostgresSqlSetupProperties props) throws SQLException {
    Admin admin = props.getPgCron() != null ? props.getPgCron().getAdmin() : null;
    String rawUrl = admin != null ? admin.getJdbcUrl() : null;
    if (rawUrl == null || rawUrl.isBlank()) {
      throw new IllegalStateException(
          "postgres.pgCron.admin.jdbcUrl must be configured when pg_cron SqlSetup runs.");
    }
    String url = rawUrl.trim();
    String user = admin.getUsername();
    String pass = admin.getPassword();

    Iam iam = props.getPgCron() != null ? props.getPgCron().getIam() : null;
    boolean shouldUseIam = iam != null && (iam.isUseIamAuth() || iam.isPostgresUseIamAuth());

    String defaultDriver =
        admin.getDriver() != null && !admin.getDriver().isBlank()
            ? admin.getDriver().trim()
            : "org.postgresql.Driver";

    if (!shouldUseIam) {
      return DriverManager.getConnection(url, user != null ? user : "", pass != null ? pass : "");
    }

    CrossCloudIamUtils.CrossCloudConfig cfg =
        CrossCloudIamUtils.configureCrossCloudIam(
            url,
            defaultDriver,
            true,
            emptyToNull(iam != null ? iam.getCloudProvider() : null),
            emptyToNull(iam != null ? iam.getAwsRegion() : null),
            emptyToNull(iam != null ? iam.getAwsAccessKeyId() : null),
            emptyToNull(iam != null ? iam.getAwsSecretAccessKey() : null),
            emptyToNull(iam != null ? iam.getAwsSessionToken() : null),
            emptyToNull(iam != null ? iam.getGoogleApplicationCredentials() : null),
            emptyToNull(iam != null ? iam.getGcpProject() : null),
            emptyToNull(iam != null ? iam.getInstanceConnectionName() : null));

    try {
      Class.forName(cfg.driver);
    } catch (ClassNotFoundException e) {
      throw new SQLException("JDBC driver not found: " + cfg.driver, e);
    }

    Properties connProps = new Properties();
    if (user != null && !user.isEmpty()) {
      connProps.setProperty("user", user);
    }
    if (pass != null && !pass.isEmpty()) {
      connProps.setProperty("password", pass);
    }
    if (cfg.customProperties != null) {
      cfg.customProperties.forEach(connProps::setProperty);
    }

    return DriverManager.getConnection(cfg.url, connProps);
  }

  private static String emptyToNull(String s) {
    if (s == null || s.isBlank()) {
      return null;
    }
    return s;
  }
}
