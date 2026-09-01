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
import java.util.Arrays;
import java.util.Map;
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
    boolean shouldUseIam = shouldUseIam(iam, ebeanDataSourceConfig);
    if (!shouldUseIam) {
      return DriverManager.getConnection(url.trim(), user, pass);
    }

    String jdbcUrl = url.trim();
    boolean sharesEbeanPool = sharesEbeanPoolUrl(jdbcUrl, ebeanDataSourceConfig);
    String cloudProvider =
        firstNonBlank(
            iam == null ? null : emptyToNull(iam.getCloudProvider()),
            inferCloudProvider(null, jdbcUrl),
            sharesEbeanPool ? inferCloudProvider(ebeanDataSourceConfig, jdbcUrl) : null,
            "auto");

    CrossCloudIamUtils.CrossCloudConfig cfg =
        CrossCloudIamUtils.configureCrossCloudIam(
            jdbcUrl,
            defaultDriver,
            true,
            cloudProvider,
            iam == null ? null : emptyToNull(iam.getAwsRegion()),
            iam == null ? null : emptyToNull(iam.getAwsAccessKeyId()),
            iam == null ? null : emptyToNull(iam.getAwsSecretAccessKey()),
            iam == null ? null : emptyToNull(iam.getAwsSessionToken()),
            iam == null ? null : emptyToNull(iam.getGoogleApplicationCredentials()),
            iam == null ? null : emptyToNull(iam.getGcpProject()),
            iam == null ? null : emptyToNull(iam.getInstanceConnectionName()));

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
    // Copy IAM properties from the GMS ebean pool only when this store uses the same JDBC URL.
    if (sharesEbeanPool) {
      mergeNonBlank(connProps, ebeanCustomProperties(ebeanDataSourceConfig));
    }
    mergeNonBlank(connProps, cfg.customProperties);
    return DriverManager.getConnection(cfg.url, connProps);
  }

  static boolean sharesEbeanPoolUrl(
      @Nonnull String storeUrl, @Nullable DataSourceBuilder.Settings ebeanDataSourceConfig) {
    if (ebeanDataSourceConfig == null) {
      return false;
    }
    String ebeanUrl = ebeanDataSourceConfig.getUrl();
    if (ebeanUrl == null || ebeanUrl.isBlank()) {
      return false;
    }
    return storeUrl.trim().equals(ebeanUrl.trim());
  }

  /**
   * Match runtime pools: IAM is on when pgCron IAM flags are set, or the GMS ebean datasource is
   * already IAM-configured ({@code ebean.useIamAuth} / {@code ebean.postgresUseIamAuth}).
   */
  static boolean shouldUseIam(
      @Nullable Iam iam, @Nullable DataSourceBuilder.Settings ebeanDataSourceConfig) {
    if (iam != null && (iam.isUseIamAuth() || iam.isPostgresUseIamAuth())) {
      return true;
    }
    return ebeanPoolUsesIam(ebeanDataSourceConfig);
  }

  /**
   * When pgCron IAM does not name a cloud, infer aws/gcp from the already-configured ebean pool so
   * {@link CrossCloudIamUtils#configureCrossCloudIam} still emits {@code wrapperPlugins=iam} /
   * Cloud SQL socket factory instead of returning an empty config for {@code cloudProvider=null}.
   */
  @Nullable
  static String inferCloudProvider(
      @Nullable DataSourceBuilder.Settings cfg, @Nullable String jdbcUrl) {
    String fromEbean = inferCloudProviderFromEbeanSettings(cfg);
    if (fromEbean != null) {
      return fromEbean;
    }
    if (jdbcUrl != null) {
      if (jdbcUrl.contains("rds.amazonaws.com") || jdbcUrl.contains("amazonaws.com")) {
        return "aws";
      }
      if (jdbcUrl.contains("googleapis.com") || jdbcUrl.contains("cloudsql")) {
        return "gcp";
      }
    }
    return null;
  }

  @Nullable
  private static String inferCloudProviderFromEbeanSettings(
      @Nullable DataSourceBuilder.Settings cfg) {
    if (cfg == null) {
      return null;
    }
    Map<String, String> custom = cfg.getCustomProperties();
    if (custom != null) {
      String wrapperPlugins = custom.get("wrapperPlugins");
      if (wrapperPlugins != null
          && Arrays.stream(wrapperPlugins.split(","))
              .map(String::trim)
              .anyMatch("iam"::equalsIgnoreCase)) {
        return "aws";
      }
      if ("true".equalsIgnoreCase(custom.get("enableIamAuth"))) {
        return "gcp";
      }
    }
    String driver = cfg.getDriver();
    if (driver != null && driver.contains("cloud.sql")) {
      return "gcp";
    }
    return null;
  }

  @Nullable
  private static Map<String, String> ebeanCustomProperties(
      @Nullable DataSourceBuilder.Settings cfg) {
    return cfg == null ? null : cfg.getCustomProperties();
  }

  private static void mergeNonBlank(
      @Nonnull Properties target, @Nullable Map<String, String> extra) {
    if (extra == null) {
      return;
    }
    for (Map.Entry<String, String> e : extra.entrySet()) {
      if (e.getKey() == null || e.getValue() == null || e.getValue().isBlank()) {
        continue;
      }
      target.setProperty(e.getKey(), e.getValue());
    }
  }

  @Nullable
  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String v : values) {
      if (v != null && !v.isBlank()) {
        return v;
      }
    }
    return null;
  }

  private static boolean ebeanPoolUsesIam(@Nullable DataSourceBuilder.Settings cfg) {
    if (cfg == null) {
      return false;
    }
    if (inferCloudProviderFromEbeanSettings(cfg) != null) {
      return true;
    }
    String dsUrl = cfg.getUrl();
    return dsUrl != null && dsUrl.contains("wrapperPlugins=iam");
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
