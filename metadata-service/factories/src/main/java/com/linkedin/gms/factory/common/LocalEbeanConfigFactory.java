package com.linkedin.gms.factory.common;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import io.ebean.datasource.DataSourcePoolListener;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class LocalEbeanConfigFactory {

  @Value("${ebean.username}")
  private String ebeanDatasourceUsername;

  @Value("${ebean.password}")
  private String ebeanDatasourcePassword;

  @Value("${ebean.driver}")
  private String ebeanDatasourceDriver;

  @Value("${ebean.url}")
  private String ebeanDatasourceUrl;

  @Value("${ebean.minConnections:2}")
  private Integer ebeanMinConnections;

  @Value("${ebean.maxConnections:50}")
  private Integer ebeanMaxConnections;

  @Value("${ebean.maxInactiveTimeSeconds:120}")
  private Integer ebeanMaxInactiveTimeSecs;

  @Value("${ebean.maxAgeMinutes:120}")
  private Integer ebeanMaxAgeMinutes;

  @Value("${ebean.leakTimeMinutes:15}")
  private Integer ebeanLeakTimeMinutes;

  @Value("${ebean.waitTimeoutMillis:1000}")
  private Integer ebeanWaitTimeoutMillis;

  @Value("${ebean.autoCreateDdl:false}")
  private Boolean ebeanAutoCreate;

  @Value("${ebean.postgresUseIamAuth:false}")
  private Boolean postgresUseIamAuth;

  @Value("${ebean.captureStackTrace:false}")
  private Boolean captureStackTrace;

  @Value("${ebean.useIamAuth:false}")
  private Boolean useIamAuth;

  @Value("${ebean.cloudProvider:auto}")
  private String cloudProvider;

  // Environment variable properties for cloud detection
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

  public static DataSourcePoolListener getListenerToTrackCounts(
      MetricUtils metricUtils, String metricName) {
    final String counterName = "ebeans_connection_pool_size_" + metricName;
    return new DataSourcePoolListener() {
      @Override
      public void onAfterBorrowConnection(Connection connection) {
        if (metricUtils != null) metricUtils.increment(counterName, 1);
      }

      @Override
      public void onBeforeReturnConnection(Connection connection) {
        if (metricUtils != null) metricUtils.increment(counterName, -1);
      }
    };
  }

  @Bean("ebeanDataSourceConfig")
  public DataSourceConfig buildDataSourceConfig(MetricUtils metricUtils) {
    return buildDataSourceConfig(ebeanDatasourceUrl, metricUtils);
  }

  public DataSourceConfig buildDataSourceConfig(String dataSourceUrl, MetricUtils metricUtils) {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();

    // Configure cross-cloud IAM authentication
    CrossCloudConfig crossCloudConfig = configureCrossCloudIam(dataSourceUrl);

    dataSourceConfig.setUsername(ebeanDatasourceUsername);
    dataSourceConfig.setPassword(ebeanDatasourcePassword);
    dataSourceConfig.setUrl(crossCloudConfig.url);
    dataSourceConfig.setDriver(crossCloudConfig.driver);
    dataSourceConfig.setMinConnections(ebeanMinConnections);
    dataSourceConfig.setMaxConnections(ebeanMaxConnections);
    dataSourceConfig.setMaxInactiveTimeSecs(ebeanMaxInactiveTimeSecs);
    dataSourceConfig.setMaxAgeMinutes(ebeanMaxAgeMinutes);
    dataSourceConfig.setLeakTimeMinutes(ebeanLeakTimeMinutes);
    dataSourceConfig.setWaitTimeoutMillis(ebeanWaitTimeoutMillis);
    dataSourceConfig.setCaptureStackTrace(captureStackTrace);
    dataSourceConfig.setListener(getListenerToTrackCounts(metricUtils, "main"));

    // Set custom properties for IAM authentication
    if (crossCloudConfig.customProperties != null) {
      dataSourceConfig.setCustomProperties(crossCloudConfig.customProperties);
    }

    return dataSourceConfig;
  }

  @Bean(name = "gmsEbeanDatabaseConfig")
  protected DatabaseConfig createInstance(
      @Qualifier("ebeanDataSourceConfig") DataSourceConfig config) {
    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("gmsEbeanDatabaseConfig");
    serverConfig.setDataSourceConfig(config);
    serverConfig.setDdlGenerate(ebeanAutoCreate);
    serverConfig.setDdlRun(ebeanAutoCreate);
    return serverConfig;
  }

  /** Configuration result for cross-cloud IAM authentication */
  static class CrossCloudConfig {
    final String driver;
    final String url;
    final Map<String, String> customProperties;

    CrossCloudConfig(String driver, String url, Map<String, String> customProperties) {
      this.driver = driver;
      this.url = url;
      this.customProperties = customProperties;
    }
  }

  /** Configure cross-cloud IAM authentication based on environment and settings */
  CrossCloudConfig configureCrossCloudIam(String originalUrl) {
    String detectedCloud = detectCloudProvider(originalUrl);
    String finalDriver = ebeanDatasourceDriver;
    String finalUrl = originalUrl;
    Map<String, String> customProperties = new HashMap<>();

    // Determine if IAM authentication should be used
    boolean shouldUseIam = useIamAuth || postgresUseIamAuth;

    if (shouldUseIam) {
      log.info("IAM authentication enabled for cloud provider: {}", detectedCloud);

      if ("aws".equalsIgnoreCase(detectedCloud) || "aws".equalsIgnoreCase(cloudProvider)) {
        // AWS IAM authentication
        if (isPostgresUrl(originalUrl)) {
          // PostgreSQL with AWS IAM
          customProperties.put("wrapperPlugins", "iam");
          log.info("Configured PostgreSQL with AWS IAM authentication");
        } else if (isMysqlUrl(originalUrl)) {
          // MySQL with AWS IAM - use AWS JDBC Wrapper
          finalDriver = "software.amazon.jdbc.Driver";
          String baseUrl = originalUrl.replace("jdbc:mysql://", "jdbc:aws-wrapper:mysql://");

          // Add wrapperPlugins=iam parameter to enable IAM authentication
          finalUrl =
              baseUrl.contains("?")
                  ? baseUrl + "&wrapperPlugins=iam"
                  : baseUrl + "?wrapperPlugins=iam";

          log.info("Configured MySQL with AWS JDBC Wrapper for IAM authentication: {}", finalUrl);
        }
      } else if ("gcp".equalsIgnoreCase(detectedCloud) || "gcp".equalsIgnoreCase(cloudProvider)) {
        // GCP IAM authentication
        if (isMysqlUrl(originalUrl)) {
          // MySQL with GCP Cloud SQL IAM
          finalDriver = "com.google.cloud.sql.mysql.SocketFactory";
          finalUrl = originalUrl; // Keep original URL format
          customProperties.put("socketFactory", "com.google.cloud.sql.mysql.SocketFactory");
          customProperties.put("cloudSqlInstance", instanceConnectionName);
          customProperties.put("enableIamAuth", "true");
          customProperties.put("sslmode", "disable");
          customProperties.put("cloudSqlRefreshStrategy", "lazy");
          log.info("Configured MySQL with GCP Cloud SQL IAM authentication");
        } else if (isPostgresUrl(originalUrl)) {
          // PostgreSQL with GCP Cloud SQL IAM
          finalDriver = "com.google.cloud.sql.postgres.SocketFactory";
          finalUrl = originalUrl; // Keep original URL format
          customProperties.put("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
          customProperties.put("cloudSqlInstance", instanceConnectionName);
          customProperties.put("enableIamAuth", "true");
          customProperties.put("sslmode", "disable");
          customProperties.put("cloudSqlRefreshStrategy", "lazy");
          log.info("Configured PostgreSQL with GCP Cloud SQL IAM authentication");
        }
      } else {
        log.warn(
            "IAM authentication requested but cloud provider '{}' not supported", detectedCloud);
      }
    }

    return new CrossCloudConfig(
        finalDriver, finalUrl, customProperties.isEmpty() ? null : customProperties);
  }

  /** Detect cloud provider based on environment variables and URL */
  String detectCloudProvider(String dataSourceUrl) {
    if (!"auto".equalsIgnoreCase(cloudProvider)) {
      return cloudProvider;
    }

    // Check for AWS environment variables (treat empty strings as null)
    if ((awsRegion != null && !awsRegion.isEmpty())
        || (awsAccessKeyId != null && !awsAccessKeyId.isEmpty())
        || (awsSecretAccessKey != null && !awsSecretAccessKey.isEmpty())
        || (awsSessionToken != null && !awsSessionToken.isEmpty())) {
      return "aws";
    }

    // Check for GCP environment variables (treat empty strings as null)
    if ((googleApplicationCredentials != null && !googleApplicationCredentials.isEmpty())
        || (gcpProject != null && !gcpProject.isEmpty())
        || (instanceConnectionName != null && !instanceConnectionName.isEmpty())) {
      return "gcp";
    }

    // Check URL patterns
    if (dataSourceUrl != null) {
      if (dataSourceUrl.contains("rds.amazonaws.com") || dataSourceUrl.contains("amazonaws.com")) {
        return "aws";
      }
      if (dataSourceUrl.contains("googleapis.com") || dataSourceUrl.contains("cloudsql")) {
        return "gcp";
      }
    }

    return "traditional";
  }

  /** Check if URL is PostgreSQL */
  boolean isPostgresUrl(String url) {
    return url != null && url.toLowerCase().contains("postgresql");
  }

  /** Check if URL is MySQL */
  boolean isMysqlUrl(String url) {
    return url != null && url.toLowerCase().contains("mysql");
  }
}
