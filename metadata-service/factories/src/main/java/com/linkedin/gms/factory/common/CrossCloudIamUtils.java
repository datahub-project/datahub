package com.linkedin.gms.factory.common;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Utility class for cross-cloud IAM authentication configuration */
@Slf4j
public class CrossCloudIamUtils {

  /** Configuration result for cross-cloud IAM authentication */
  public static class CrossCloudConfig {
    public final String driver;
    public final String url;
    public final Map<String, String> customProperties;

    public CrossCloudConfig(String driver, String url, Map<String, String> customProperties) {
      this.driver = driver;
      this.url = url;
      this.customProperties = customProperties;
    }
  }

  /**
   * Configure cross-cloud IAM authentication based on environment and settings
   *
   * @param originalUrl The original JDBC URL
   * @param defaultDriver The default JDBC driver to use
   * @param shouldUseIam Whether IAM authentication should be enabled
   * @param cloudProvider The cloud provider (aws/gcp/auto/traditional)
   * @param awsRegion AWS region environment variable
   * @param awsAccessKeyId AWS access key ID
   * @param awsSecretAccessKey AWS secret access key
   * @param awsSessionToken AWS session token
   * @param googleApplicationCredentials GCP credentials path
   * @param gcpProject GCP project ID
   * @param instanceConnectionName The instance connection name (for GCP)
   * @return CrossCloudConfig with driver, URL, and custom properties
   */
  public static CrossCloudConfig configureCrossCloudIam(
      String originalUrl,
      String defaultDriver,
      boolean shouldUseIam,
      String cloudProvider,
      String awsRegion,
      String awsAccessKeyId,
      String awsSecretAccessKey,
      String awsSessionToken,
      String googleApplicationCredentials,
      String gcpProject,
      String instanceConnectionName) {

    // Detect cloud provider internally
    String detectedCloud =
        detectCloudProvider(
            originalUrl,
            cloudProvider,
            awsRegion,
            awsAccessKeyId,
            awsSecretAccessKey,
            awsSessionToken,
            googleApplicationCredentials,
            gcpProject,
            instanceConnectionName);

    String finalDriver = defaultDriver;
    String finalUrl = originalUrl;
    Map<String, String> customProperties = new HashMap<>();

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

  /**
   * Detect cloud provider based on environment variables and URL
   *
   * @param dataSourceUrl The JDBC URL
   * @param cloudProvider The configured cloud provider
   * @param awsRegion AWS region environment variable
   * @param awsAccessKeyId AWS access key ID
   * @param awsSecretAccessKey AWS secret access key
   * @param awsSessionToken AWS session token
   * @param googleApplicationCredentials GCP credentials path
   * @param gcpProject GCP project ID
   * @param instanceConnectionName GCP instance connection name
   * @return The detected cloud provider (aws/gcp/traditional)
   */
  public static String detectCloudProvider(
      String dataSourceUrl,
      String cloudProvider,
      String awsRegion,
      String awsAccessKeyId,
      String awsSecretAccessKey,
      String awsSessionToken,
      String googleApplicationCredentials,
      String gcpProject,
      String instanceConnectionName) {

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
  public static boolean isPostgresUrl(String url) {
    return url != null && url.toLowerCase().contains("postgresql");
  }

  /** Check if URL is MySQL */
  public static boolean isMysqlUrl(String url) {
    return url != null && url.toLowerCase().contains("mysql");
  }
}
