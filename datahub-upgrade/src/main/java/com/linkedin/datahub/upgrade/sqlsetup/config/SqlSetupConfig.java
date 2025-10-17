package com.linkedin.datahub.upgrade.sqlsetup.config;

import com.linkedin.datahub.upgrade.UpgradeUtils;
import com.linkedin.datahub.upgrade.sqlsetup.DatabaseType;
import com.linkedin.datahub.upgrade.sqlsetup.JdbcUrlParser;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetup;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetupArgs;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Slf4j
@Configuration
@Import(SystemAuthenticationFactory.class)
public class SqlSetupConfig {

  @Value("${ebean.url:}")
  @Setter
  @Getter
  private String ebeanUrl;

  /**
   * Provides a no-op MetricUtils for SqlSetup context to avoid dependency on system telemetry. This
   * allows LocalEbeanConfigFactory to work without requiring the full metrics infrastructure.
   */
  @Bean
  public MetricUtils metricUtils() {
    return MetricUtils.builder().build();
  }

  @Bean(name = "systemOperationContext")
  @ConditionalOnMissingBean
  public OperationContext operationContext(EntityRegistry entityRegistry) {
    // Create a minimal OperationContext for SqlSetup that essentially does nothing
    // This avoids the need for complex dependencies like entity services, search, etc.
    return TestOperationContexts.systemContextNoSearchAuthorization(entityRegistry);
  }

  @Bean(name = "sqlSetupArgs")
  @Nonnull
  public SqlSetupArgs createSetupArgs() {
    SqlSetupArgs args = new SqlSetupArgs();

    // Auto-detect database type from Ebean configuration
    DatabaseType dbType = detectDatabaseType();
    args.dbType = dbType;

    // Configure based on SqlSetup-specific environment variables only
    args.createTables = UpgradeUtils.getBoolean("CREATE_TABLES", true);
    args.createDatabase = UpgradeUtils.getBoolean("CREATE_DB", true);
    args.createUser = UpgradeUtils.getBoolean("CREATE_USER", false);
    args.createUserIamRole = UpgradeUtils.getString("IAM_ROLE");
    args.iamAuthEnabled =
        args.createUserIamRole != null && !args.createUserIamRole.trim().isEmpty();
    args.cdcEnabled = UpgradeUtils.getBoolean("CDC_MCL_PROCESSING_ENABLED", false);
    args.cdcUser = UpgradeUtils.getString("CDC_USER", "datahub_cdc");
    args.cdcPassword = UpgradeUtils.getString("CDC_PASSWORD", "datahub_cdc");

    // Extract database connection info from Spring Ebean configuration
    if (ebeanUrl == null || ebeanUrl.trim().isEmpty()) {
      throw new IllegalStateException(
          "ebean.url is required but not configured. Please set the ebean.url property.");
    }

    JdbcUrlParser.JdbcInfo jdbcInfo = JdbcUrlParser.parseJdbcUrl(ebeanUrl);
    args.databaseName = jdbcInfo.database;
    args.host = jdbcInfo.host;
    args.port = jdbcInfo.port;

    // Set user creation credentials based on CREATE_USER setting
    if (args.createUser) {
      if (args.iamAuthEnabled) {
        // IAM authentication: only set username, no password
        args.createUserUsername = UpgradeUtils.getString("CREATE_USER_USERNAME");
        args.createUserPassword = null; // No password for IAM auth
      } else {
        // Traditional authentication: set both username and password
        args.createUserUsername = UpgradeUtils.getString("CREATE_USER_USERNAME");
        args.createUserPassword = UpgradeUtils.getString("CREATE_USER_PASSWORD");
      }
    } else {
      // When CREATE_USER is disabled, these fields are not used
      args.createUserUsername = null;
      args.createUserPassword = null;
    }

    // Validate authentication configuration
    validateAuthenticationConfig(args);

    log.info(
        "SqlSetup configured for database type: {} based on Ebean configuration",
        dbType.getValue());

    return args;
  }

  /**
   * Auto-detect database type from Spring Ebean configuration using parsed JDBC URL scheme. This
   * provides a more reliable way to determine the database type.
   */
  DatabaseType detectDatabaseType() {
    // Detect from Spring Ebean JDBC URL using parsed scheme
    if (ebeanUrl != null && !ebeanUrl.trim().isEmpty()) {
      try {
        JdbcUrlParser.JdbcInfo jdbcInfo = JdbcUrlParser.parseJdbcUrl(ebeanUrl);

        // Map JDBC schemes to our internal database types
        try {
          DatabaseType dbType = jdbcInfo.databaseType;
          log.debug(
              "Detected {} from JDBC URL scheme: {}", dbType.getValue(), jdbcInfo.databaseType);
          return dbType;
        } catch (IllegalArgumentException e) {
          log.warn("Unknown database scheme from JDBC URL, attempting fallback detection");
          try {
            DatabaseType dbType = DatabaseType.fromJdbcUrlContent(ebeanUrl);
            log.debug("Detected {} from URL content fallback", dbType.getValue());
            return dbType;
          } catch (IllegalArgumentException fallbackException) {
            log.error(
                "Unsupported database scheme from JDBC URL. Only PostgreSQL and MySQL variants are supported.");
            throw new IllegalStateException(fallbackException.getMessage());
          }
        }
      } catch (Exception e) {
        log.error("Failed to parse JDBC URL for database type detection: {}", ebeanUrl, e);
        throw new IllegalStateException(
            "Failed to detect database type from JDBC URL. Please ensure the URL is valid and uses a supported database type (PostgreSQL or MySQL variants).",
            e);
      }
    }

    // Fallback to MySQL as default
    log.debug("No database type detected from Spring Ebean configuration, defaulting to MySQL");
    return DatabaseType.MYSQL;
  }

  /**
   * Validate authentication configuration to ensure either IAM or traditional credentials are
   * properly configured. This prevents accidental use of wrong authentication methods.
   */
  void validateAuthenticationConfig(SqlSetupArgs args) {
    // Only validate authentication if user creation is enabled
    if (!args.createUser) {
      return;
    }

    if (args.iamAuthEnabled) {
      // IAM authentication enabled - validate IAM role is provided
      if (args.createUserIamRole == null || args.createUserIamRole.trim().isEmpty()) {
        throw new IllegalStateException(
            "IAM user creation is enabled but IAM_ROLE is not specified. "
                + "Either set IAM_ROLE environment variable or disable IAM authentication.");
      }

      // Validate username is provided for IAM
      if (args.createUserUsername == null || args.createUserUsername.trim().isEmpty()) {
        throw new IllegalStateException(
            "IAM user creation is enabled but username is not specified. "
                + "Set CREATE_USER_USERNAME environment variable.");
      }

      log.info(
          "IAM user creation validated: role='{}', username='{}'",
          args.createUserIamRole,
          args.createUserUsername);
    } else {
      // Traditional authentication - validate username and password are provided
      if (args.createUserUsername == null || args.createUserUsername.trim().isEmpty()) {
        throw new IllegalStateException(
            "Traditional user creation requires username. "
                + "Set CREATE_USER_USERNAME environment variable.");
      }

      if (args.createUserPassword == null || args.createUserPassword.trim().isEmpty()) {
        throw new IllegalStateException(
            "Traditional user creation requires password. "
                + "Set CREATE_USER_PASSWORD environment variable.");
      }

      log.info("Traditional user creation validated: username='{}'", args.createUserUsername);
    }
  }

  @Bean(name = "sqlSetup")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  public SqlSetup createInstance(
      final Database ebeanServer, @Qualifier("sqlSetupArgs") final SqlSetupArgs setupArgs) {
    return new SqlSetup(ebeanServer, setupArgs);
  }

  @Bean(name = "sqlSetupCassandra")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  public SqlSetup createNotImplInstance() {
    throw new IllegalStateException("sqlSetup is not supported for cassandra!");
  }
}
