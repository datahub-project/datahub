package com.linkedin.datahub.upgrade.sqlsetup.config;

import com.linkedin.datahub.upgrade.sqlsetup.DatabaseType;
import com.linkedin.datahub.upgrade.sqlsetup.JdbcUrlParser;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetup;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetupArgs;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EnvironmentUtils;
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
    // Auto-detect database type from Ebean configuration
    DatabaseType dbType = detectDatabaseType();

    // Configure based on SqlSetup-specific environment variables only
    boolean createTables = EnvironmentUtils.getBoolean("CREATE_TABLES", true);
    boolean createDatabase = EnvironmentUtils.getBoolean("CREATE_DB", true);
    boolean createUser = EnvironmentUtils.getBoolean("CREATE_USER", false);
    boolean cdcEnabled = EnvironmentUtils.getBoolean("CDC_MCL_PROCESSING_ENABLED", false);
    String cdcUser = EnvironmentUtils.getString("CDC_USER", "datahub_cdc");
    String cdcPassword = EnvironmentUtils.getString("CDC_PASSWORD", "datahub_cdc");

    // Extract database connection info from Spring Ebean configuration
    if (ebeanUrl == null || ebeanUrl.trim().isEmpty()) {
      throw new IllegalStateException(
          "ebean.url is required but not configured. Please set the ebean.url property.");
    }

    JdbcUrlParser.JdbcInfo jdbcInfo = JdbcUrlParser.parseJdbcUrl(ebeanUrl);
    String databaseName = jdbcInfo.database;
    String host = jdbcInfo.host;
    int port = jdbcInfo.port;

    // Set user creation credentials based on CREATE_USER setting
    // If CREATE_USER_PASSWORD is not provided, assume IAM authentication
    String createUserUsername;
    String createUserPassword;
    boolean iamAuthEnabled = false;
    if (createUser) {
      createUserUsername = EnvironmentUtils.getString("CREATE_USER_USERNAME");
      createUserPassword = EnvironmentUtils.getString("CREATE_USER_PASSWORD");

      // If password is not provided, use IAM authentication
      if (createUserPassword == null || createUserPassword.trim().isEmpty()) {
        iamAuthEnabled = true;
        createUserPassword = null; // No password for IAM auth
        log.info("User creation with IAM authentication detected (no password provided)");
      } else {
        iamAuthEnabled = false;
        log.info("User creation with traditional password authentication");
      }
    } else {
      // When CREATE_USER is disabled, these fields are not used
      createUserUsername = null;
      createUserPassword = null;
    }

    SqlSetupArgs args =
        new SqlSetupArgs(
            createTables,
            createDatabase,
            createUser,
            iamAuthEnabled,
            dbType,
            cdcEnabled,
            cdcUser,
            cdcPassword,
            createUserUsername,
            createUserPassword,
            host,
            port,
            databaseName);

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

    throw new IllegalStateException("No database type detected from Spring Ebean configuration");
  }

  /**
   * Validate authentication configuration to ensure either IAM or traditional credentials are
   * properly configured. This prevents accidental use of wrong authentication methods.
   */
  void validateAuthenticationConfig(SqlSetupArgs args) {
    // Only validate authentication if user creation is enabled
    if (!args.isCreateUser()) {
      return;
    }

    if (args.isIamAuthEnabled()) {
      // For both MySQL and PostgreSQL RDS IAM authentication:
      // - MySQL: Uses constant 'RDS' in CREATE USER statement
      // - PostgreSQL: Uses rds_iam role grant
      // The actual IAM permissions are managed by AWS IAM policies, not stored in the database

      // Validate username is provided for IAM
      if (args.getCreateUserUsername() == null || args.getCreateUserUsername().trim().isEmpty()) {
        throw new IllegalStateException(
            "IAM user creation is enabled but username is not specified. "
                + "Set CREATE_USER_USERNAME environment variable.");
      }

      log.info(
          "IAM user creation validated for {}: username='{}' (IAM permissions managed by AWS IAM policies)",
          args.getDbType().getValue(),
          args.getCreateUserUsername());
    } else {
      // Traditional authentication - validate username and password are provided
      if (args.getCreateUserUsername() == null || args.getCreateUserUsername().trim().isEmpty()) {
        throw new IllegalStateException(
            "Traditional user creation requires username. "
                + "Set CREATE_USER_USERNAME environment variable.");
      }

      if (args.getCreateUserPassword() == null || args.getCreateUserPassword().trim().isEmpty()) {
        throw new IllegalStateException(
            "Traditional user creation requires password. "
                + "Set CREATE_USER_PASSWORD environment variable.");
      }

      log.info("Traditional user creation validated: username='{}'", args.getCreateUserUsername());
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
