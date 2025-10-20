package com.linkedin.datahub.upgrade.sqlsetup.config;

import static com.linkedin.datahub.upgrade.UpgradeUtils.getBoolean;

import com.linkedin.datahub.upgrade.sqlsetup.DatabaseOperations;
import com.linkedin.datahub.upgrade.sqlsetup.DatabaseType;
import com.linkedin.datahub.upgrade.sqlsetup.JdbcUrlParser;
import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Custom Ebean factory for SqlSetup that handles database creation. This factory creates a Database
 * instance that can connect to the server without requiring the specific database to exist first.
 */
@Configuration
@Slf4j
public class SqlSetupEbeanFactory {

  @Bean("ebeanServer")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected Database createServer(
      @Qualifier("gmsEbeanDatabaseConfig") DatabaseConfig serverConfig) {

    String originalUrl = serverConfig.getDataSourceConfig().getUrl();
    String modifiedUrl = originalUrl;

    // Check if database creation is needed
    boolean createDb = getBoolean("CREATE_DB", true);

    // Parse the JDBC URL to determine database type
    JdbcUrlParser.JdbcInfo jdbcInfo = JdbcUrlParser.parseJdbcUrl(originalUrl);
    DatabaseType dbType = jdbcInfo.databaseType;

    // Use database operations to modify URL
    DatabaseOperations dbOps = DatabaseOperations.create(dbType);
    modifiedUrl = dbOps.modifyJdbcUrl(originalUrl, createDb);

    serverConfig.getDataSourceConfig().url(modifiedUrl);

    try {
      return DatabaseFactory.create(serverConfig);
    } catch (Exception e) {
      log.error("Failed to create SqlSetup database connection: {}", e.getMessage());
      throw e;
    }
  }
}
