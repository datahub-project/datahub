package com.linkedin.metadata;

import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EbeanTestUtils {

  private EbeanTestUtils() {}

  @Nonnull
  public static Database createTestServer(String instanceId) {
    return DatabaseFactory.create(createTestingH2ServerConfig(instanceId));
  }

  @Nonnull
  public static Database createNamedTestServer(String instanceId, String serverName) {
    DatabaseConfig config = createTestingH2ServerConfig(instanceId);
    config.setName(serverName);
    config.setDefaultServer(false); // Explicitly set as non-default to avoid conflicts

    // Add the required entity packages for DataHub
    config.getPackages().add("com.linkedin.metadata.entity.ebean");

    return DatabaseFactory.create(config);
  }

  @Nonnull
  private static DatabaseConfig createTestingH2ServerConfig(String instanceId) {
    DataSourceConfig dataSourceConfig = new DataSourceConfig();
    dataSourceConfig.setUsername("tester");
    dataSourceConfig.setPassword("");
    dataSourceConfig.setUrl(
        String.format("jdbc:h2:mem:%s;IGNORECASE=TRUE;mode=mysql;", instanceId));
    dataSourceConfig.setDriver("org.h2.Driver");

    DatabaseConfig serverConfig = new DatabaseConfig();
    serverConfig.setName("gma");
    serverConfig.setDataSourceConfig(dataSourceConfig);
    serverConfig.setDdlGenerate(true);
    serverConfig.setDdlRun(true);

    return serverConfig;
  }

  /**
   * Shutdown a Database instance to prevent thread pool and connection leaks. This includes the
   * "gma.heartBeat" thread and connection pools.
   *
   * @param database The Database instance to shutdown, or null if none
   */
  public static void shutdownDatabase(@Nullable Database database) {
    if (database != null) {
      try {
        database.shutdown();
      } catch (Exception e) {
        // Log but don't fail - this is called from test cleanup
        System.err.println("Failed to shutdown Database: " + e.getMessage());
      }
    }
  }

  /**
   * Shutdown a Database instance retrieved from an EbeanAspectDao.
   *
   * @param aspectDao The EbeanAspectDao instance, or null if none
   */
  public static void shutdownDatabaseFromAspectDao(@Nullable EbeanAspectDao aspectDao) {
    if (aspectDao != null) {
      try {
        Database server = aspectDao.getServer();
        shutdownDatabase(server);
      } catch (Exception e) {
        // Log but don't fail - this is called from test cleanup
        System.err.println("Failed to shutdown Database from AspectDao: " + e.getMessage());
      }
    }
  }

  /**
   * Shutdown multiple Database instances.
   *
   * @param databases Collection of Database instances to shutdown
   */
  public static void shutdownDatabases(@Nullable Collection<Database> databases) {
    if (databases != null) {
      for (Database db : databases) {
        shutdownDatabase(db);
      }
    }
  }
}
