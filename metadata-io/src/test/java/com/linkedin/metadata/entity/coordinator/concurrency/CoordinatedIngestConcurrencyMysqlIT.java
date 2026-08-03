package com.linkedin.metadata.entity.coordinator.concurrency;

import com.linkedin.metadata.EbeanTestUtils;
import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import java.time.Duration;
import java.util.UUID;
import org.testcontainers.containers.MySQLContainer;

/**
 * Coordinated-ingest concurrency IT against a real MySQL/InnoDB (REPEATABLE READ + gap locks +
 * {@code FOR UPDATE}). All scenarios live in {@link AbstractCoordinatedIngestConcurrencyIT}.
 */
public class CoordinatedIngestConcurrencyMysqlIT extends AbstractCoordinatedIngestConcurrencyIT {

  private MySQLContainer<?> container;

  @Override
  protected Database startEngineDatabase() {
    container =
        new MySQLContainer<>("mysql:8.0").withStartupTimeout(Duration.ofMinutes(2)).withReuse(true);
    container.start();

    DataSourceConfig dataSource = new DataSourceConfig();
    dataSource.setUrl(container.getJdbcUrl());
    dataSource.setUsername(container.getUsername());
    dataSource.setPassword(container.getPassword());
    dataSource.setDriver("com.mysql.cj.jdbc.Driver");

    DatabaseConfig config = new DatabaseConfig();
    config.setName("coord_concurrency_mysql_" + UUID.randomUUID().toString().replace("-", ""));
    config.setDataSourceConfig(dataSource);
    config.setDefaultServer(false);
    config.setDdlGenerate(true);
    config.setDdlRun(true);
    config.addPackage("com.linkedin.metadata.entity.ebean");
    config.addPackage("com.linkedin.metadata.queue.ebean");
    return DatabaseFactory.create(config);
  }

  @Override
  protected void stopEngineDatabase(Database database) {
    EbeanTestUtils.shutdownDatabase(database);
    if (container != null) {
      container.stop();
    }
  }

  @Override
  protected String engineLabel() {
    return "mysql";
  }
}
