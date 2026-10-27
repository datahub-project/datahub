package com.linkedin.metadata;

import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainers MySQL helpers for metadata-io integration tests (optimistic-locking dialect
 * coverage).
 */
public final class MysqlTestUtils {

  private MysqlTestUtils() {}

  private static MySQLContainer<?> sharedMysql;

  @Nonnull
  public static DockerImageName datahubMysqlDockerImageName() {
    String tag =
        Optional.ofNullable(System.getenv("DATAHUB_MYSQL_VERSION"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .orElseGet(() -> System.getProperty("datahub.testcontainers.mysql.version", "8.2"));
    return DockerImageName.parse("mysql:" + tag);
  }

  @Nonnull
  public static synchronized MySQLContainer<?> startMysql() {
    if (sharedMysql != null && sharedMysql.isRunning()) {
      return sharedMysql;
    }
    MySQLContainer<?> container =
        new MySQLContainer<>(datahubMysqlDockerImageName())
            .withDatabaseName("datahub")
            .withUsername("datahub")
            .withPassword("datahub")
            .withCommand(
                "--character-set-server=utf8mb4",
                "--collation-server=utf8mb4_bin",
                "--default-authentication-plugin=caching_sha2_password")
            .withStartupTimeout(Duration.ofMinutes(3))
            .withReuse(true);
    container.start();
    sharedMysql = container;
    return container;
  }

  @Nonnull
  public static Database createEbeanPrimaryDatabase(
      @Nonnull MySQLContainer<?> container, @Nonnull String serverName) {
    DataSourceConfig dsc = new DataSourceConfig();
    dsc.setUrl(
        container.getJdbcUrl()
            + "?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8");
    dsc.setUsername(container.getUsername());
    dsc.setPassword(container.getPassword());
    dsc.setDriver("com.mysql.cj.jdbc.Driver");

    DatabaseConfig cfg = new DatabaseConfig();
    cfg.setName(serverName);
    cfg.setDataSourceConfig(dsc);
    cfg.setDefaultServer(false);
    cfg.setDdlGenerate(true);
    cfg.setDdlRun(true);
    cfg.addPackage("com.linkedin.metadata.entity.ebean");
    return DatabaseFactory.create(cfg);
  }

  @Nonnull
  public static String uniqueServerName(@Nonnull String base) {
    return base + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  /** Short random identifier for building unique test URNs. */
  @Nonnull
  public static String shortId() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }
}
