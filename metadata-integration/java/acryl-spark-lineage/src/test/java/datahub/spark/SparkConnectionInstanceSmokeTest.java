package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * The fix: a JDBC upstream's URN is stamped with the {@code platform_instance} resolved from the
 * {@code connections} map keyed by the OpenLineage namespace (postgres://host:port).
 */
@Tag("integration")
public class SparkConnectionInstanceSmokeTest extends SparkSmokeTestBase {

  private static PostgreSQLContainer<?> pg;

  @BeforeAll
  static void start() throws Exception {
    pg = new PostgreSQLContainer<>("postgres:15-alpine").withDatabaseName("wh");
    pg.start();
    seed(pg, "CREATE TABLE orders (id INT, amount INT)", "INSERT INTO orders VALUES (1, 100)");
  }

  @AfterAll
  static void stop() {
    if (pg != null) {
      pg.stop();
    }
  }

  @Test
  public void stampsConnectionInstanceOnJdbcUpstream(@TempDir Path tmp) throws Exception {
    EmittedMetadata md =
        runJob(
            listener()
                .emitToFile(tmp.resolve("mcps.json"))
                .connection(pgNamespace(pg), "warehouse_a", "PROD"),
            spark -> jdbcReadToCsv(spark, pg, "orders", tmp.resolve("out")));

    assertTrue(
        md.datasetUrnsOnPlatform("postgres").stream()
            .anyMatch(u -> u.contains("postgres,warehouse_a.")),
        "postgres upstream URN should carry the per-connection instance 'warehouse_a':\n" + md.raw);
  }
}
