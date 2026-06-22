package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Cross-domain A + B: a single Spark job reading from two distinct Postgres connections stamps each
 * upstream with its own connection's {@code platform_instance} — not one collapsed instance.
 */
@Tag("integration")
public class SparkMultiConnectionSmokeTest extends SparkSmokeTestBase {

  private static PostgreSQLContainer<?> pgA;
  private static PostgreSQLContainer<?> pgB;

  @BeforeAll
  static void start() throws Exception {
    pgA = new PostgreSQLContainer<>("postgres:15-alpine").withDatabaseName("wha");
    pgB = new PostgreSQLContainer<>("postgres:15-alpine").withDatabaseName("whb");
    pgA.start();
    pgB.start();
    seed(pgA, "CREATE TABLE orders (id INT)", "INSERT INTO orders VALUES (1)");
    seed(pgB, "CREATE TABLE customers (id INT)", "INSERT INTO customers VALUES (1)");
  }

  @AfterAll
  static void stop() {
    if (pgA != null) {
      pgA.stop();
    }
    if (pgB != null) {
      pgB.stop();
    }
  }

  @Test
  public void eachConnectionResolvesOwnInstance(@TempDir Path tmp) throws Exception {
    EmittedMetadata md =
        runJob(
            listener()
                .emitToFile(tmp.resolve("mcps.json"))
                .connection(pgNamespace(pgA), "warehouse_a", "PROD")
                .connection(pgNamespace(pgB), "warehouse_b", "PROD"),
            spark -> {
              Dataset<Row> a = spark.read().jdbc(pgA.getJdbcUrl(), "orders", jdbcProps(pgA));
              Dataset<Row> b = spark.read().jdbc(pgB.getJdbcUrl(), "customers", jdbcProps(pgB));
              a.union(b).write().mode(SaveMode.Overwrite).csv(tmp.resolve("out").toString());
            });

    assertTrue(
        md.contains("urn:li:dataPlatform:postgres,warehouse_a."),
        "pg A upstream should carry 'warehouse_a':\n" + md.raw);
    assertTrue(
        md.contains("urn:li:dataPlatform:postgres,warehouse_b."),
        "pg B upstream should carry 'warehouse_b':\n" + md.raw);
  }
}
