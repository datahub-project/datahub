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
 * Global {@code platform_instance} fallback: when there is no {@code connections} entry for a
 * source, the upstream URN is stamped with the global {@code metadata.dataset.platformInstance}.
 */
@Tag("integration")
public class SparkGlobalInstanceSmokeTest extends SparkSmokeTestBase {

  private static PostgreSQLContainer<?> pg;

  @BeforeAll
  static void start() throws Exception {
    pg = new PostgreSQLContainer<>("postgres:15-alpine").withDatabaseName("wh");
    pg.start();
    seed(pg, "CREATE TABLE orders (id INT)", "INSERT INTO orders VALUES (1)");
  }

  @AfterAll
  static void stop() {
    if (pg != null) {
      pg.stop();
    }
  }

  @Test
  public void stampsGlobalInstanceWhenNoConnectionMapping(@TempDir Path tmp) throws Exception {
    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")).platformInstance("prod_a"),
            spark -> jdbcReadToCsv(spark, pg, "orders", tmp.resolve("out")));

    assertTrue(
        md.contains("urn:li:dataPlatform:postgres,prod_a."),
        "postgres upstream URN should carry the global platform_instance 'prod_a':\n" + md.raw);
  }
}
