package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * A JDBC {@code dbtable} that is a FROM clause rather than a bare table name must still resolve to
 * the tables it reads. OpenLineage parses {@code select * from <dbtable>} for these. DataHub's
 * former vendored {@code JdbcSparkUtils} took any {@code dbtable} not starting with {@code (} as a
 * literal table name ({@code wh.public.orders AS o}), which the converter cannot turn into a URI,
 * so the read lost its input entirely.
 */
@Tag("integration")
public class SparkJdbcDbtableNamingSmokeTest extends SparkSmokeTestBase {

  private static PostgreSQLContainer<?> pg;

  @BeforeAll
  static void start() throws Exception {
    pg = new PostgreSQLContainer<>("postgres:15-alpine").withDatabaseName("wh");
    pg.start();
    seed(
        pg,
        "CREATE TABLE orders (order_id INT, customer_id INT, amount INT)",
        "CREATE TABLE customers (cust_id INT, name VARCHAR(20))",
        "INSERT INTO orders VALUES (1, 1, 100)",
        "INSERT INTO customers VALUES (1, 'a')");
  }

  @AfterAll
  static void stop() {
    if (pg != null) {
      pg.stop();
    }
  }

  @Test
  public void aliasAndJoinDbtablesResolveToTheirTables(@TempDir Path tmp) throws Exception {
    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")),
            spark -> {
              for (String dbtable :
                  Arrays.asList(
                      "public.orders AS o",
                      "public.orders o JOIN public.customers c ON o.customer_id = c.cust_id")) {
                spark
                    .read()
                    .jdbc(pg.getJdbcUrl(), dbtable, jdbcProps(pg))
                    .write()
                    .mode(SaveMode.Append)
                    .csv(tmp.resolve("out").toString());
              }
            });

    assertEquals(
        new TreeSet<>(
            Arrays.asList(
                "urn:li:dataset:(urn:li:dataPlatform:postgres,wh.public.customers,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,wh.public.orders,PROD)")),
        new TreeSet<String>(postgresInputs(md)),
        "JDBC inputs should be the tables the dbtable reads:\n" + md.raw);
  }

  private static Set<String> postgresInputs(EmittedMetadata md) {
    Set<String> out = new TreeSet<>();
    for (String urn : md.inputDatasetUrns()) {
      if (urn.contains("urn:li:dataPlatform:postgres,")) {
        out.add(urn);
      }
    }
    return out;
  }
}
