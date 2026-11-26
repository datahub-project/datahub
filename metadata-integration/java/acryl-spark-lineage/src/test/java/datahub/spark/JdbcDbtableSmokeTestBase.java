package datahub.spark;

import static datahub.spark.ListenerConf.listener;

import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Shared Postgres fixture for the JDBC {@code dbtable} naming smoke tests. Each subclass reads one
 * {@code dbtable} in its own Spark application, so its assertion covers that read alone.
 *
 * <p>A {@code dbtable} that is a FROM clause rather than a bare table name must still resolve to
 * the tables it reads. OpenLineage parses {@code select * from <dbtable>} for these. DataHub's
 * former vendored {@code JdbcSparkUtils} took any {@code dbtable} not starting with {@code (} as a
 * literal table name ({@code wh.public.orders AS o}), which the converter cannot turn into a URI,
 * so the read lost its input entirely.
 */
abstract class JdbcDbtableSmokeTestBase extends SparkSmokeTestBase {

  protected static final String ORDERS =
      "urn:li:dataset:(urn:li:dataPlatform:postgres,wh.public.orders,PROD)";
  protected static final String CUSTOMERS =
      "urn:li:dataset:(urn:li:dataPlatform:postgres,wh.public.customers,PROD)";

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

  /** Reads {@code dbtable} over JDBC into a CSV and returns the job's Postgres input URNs. */
  protected EmittedMetadata readDbtable(Path tmp, String dbtable) throws Exception {
    return runJob(
        listener().emitToFile(tmp.resolve("mcps.json")),
        spark ->
            spark
                .read()
                .jdbc(pg.getJdbcUrl(), dbtable, jdbcProps(pg))
                .write()
                .mode(SaveMode.Overwrite)
                .csv(tmp.resolve("out").toString()));
  }

  protected static Set<String> postgresInputs(EmittedMetadata md) {
    Set<String> out = new TreeSet<>();
    for (String urn : md.inputDatasetUrns()) {
      if (urn.contains("urn:li:dataPlatform:postgres,")) {
        out.add(urn);
      }
    }
    return out;
  }
}
