package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Real-Spark smoke test for the cross-platform {@code platform_instance} fix.
 *
 * <p>Runs an actual local {@link SparkSession} with the DataHub listener attached and the file
 * emitter, reading from a real Postgres (testcontainers) so OpenLineage emits a connection-bearing
 * namespace ({@code postgres://host:port}). It then asserts that the configured {@code
 * spark.datahub.metadata.dataset.connections} entry stamps the owning {@code platform_instance}
 * onto the upstream dataset URN — i.e. the exact path exercised by the Glue cross-account fix,
 * minus the AWS dependency (Glue would need real AWS; the converter-level Glue ARN handling is
 * covered by the unit tests using {@code sample_glue.json}).
 *
 * <p>Tagged {@code integration}; requires Docker. Run via {@code ./gradlew
 * :metadata-integration:java:acryl-spark-lineage:sparkRealSmokeTest}.
 */
@Tag("integration")
public class SparkConnectionInstanceIntegrationTest {

  @Test
  public void testConnectionsMapStampsPlatformInstanceOnJdbcUpstream(@TempDir Path tmp)
      throws Exception {
    try (PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("warehouse")
            .withUsername("dh")
            .withPassword("dh")) {
      postgres.start();

      // Seed a source table.
      try (Connection conn =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement st = conn.createStatement()) {
        st.execute("CREATE TABLE orders (id INT, amount INT)");
        st.execute("INSERT INTO orders VALUES (1, 100), (2, 200)");
      }

      // OpenLineage names this JDBC source by its connection authority: postgres://{host}:{port}.
      String connectionKey =
          "postgres://"
              + postgres.getHost()
              + ":"
              + postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);

      Path mcpFile = tmp.resolve("mcps.json");
      Path outDir = tmp.resolve("orders_out");

      SparkSession spark =
          SparkSession.builder()
              .appName("connection-instance-smoke")
              .master("local[1]")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.shuffle.partitions", "1")
              .config("spark.extraListeners", "datahub.spark.DatahubSparkListener")
              .config("spark.datahub.emitter", "file")
              .config("spark.datahub.file.filename", mcpFile.toString())
              // The fix under test: resolve the owning instance from the connection namespace.
              .config(
                  "spark.datahub.metadata.dataset.connections.\""
                      + connectionKey
                      + "\".platformInstance",
                  "warehouse_a")
              .config(
                  "spark.datahub.metadata.dataset.connections.\"" + connectionKey + "\".env",
                  "PROD")
              .getOrCreate();

      try {
        Properties props = new Properties();
        props.setProperty("user", postgres.getUsername());
        props.setProperty("password", postgres.getPassword());
        props.setProperty("driver", "org.postgresql.Driver");

        Dataset<Row> df = spark.read().jdbc(postgres.getJdbcUrl(), "orders", props);
        // A write forces a Spark job with the JDBC table as a tracked input.
        df.write().mode(SaveMode.Overwrite).csv(outDir.toString());
      } finally {
        // Stopping the session flushes the listener, which writes the emitted MCPs to the file.
        spark.stop();
      }

      assertTrue(Files.exists(mcpFile), "Emitter wrote no file at " + mcpFile);
      String emitted = new String(Files.readAllBytes(mcpFile));

      // The Postgres upstream URN must carry the per-connection platform_instance ("warehouse_a")
      // resolved from the connections map — not a bare postgres URN.
      assertTrue(
          emitted.contains("urn:li:dataPlatform:postgres,warehouse_a."),
          "Expected the postgres upstream URN to be stamped with platform_instance 'warehouse_a' "
              + "(connection key '"
              + connectionKey
              + "'). Emitted MCPs:\n"
              + emitted);
    }
  }
}
