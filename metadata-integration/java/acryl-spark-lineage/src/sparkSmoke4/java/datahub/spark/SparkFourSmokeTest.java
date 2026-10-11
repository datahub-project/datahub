package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Apache Spark 4.x compatibility check for the DataHub Spark plugin.
 *
 * <p>Spark 4 is Scala 2.13 + Java 17 only, so this test lives in its own {@code sparkSmoke4} source
 * set built against {@code spark-sql_2.13:4.0.0} + {@code openlineage-spark_2.13} (OpenLineage
 * 1.50's {@code spark40} integration module) — it cannot share the Scala 2.12 / Spark 3.5 smoke
 * suite's classpath.
 *
 * <p>It runs a real local Spark 4 job (CSV read → write) with the {@code DatahubSparkListener}
 * attached and a file emitter, then asserts the plugin actually produced Spark lineage. If the
 * plugin does not support Spark 4, the emitter file is empty / the job errors and this test fails —
 * which is the signal we want.
 */
public class SparkFourSmokeTest {

  @Test
  public void emitsLineageOnSpark4(@TempDir Path tmp) throws Exception {
    Path in = tmp.resolve("in.csv");
    Files.write(in, "id,c\n1,x\n2,y\n".getBytes(StandardCharsets.UTF_8));
    Path out = tmp.resolve("out");
    Path mcps = tmp.resolve("mcps.json");

    SparkSession spark =
        SparkSession.builder()
            .appName("spark4-smoke")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            // Bind to loopback so stray LAN hosts can't connect to Spark's internal ports.
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.extraListeners", "datahub.spark.DatahubSparkListener")
            .config("spark.datahub.emitter", "file")
            .config("spark.datahub.file.filename", mcps.toString())
            .getOrCreate();
    try {
      Dataset<Row> df = spark.read().option("header", "true").csv(in.toString());
      df.write().mode(SaveMode.Overwrite).csv(out.toString());
    } finally {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }

    String emitted =
        Files.exists(mcps) ? new String(Files.readAllBytes(mcps), StandardCharsets.UTF_8) : "";

    assertTrue(
        emitted.contains("dataFlowInfo"),
        "DataHub listener emitted no DataFlow under Spark 4 (plugin did not attach/emit):\n"
            + emitted);
    assertTrue(
        emitted.contains("dataJobInputOutput"),
        "no DataJob lineage emitted under Spark 4:\n" + emitted);
    assertTrue(
        emitted.contains("in.csv"),
        "CSV input dataset was not captured in the Spark 4 lineage:\n" + emitted);
  }
}
