package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.Dependency;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.UnionRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
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

  private static final String SHADED_RDD_EXTRACTOR =
      "io.acryl.shaded.io.openlineage.spark.agent.util.RddDatasetInfoExtractor";

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

  @Test
  public void dataHubRddExtractorsRunWithScala213(@TempDir Path tmp) throws Exception {
    SparkSession spark =
        SparkSession.builder()
            .appName("spark4-scala213-rdd-smoke")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate();
    try {
      Path inputPath = tmp.resolve("input.parquet");
      spark.range(2).write().mode(SaveMode.Overwrite).parquet(inputPath.toString());

      Dataset<Row> input = spark.read().parquet(inputPath.toString());
      RDD<?> fileScanRdd = findRdd(input.queryExecution().toRdd(), FileScanRDD.class);
      assertNotNull(fileScanRdd, "Spark plan did not contain a FileScanRDD");
      assertFalse(
          findDatasetIdentifiers(fileScanRdd).isEmpty(),
          "DataHub's shaded FileScanRDD extractor returned no dataset");

      JavaRDD<Row> rows = input.javaRDD();
      RDD<?> unionRdd = findRdd(rows.union(rows).rdd(), UnionRDD.class);
      assertNotNull(unionRdd, "Spark did not create a UnionRDD");
      assertDoesNotThrow(
          () -> findDatasetIdentifiers(unionRdd),
          "DataHub's shaded UnionRDD extractor was not compatible with Scala 2.13");
    } finally {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }

  private static RDD<?> findRdd(RDD<?> rdd, Class<?> targetClass) {
    if (targetClass.isInstance(rdd)) {
      return rdd;
    }
    scala.collection.Iterator<Dependency<?>> dependencies = rdd.dependencies().iterator();
    while (dependencies.hasNext()) {
      RDD<?> match = findRdd(dependencies.next().rdd(), targetClass);
      if (match != null) {
        return match;
      }
    }
    return null;
  }

  private static List<?> findDatasetIdentifiers(RDD<?> rdd) throws Exception {
    // Resolve the relocated class from the artifact under test, not the unshaded compile classpath.
    Class<?> extractor = Class.forName(SHADED_RDD_EXTRACTOR);
    Method findDatasetIdentifiers = extractor.getMethod("findDatasetIdentifiers", RDD.class);
    try (Stream<?> identifiers = (Stream<?>) findDatasetIdentifiers.invoke(null, rdd)) {
      return identifiers.toList();
    }
  }
}
