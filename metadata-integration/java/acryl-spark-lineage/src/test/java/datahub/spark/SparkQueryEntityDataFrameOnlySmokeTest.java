package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spark emits no SQLJobFacet for a job expressed purely via the DataFrame API, so no query entity
 * should be emitted even with {@code captureQueryEntities} on. Pins that documented limitation.
 */
@Tag("integration")
public class SparkQueryEntityDataFrameOnlySmokeTest extends SparkSmokeTestBase {

  @Test
  public void dataFrameOnlyJobEmitsNoQueryEntity(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("people.csv"), "age,name\n30,Alice\n10,Bob\n");
    Path out = tmp.resolve("renamed");

    EmittedMetadata md =
        runJob(
            listener()
                .emitToFile(tmp.resolve("mcps.json"))
                .captureColumnLevelLineage(true)
                .captureQueryEntities(true),
            spark -> {
              Dataset<Row> df = spark.read().option("header", "true").csv(in.toString());
              df.selectExpr("age as a", "name as n")
                  .write()
                  .mode(SaveMode.Overwrite)
                  .csv(out.toString());
            });

    assertFalse(md.hasEntity("query"), "expected no query entity to be emitted:\n" + md.raw);
  }
}
