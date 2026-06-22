package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Column-level lineage: a projecting SELECT emits fine-grained (field-level) lineage edges. */
@Tag("integration")
public class SparkColumnLineageSmokeTest extends SparkSmokeTestBase {

  @Test
  public void emitsFineGrainedEdges(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("cll_in.csv"), "a,b\n1,2\n3,4\n");
    Path out = tmp.resolve("cll_out");

    EmittedMetadata md =
        runJob(
            Map.of("spark.datahub.metadata.dataset.captureColumnLevelLineage", "true"),
            spark -> {
              Dataset<Row> df = spark.read().option("header", "true").csv(in.toString());
              df.selectExpr("a as x", "b as y")
                  .write()
                  .mode(SaveMode.Overwrite)
                  .csv(out.toString());
            });

    assertTrue(
        md.contains("fineGrainedLineages"),
        "expected column-level (fineGrained) lineage to be emitted:\n" + md.raw);
  }
}
