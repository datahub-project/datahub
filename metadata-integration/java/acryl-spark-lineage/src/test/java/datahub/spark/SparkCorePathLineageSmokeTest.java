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

/**
 * Core path lineage: a CSV read -> write emits a DataFlow, a DataJob, and an input->output edge.
 */
@Tag("integration")
public class SparkCorePathLineageSmokeTest extends SparkSmokeTestBase {

  @Test
  public void emitsFlowJobAndDatasetEdge(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("in.csv"), "id,c\n1,x\n2,y\n");
    Path out = tmp.resolve("out");

    EmittedMetadata md =
        runJob(
            Map.of(),
            spark -> {
              Dataset<Row> df = spark.read().option("header", "true").csv(in.toString());
              df.write().mode(SaveMode.Overwrite).csv(out.toString());
            });

    assertTrue(md.contains("urn:li:dataFlow:"), "no DataFlow emitted:\n" + md.raw);
    assertTrue(md.contains("urn:li:dataJob:"), "no DataJob emitted:\n" + md.raw);
    assertTrue(md.hasAspect("dataJobInputOutput"), "no dataJobInputOutput emitted:\n" + md.raw);
    // The CSV input must appear as a file-platform dataset in the job's input edges.
    assertTrue(
        md.contains("urn:li:dataPlatform:file") && md.contains("in.csv"),
        "expected the CSV input as a file dataset in the lineage edges:\n" + md.raw);
  }
}
