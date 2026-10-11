package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * file_partition_regexp: a configured regex is stripped (anchored at the end) from file dataset
 * paths so a partitioned location (e.g. {@code .../events/dt=2024-01-01}) maps to the logical table
 * (e.g. {@code .../events}). Exercises the end-to-end Spark→listener→converter path for the
 * bare-{@code file}-namespace case (which bypasses HdfsPathDataset and is handled in the
 * converter's fallback branch).
 */
@Tag("integration")
public class SparkPartitionPatternSmokeTest extends SparkSmokeTestBase {

  @Test
  public void stripsPartitionFromDatasetPath(@TempDir Path tmp) throws Exception {
    Path partition = tmp.resolve("events/dt=2024-01-01");
    Files.createDirectories(partition);
    writeCsv(partition.resolve("part.csv"), "id,c\n1,x\n");

    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")).filePartitionRegexp("/dt=[^/]*"),
            spark ->
                spark
                    .read()
                    .option("header", "true")
                    .csv(partition.toString())
                    .write()
                    .mode(SaveMode.Overwrite)
                    .csv(tmp.resolve("out").toString()));

    Set<String> fileUrns = md.datasetUrnsOnPlatform("file");
    assertTrue(
        fileUrns.stream().anyMatch(u -> u.contains("/events,") && !u.contains("dt=")),
        "the partition (dt=) should be stripped from the file dataset path:\n" + md.raw);
  }
}
