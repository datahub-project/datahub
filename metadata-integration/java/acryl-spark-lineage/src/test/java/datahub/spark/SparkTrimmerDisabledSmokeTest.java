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
 * Regression guard for the OpenLineage-trimmer vs DataHub-path-trimming collision (ING-2959).
 *
 * <p>OpenLineage's built-in dataset-name trimmers (on by default since OL 1.39) would pre-strip
 * partition segments before DataHub's own path handling runs. When a DataHub path-trimming
 * mechanism is configured (here {@code file_partition_regexp}), the plugin disables the OL trimmers
 * so they can't interfere.
 *
 * <p>Why the existing partition test didn't catch the collision: it uses a regex that strips the
 * same {@code dt=} segment the OL trimmer would, so both converge to the same URN whether the
 * trimmers are on or off. This test instead uses a regex that <b>does not</b> match {@code dt=}, so
 * DataHub does not strip it — meaning {@code dt=} survives in the URN <i>only</i> if the OL
 * trimmers are disabled. If they were (re-)enabled, OpenLineage's {@code KeyValueTrimmer} would
 * strip {@code dt=} and this assertion would fail.
 */
@Tag("integration")
public class SparkTrimmerDisabledSmokeTest extends SparkSmokeTestBase {

  @Test
  public void datahubPathTrimmingDisablesOpenLineageTrimmers(@TempDir Path tmp) throws Exception {
    Path partition = tmp.resolve("events/dt=2024-01-01");
    Files.createDirectories(partition);
    writeCsv(partition.resolve("part.csv"), "id,c\n1,x\n");

    EmittedMetadata md =
        runJob(
            // Deliberately non-matching regex: DataHub won't strip dt=, but configuring
            // file_partition_regexp still signals DataHub owns path trimming, so the OL trimmers
            // must be disabled.
            listener().emitToFile(tmp.resolve("mcps.json")).filePartitionRegexp("/nomatch=[^/]*"),
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
        fileUrns.stream().anyMatch(u -> u.contains("dt=2024-01-01")),
        "dt= should survive: OpenLineage trimmers must be disabled when DataHub path trimming is "
            + "configured. If this fails, the OL trimmers were left enabled and stripped dt=:\n"
            + md.raw);
  }
}
