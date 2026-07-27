package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** lowerCaseUrns: {@code metadata.dataset.lowerCaseUrns} lower-cases emitted dataset URNs. */
@Tag("integration")
public class SparkLowerCaseUrnsSmokeTest extends SparkSmokeTestBase {

  @Test
  public void datasetUrnsAreLowerCased(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("in.csv"), "id,c\n1,x\n");
    Path mixedCaseOut = tmp.resolve("MixedCaseOut");

    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")).lowerCaseUrns(true),
            spark ->
                spark
                    .read()
                    .option("header", "true")
                    .csv(in.toString())
                    .write()
                    .mode(SaveMode.Overwrite)
                    .csv(mixedCaseOut.toString()));

    assertTrue(
        md.datasetUrnsOnPlatform("file").stream().anyMatch(u -> u.contains("mixedcaseout")),
        "expected the mixed-case output path lower-cased in the dataset URN:\n" + md.raw);
    assertTrue(
        md.datasetUrnsOnPlatform("file").stream().noneMatch(u -> u.contains("MixedCaseOut")),
        "no dataset URN should retain the mixed-case path:\n" + md.raw);
  }
}
