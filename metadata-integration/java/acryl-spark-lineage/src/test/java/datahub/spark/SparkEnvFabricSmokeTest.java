package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** env/fabric: {@code metadata.dataset.env} sets the fabric segment of emitted dataset URNs. */
@Tag("integration")
public class SparkEnvFabricSmokeTest extends SparkSmokeTestBase {

  @Test
  public void datasetUrnsCarryConfiguredEnv(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("in.csv"), "id,c\n1,x\n");

    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")).env("DEV"),
            spark ->
                spark
                    .read()
                    .option("header", "true")
                    .csv(in.toString())
                    .write()
                    .mode(SaveMode.Overwrite)
                    .csv(tmp.resolve("out").toString()));

    assertTrue(
        !md.datasetUrnsOnPlatform("file").isEmpty()
            && md.datasetUrnsOnPlatform("file").stream().allMatch(u -> u.endsWith(",DEV)")),
        "all file dataset URNs should carry the configured env DEV:\n" + md.raw);
  }
}
