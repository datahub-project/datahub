package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tags: {@code spark.datahub.tags} attaches a globalTags aspect to the emitted DataFlow. */
@Tag("integration")
public class SparkTagsSmokeTest extends SparkSmokeTestBase {

  @Test
  public void emitsGlobalTags(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("in.csv"), "id,c\n1,x\n");

    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")).tags("pii", "gold"),
            spark ->
                spark
                    .read()
                    .option("header", "true")
                    .csv(in.toString())
                    .write()
                    .mode(SaveMode.Overwrite)
                    .csv(tmp.resolve("out").toString()));

    assertTrue(md.hasAspect("globalTags"), "expected a globalTags aspect:\n" + md.raw);
    assertTrue(md.contains("pii") && md.contains("gold"), "expected both tags:\n" + md.raw);
  }
}
