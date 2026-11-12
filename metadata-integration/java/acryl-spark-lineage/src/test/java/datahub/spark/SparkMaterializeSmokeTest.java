package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * materialize: {@code metadata.dataset.materialize} emits standalone dataset entities (not just the
 * lineage edges between them).
 */
@Tag("integration")
public class SparkMaterializeSmokeTest extends SparkSmokeTestBase {

  @Test
  public void emitsStandaloneDatasetEntities(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("in.csv"), "id,c\n1,x\n");

    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")).materialize(true),
            spark ->
                spark
                    .read()
                    .option("header", "true")
                    .csv(in.toString())
                    .write()
                    .mode(SaveMode.Overwrite)
                    .csv(tmp.resolve("out").toString()));

    assertTrue(
        md.hasEntity("dataset"),
        "materialize=true should emit standalone dataset entities, not just edges:\n" + md.raw);
  }
}
