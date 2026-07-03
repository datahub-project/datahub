package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Domains: {@code spark.datahub.domains} attaches a domains aspect to the emitted DataFlow. */
@Tag("integration")
public class SparkDomainsSmokeTest extends SparkSmokeTestBase {

  @Test
  public void emitsDomains(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("in.csv"), "id,c\n1,x\n");

    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")).domains("urn:li:domain:finance"),
            spark ->
                spark
                    .read()
                    .option("header", "true")
                    .csv(in.toString())
                    .write()
                    .mode(SaveMode.Overwrite)
                    .csv(tmp.resolve("out").toString()));

    assertTrue(md.hasAspect("domains"), "expected a domains aspect:\n" + md.raw);
    assertTrue(md.contains("urn:li:domain:finance"), "expected the configured domain:\n" + md.raw);
  }
}
