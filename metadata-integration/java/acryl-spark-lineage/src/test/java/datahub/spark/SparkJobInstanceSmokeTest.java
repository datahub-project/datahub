package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pipeline platform_instance: {@code metadata.pipeline.platformInstance} qualifies the pipeline's
 * DataFlow (its name is prefixed with the instance).
 */
@Tag("integration")
public class SparkJobInstanceSmokeTest extends SparkSmokeTestBase {

  @Test
  public void flowCarriesPipelinePlatformInstance(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("in.csv"), "id,c\n1,x\n");

    EmittedMetadata md =
        runJob(
            listener()
                .emitToFile(tmp.resolve("mcps.json"))
                .pipelinePlatformInstance("prod_pipelines"),
            spark ->
                spark
                    .read()
                    .option("header", "true")
                    .csv(in.toString())
                    .write()
                    .mode(SaveMode.Overwrite)
                    .csv(tmp.resolve("out").toString()));

    assertTrue(
        md.contains("urn:li:dataFlow:(spark,prod_pipelines"),
        "DataFlow URN should be qualified by the job platform_instance 'prod_pipelines':\n"
            + md.raw);
  }
}
