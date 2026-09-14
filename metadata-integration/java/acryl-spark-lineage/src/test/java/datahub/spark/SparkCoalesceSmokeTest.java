package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * coalesce_jobs (default ON): the application's multiple SQL executions are coalesced into a single
 * DataFlow/DataJob emitted at app end, aggregating their inputs/outputs — rather than one DataJob
 * per execution.
 */
@Tag("integration")
public class SparkCoalesceSmokeTest extends SparkSmokeTestBase {

  @Test
  public void coalescesExecutionsIntoOneJob(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("in.csv"), "id,c\n1,x\n");

    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")), // coalesce is on by default
            spark -> {
              Dataset<Row> df = spark.read().option("header", "true").csv(in.toString());
              df.write().mode(SaveMode.Overwrite).csv(tmp.resolve("out1").toString());
              df.write().mode(SaveMode.Overwrite).csv(tmp.resolve("out2").toString());
            });

    assertEquals(
        1,
        md.entityUrns("dataJob").size(),
        "the two write executions should coalesce into a single DataJob:\n" + md.raw);
    Set<String> outs = md.outputDatasetUrns();
    assertTrue(
        outs.stream().anyMatch(u -> u.contains("out1"))
            && outs.stream().anyMatch(u -> u.contains("out2")),
        "the coalesced job should carry both outputs:\n" + md.raw);
  }
}
