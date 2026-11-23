package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A file-based Structured Streaming source reports the files it reads as inputs. Each micro-batch
 * of a file stream is a {@code LogicalRelation} over a {@code HadoopFsRelation}, which upstream
 * OpenLineage handles without DataHub's former vendored {@code
 * StreamingDataSourceV2RelationVisitor} / {@code FileStreamMicroBatchStreamStrategy}.
 */
@Tag("integration")
public class SparkFileStreamSourceSmokeTest extends SparkSmokeTestBase {

  @Test
  public void csvFileStreamReportsItsInputFile(@TempDir Path tmp) throws Exception {
    Path in = Files.createDirectories(tmp.resolve("in"));
    Path csv = writeCsv(in.resolve("part1.csv"), "1,a\n2,b\n");

    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")),
            spark -> {
              try {
                spark
                    .readStream()
                    .schema("id INT, v STRING")
                    .csv(in.toString())
                    .writeStream()
                    .format("parquet")
                    .option("checkpointLocation", tmp.resolve("ckpt").toString())
                    .trigger(Trigger.AvailableNow())
                    .start(tmp.resolve("out").toString())
                    .awaitTermination();
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });

    assertEquals(
        Collections.singleton(
            "urn:li:dataset:(urn:li:dataPlatform:file," + csv.toAbsolutePath() + ",PROD)"),
        fileInputs(md),
        "the file stream should report the CSV it read:\n" + md.raw);
  }

  private static Set<String> fileInputs(EmittedMetadata md) {
    Set<String> out = new TreeSet<>();
    for (String urn : md.inputDatasetUrns()) {
      if (urn.contains("urn:li:dataPlatform:file,")) {
        out.add(urn);
      }
    }
    return out;
  }
}
