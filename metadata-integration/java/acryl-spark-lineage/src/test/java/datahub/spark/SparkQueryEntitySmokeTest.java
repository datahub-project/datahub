package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Query entity emission: a job whose SQL is captured via an OpenLineage SQLJobFacet emits a Query
 * entity and links it as the `query` on the corresponding fine-grained lineage entries.
 *
 * <p>The DataFrame-only negative case ({@code no SQLJobFacet -> no query entity}) lives in its own
 * {@link SparkQueryEntityDataFrameOnlySmokeTest} class, per {@link SparkSmokeTestBase}'s one-class
 * one-Spark-job rule.
 */
@Tag("integration")
public class SparkQueryEntitySmokeTest extends SparkSmokeTestBase {

  @Test
  public void emitsQueryEntityForSparkSqlJob(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("people.csv"), "age,name\n30,Alice\n10,Bob\n");
    Path out = tmp.resolve("adults");

    EmittedMetadata md =
        runJob(
            listener()
                .emitToFile(tmp.resolve("mcps.json"))
                .captureColumnLevelLineage(true)
                .captureQueryEntities(true),
            spark -> {
              Dataset<Row> df =
                  spark
                      .read()
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .csv(in.toString());
              df.createOrReplaceTempView("people");
              spark
                  .sql("SELECT age, name FROM people WHERE age > 18")
                  .write()
                  .mode(SaveMode.Overwrite)
                  .csv(out.toString());
            });

    assertTrue(md.hasEntity("query"), "expected a query entity to be emitted:\n" + md.raw);

    JsonNode fineGrainedLineages =
        md.aspect("dataJobInputOutput").map(io -> io.get("fineGrainedLineages")).orElse(null);
    assertTrue(
        fineGrainedLineages != null
            && fineGrainedLineages.isArray()
            && anyHasNonNullQuery(fineGrainedLineages),
        "expected a fineGrainedLineages entry with a non-null query field:\n" + md.raw);
  }

  private static boolean anyHasNonNullQuery(JsonNode fineGrainedLineages) {
    for (JsonNode fgl : fineGrainedLineages) {
      JsonNode query = fgl.get("query");
      if (query != null && !query.isNull()) {
        return true;
      }
    }
    return false;
  }
}
