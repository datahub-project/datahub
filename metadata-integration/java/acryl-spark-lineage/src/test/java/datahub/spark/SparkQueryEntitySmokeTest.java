package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.datahubproject.openlineage.utils.QueryUrnUtils;
import java.nio.file.Path;
import java.util.Set;
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

  private static final String SQL = "SELECT age, name FROM people WHERE age > 18";

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
              spark.sql(SQL).write().mode(SaveMode.Overwrite).csv(out.toString());
            });

    Set<String> queryUrns = md.entityUrns("query");
    assertEquals(1, queryUrns.size(), "expected exactly one query entity:\n" + md.raw);
    String emittedQueryUrn = queryUrns.iterator().next();

    String expectedQueryUrn = QueryUrnUtils.queryUrnForStatement(SQL).toString();
    assertEquals(
        expectedQueryUrn,
        emittedQueryUrn,
        "emitted query entity's URN must match the fingerprint of the captured SQL:\n" + md.raw);

    JsonNode fineGrainedLineages =
        md.aspect("dataJobInputOutput").map(io -> io.get("fineGrainedLineages")).orElse(null);
    assertEquals(
        emittedQueryUrn,
        firstNonNullQuery(fineGrainedLineages),
        "fine-grained lineage's query field must reference the emitted query entity, not some"
            + " other URN:\n"
            + md.raw);

    // The statement must be the bare SQL. transformOperation carries the same SQL prefixed with
    // OpenLineage transformation tags ("-- DIRECT:TRANSFORMATION\n..."); hashing or storing that
    // prefixed form would break URN parity with the Python SDK and show tags in the Logic panel.
    String statement =
        md.aspect("queryProperties")
            .map(qp -> qp.path("statement").path("value").asText(null))
            .orElse(null);
    assertEquals(
        SQL,
        statement,
        "query entity statement must be the bare SQL, not the tag-prefixed transformOperation:\n"
            + md.raw);
  }

  private static String firstNonNullQuery(JsonNode fineGrainedLineages) {
    if (fineGrainedLineages == null || !fineGrainedLineages.isArray()) {
      return null;
    }
    for (JsonNode fgl : fineGrainedLineages) {
      JsonNode query = fgl.get("query");
      if (query != null && !query.isNull()) {
        return query.asText();
      }
    }
    return null;
  }
}
