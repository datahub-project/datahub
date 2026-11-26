package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A Delta MERGE whose {@code USING} clause is a join reports both joined tables, not just the
 * target. Before OpenLineage 1.55 (OpenLineage #5080), upstream's MERGE input builders only
 * resolved a source that was itself a table, so DataHub vendored {@code
 * MergeIntoCommandInputDatasetBuilder} and {@code MergeIntoCommandEdgeInputDatasetBuilder} to walk
 * the source plan. Upstream's {@code MergeSourceDatasetTraversal} now does the same walk, and this
 * test pins that dropping the vendored copies loses no input.
 *
 * <p>The tables are created and filled with {@code CREATE TABLE} and {@code INSERT ... VALUES},
 * which read no dataset, so every input in the run comes from the MERGE.
 */
@Tag("integration")
public class SparkDeltaMergeJoinSourceSmokeTest extends SparkSmokeTestBase {

  @Test
  public void mergeWithJoinSourceReportsBothJoinedTables(@TempDir Path tmp) throws Exception {
    Map<String, String> infra = new LinkedHashMap<>();
    infra.put("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
    infra.put("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
    infra.put("spark.sql.warehouse.dir", tmp.resolve("warehouse").toAbsolutePath().toString());

    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")),
            infra,
            spark -> {
              spark.sql("CREATE TABLE tgt (id INT, v STRING) USING delta");
              spark.sql("CREATE TABLE src_a (id INT) USING delta");
              spark.sql("CREATE TABLE src_b (id INT, v STRING) USING delta");
              spark.sql("INSERT INTO tgt VALUES (1, 'old')");
              spark.sql("INSERT INTO src_a VALUES (1), (2)");
              spark.sql("INSERT INTO src_b VALUES (1, 'new'), (2, 'added')");
              spark.sql(
                  "MERGE INTO tgt USING ("
                      + "SELECT src_a.id, src_b.v FROM src_a JOIN src_b ON src_a.id = src_b.id"
                      + ") s ON tgt.id = s.id "
                      + "WHEN MATCHED THEN UPDATE SET * "
                      + "WHEN NOT MATCHED THEN INSERT *");
            });

    assertEquals(
        new TreeSet<>(
            Arrays.asList(
                hiveUrn("default.src_a"), hiveUrn("default.src_b"), hiveUrn("default.tgt"))),
        new TreeSet<>(md.inputDatasetUrns()),
        "MERGE should report the target and both tables joined in its source:\n" + md.raw);
  }

  private static String hiveUrn(String name) {
    return "urn:li:dataset:(urn:li:dataPlatform:hive," + name + ",PROD)";
  }
}
