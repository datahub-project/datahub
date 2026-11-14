package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

/**
 * A Delta {@code saveAsTable} reports the catalog table it wrote. DataHub's former vendored {@code
 * SaveIntoDataSourceCommandVisitor} had branches for a Delta {@code saveAsTable} planned as {@code
 * SaveIntoDataSourceCommand}. Spark plans it as {@code CreateTableAsSelect} instead (Delta is a
 * {@code TableProvider}), so those branches never ran and upstream's handling produces the output.
 *
 * <p>Only the {@code DeltaCatalog} setup is covered: without it, Spark 3.5 rejects a Delta {@code
 * saveAsTable} at analysis time, so there is no write to report.
 */
@Tag("integration")
public class SparkDeltaSaveAsTableSmokeTest extends SparkSmokeTestBase {

  @Test
  public void saveAsTableReportsTheDeltaTable(@TempDir Path tmp) throws Exception {
    Path in = writeCsv(tmp.resolve("in.csv"), "id,v\n1,a\n2,b\n");
    Map<String, String> infra = new LinkedHashMap<>();
    infra.put("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
    infra.put("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
    infra.put("spark.sql.warehouse.dir", tmp.resolve("warehouse").toAbsolutePath().toString());
    Set<String> plannedNodes = Collections.synchronizedSet(new TreeSet<>());

    EmittedMetadata md =
        runJob(
            listener().emitToFile(tmp.resolve("mcps.json")),
            infra,
            spark -> {
              spark.listenerManager().register(new PlanNodeRecorder(plannedNodes));
              spark
                  .read()
                  .option("header", "true")
                  .csv(in.toString())
                  .write()
                  .format("delta")
                  .saveAsTable("delta_target");
            });

    System.out.println("analyzed plan nodes: " + plannedNodes);
    assertFalse(
        plannedNodes.contains("SaveIntoDataSourceCommand"),
        "saveAsTable is not expected to be planned as SaveIntoDataSourceCommand: " + plannedNodes);
    assertEquals(
        Collections.singleton(
            "urn:li:dataset:(urn:li:dataPlatform:hive,default.delta_target,PROD)"),
        md.outputDatasetUrns(),
        "saveAsTable should report the Delta table it wrote:\n" + md.raw);
  }

  /** Records the class name of every analyzed logical-plan node, commands' inner plans included. */
  private static final class PlanNodeRecorder implements QueryExecutionListener {
    private final Set<String> nodes;

    PlanNodeRecorder(Set<String> nodes) {
      this.nodes = nodes;
    }

    @Override
    public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
      record(qe.analyzed());
    }

    @Override
    public void onFailure(String funcName, QueryExecution qe, Exception exception) {
      record(qe.analyzed());
    }

    private void record(TreeNode<?> node) {
      nodes.add(node.getClass().getSimpleName());
      for (Object child : JavaConverters.seqAsJavaList(node.children())) {
        record((TreeNode<?>) child);
      }
      for (Object inner : JavaConverters.seqAsJavaList(node.innerChildren())) {
        record((TreeNode<?>) inner);
      }
    }
  }
}
