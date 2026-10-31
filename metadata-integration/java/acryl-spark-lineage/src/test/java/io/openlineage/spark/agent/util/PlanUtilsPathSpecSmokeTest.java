package io.openlineage.spark.agent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * DataHub's {@code PlanUtils} copy maps the file paths of a scan to their {@code path_spec} table
 * inside the agent, so the partition files of one table collapse into one input dataset. Upstream's
 * {@code getDirectoryPath} only turns a file into its parent directory, and returns the path
 * unchanged when it cannot reach the file system (here {@code s3://}, which has no file system on
 * this classpath). Needs a live {@code SparkEnv}, which is where the copy reads the {@code
 * spark.datahub.*} settings from, so it runs as a Spark smoke test.
 */
@Tag("integration")
public class PlanUtilsPathSpecSmokeTest {

  @Test
  public void directoryPathsCollapseToThePathSpecTable() {
    SparkSession spark =
        SparkSession.builder()
            .appName("plan-utils-path-spec")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.datahub.platform.s3.path_spec_list", "s3://bucket/{table}/*/*")
            .getOrCreate();
    try {
      List<Path> paths =
          PlanUtils.getDirectoryPaths(
              Arrays.asList(
                  new Path("s3://bucket/orders/dt=2024-01-01/part-0.parquet"),
                  new Path("s3://bucket/orders/dt=2024-01-02/part-0.parquet")),
              spark.sparkContext().hadoopConfiguration());

      assertEquals(Collections.singletonList(new Path("s3://bucket/orders")), paths);
    } finally {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }
}
