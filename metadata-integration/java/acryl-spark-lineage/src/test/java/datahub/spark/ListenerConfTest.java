package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Fast, Docker-free unit test for the listener-config builder used by the real-Spark smoke tests.
 * Verifies the typed methods map to the correct {@code spark.datahub.*} keys and that {@code
 * describe()} renders the on/off state a reader needs to understand a test's configuration.
 */
class ListenerConfTest {

  @Test
  void typedFlagsMapToSparkDatahubKeys() {
    Map<String, String> conf =
        listener()
            .captureColumnLevelLineage(true)
            .platformInstance("prod_a")
            .connection("postgres://h:5432", "warehouse_a", "PROD")
            .toSparkConf();

    assertEquals("true", conf.get("spark.datahub.metadata.dataset.captureColumnLevelLineage"));
    assertEquals("prod_a", conf.get("spark.datahub.metadata.dataset.platformInstance"));
    assertEquals(
        "warehouse_a",
        conf.get(
            "spark.datahub.metadata.dataset.connections.\"postgres://h:5432\".platformInstance"));
    assertEquals(
        "PROD", conf.get("spark.datahub.metadata.dataset.connections.\"postgres://h:5432\".env"));
  }

  @Test
  void describeShowsOffByDefault() {
    String desc = listener().describe();
    assertTrue(desc.contains("captureColumnLevelLineage"), desc);
    assertTrue(desc.contains("OFF"), desc);
  }

  @Test
  void describeShowsOnStateAndConnections() {
    String desc =
        listener()
            .captureColumnLevelLineage(true)
            .connection("postgres://h:5432", "warehouse_a", "PROD")
            .describe();
    assertTrue(desc.contains("ON"), desc);
    assertTrue(desc.contains("postgres://h:5432"), desc);
    assertTrue(desc.contains("warehouse_a"), desc);
  }

  @Test
  void escapeHatchSetsArbitraryKey() {
    Map<String, String> conf =
        listener().set("spark.datahub.some.future.flag", "bar").toSparkConf();
    assertEquals("bar", conf.get("spark.datahub.some.future.flag"));
  }

  @Test
  void emitFileIsTrackedAndExposed() {
    ListenerConf conf = listener().emitToFile(java.nio.file.Paths.get("/tmp/x.json"));
    assertEquals("file", conf.toSparkConf().get("spark.datahub.emitter"));
    assertEquals("/tmp/x.json", conf.toSparkConf().get("spark.datahub.file.filename"));
    assertEquals(java.nio.file.Paths.get("/tmp/x.json"), conf.emitFile());
  }

  @Test
  void unsetEmitFileIsNull() {
    assertNull(listener().emitFile());
  }
}
