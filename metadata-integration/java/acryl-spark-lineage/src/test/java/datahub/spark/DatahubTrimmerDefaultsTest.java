package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.dataset.DatasetConfig;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

/**
 * OpenLineage's built-in dataset-name trimmers collide with DataHub's PathSpec (both transform
 * names; the trimmer runs first). {@link DatahubSparkListener#configureDatasetTrimmers} therefore
 * disables the trimmers when a {@code path_spec_list} is configured, keeps them on otherwise, and
 * honors an explicit {@code openLineageTrimmersEnabled} override. These tests pin that policy and
 * verify the disabled-trimmer class names actually match OpenLineage's built-ins (so a future OL
 * rename fails here instead of silently re-enabling partition stripping). See ING-2959.
 */
public class DatahubTrimmerDefaultsTest {

  private static final String PATH_SPEC = "spark.datahub.s3.my_alias.path_spec_list";

  @Test
  public void keepsTrimmersOnByDefaultWithoutPathSpec() {
    SparkConf conf = new SparkConf();
    DatahubSparkListener.configureDatasetTrimmers(conf);
    assertFalse(
        conf.contains(DatahubSparkListener.OL_DISABLED_TRIMMERS_KEY),
        "without a path_spec, OpenLineage trimmers should stay enabled");
  }

  @Test
  public void disablesTrimmersWhenPathSpecConfigured() {
    SparkConf conf = new SparkConf().set(PATH_SPEC, "s3://bucket/{table}");
    DatahubSparkListener.configureDatasetTrimmers(conf);
    assertEquals(
        DatahubSparkListener.OL_DEFAULT_DISABLED_TRIMMERS,
        conf.get(DatahubSparkListener.OL_DISABLED_TRIMMERS_KEY));
  }

  @Test
  public void disablesTrimmersWhenFilePartitionRegexpConfigured() {
    SparkConf conf =
        new SparkConf().set(DatahubSparkListener.DATAHUB_FILE_PARTITION_REGEXP_KEY, "/dt=[^/]*");
    DatahubSparkListener.configureDatasetTrimmers(conf);
    assertEquals(
        DatahubSparkListener.OL_DEFAULT_DISABLED_TRIMMERS,
        conf.get(DatahubSparkListener.OL_DISABLED_TRIMMERS_KEY));
  }

  @Test
  public void explicitEnableKeepsTrimmersOnEvenWithPathSpec() {
    SparkConf conf =
        new SparkConf()
            .set(PATH_SPEC, "s3://bucket/{table}")
            .set(DatahubSparkListener.DATAHUB_ENABLE_TRIMMERS_KEY, "true");
    DatahubSparkListener.configureDatasetTrimmers(conf);
    assertFalse(
        conf.contains(DatahubSparkListener.OL_DISABLED_TRIMMERS_KEY),
        "explicit enable=true should keep trimmers on even when a path_spec is configured");
  }

  @Test
  public void explicitDisableTurnsTrimmersOffWithoutPathSpec() {
    SparkConf conf = new SparkConf().set(DatahubSparkListener.DATAHUB_ENABLE_TRIMMERS_KEY, "false");
    DatahubSparkListener.configureDatasetTrimmers(conf);
    assertEquals(
        DatahubSparkListener.OL_DEFAULT_DISABLED_TRIMMERS,
        conf.get(DatahubSparkListener.OL_DISABLED_TRIMMERS_KEY));
  }

  @Test
  public void respectsUserProvidedOpenLineageDisabledKey() {
    SparkConf conf =
        new SparkConf()
            .set(PATH_SPEC, "s3://bucket/{table}")
            .set(DatahubSparkListener.OL_DISABLED_TRIMMERS_KEY, "com.example.MyTrimmer");
    DatahubSparkListener.configureDatasetTrimmers(conf);
    assertEquals("com.example.MyTrimmer", conf.get(DatahubSparkListener.OL_DISABLED_TRIMMERS_KEY));
  }

  @Test
  public void defaultDisabledListDisablesAllBuiltInTrimmers() {
    // Control: OpenLineage ships built-in trimmers by default.
    assertTrue(
        DatasetConfig.defaultConfig().getDatasetNameTrimmers().size() > 0,
        "expected OpenLineage to ship built-in dataset-name trimmers");
    // Our disabled-list must actually disable every one (verifies the class names still match).
    DatasetConfig cfg = DatasetConfig.defaultConfig();
    cfg.setDisabledTrimmers(DatahubSparkListener.OL_DEFAULT_DISABLED_TRIMMERS);
    assertTrue(
        cfg.getDatasetNameTrimmers().isEmpty(),
        "DataHub's disabled list should disable every built-in OpenLineage trimmer");
  }
}
