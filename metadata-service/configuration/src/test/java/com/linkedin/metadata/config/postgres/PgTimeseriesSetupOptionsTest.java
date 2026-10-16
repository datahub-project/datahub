package com.linkedin.metadata.config.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import java.util.Map;
import org.testng.annotations.Test;

public class PgTimeseriesSetupOptionsTest {

  @Test
  public void resolveStore_unlistedUsesDefault() {
    PgTimeseriesSetupOptions options = sampleRegistry();
    assertEquals(options.resolveStore("dataset", "operation").getName(), "default");
  }

  @Test
  public void resolveStore_listedUsesNamedStore_caseInsensitive() {
    PgTimeseriesSetupOptions options = sampleRegistry();
    assertEquals(options.resolveStore("Dataset", "datasetProfile").getName(), "long");
    assertEquals(options.resolveStore("dataset", "DATASETPROFILE").getTablePrefix(), "ts_long");
  }

  @Test
  public void resolveStore_unknownTargetThrows() {
    PgTimeseriesStoreOptions onlyDefault = store("default", "ts_default");
    PgTimeseriesSetupOptions options =
        new PgTimeseriesSetupOptions(
            "default", Map.of("default", onlyDefault), Map.of("dataset.datasetprofile", "missing"));
    assertThrows(
        IllegalStateException.class, () -> options.resolveStore("dataset", "datasetProfile"));
  }

  @Test
  public void routingKey_normalizes() {
    assertEquals(PgTimeseriesSetupOptions.routingKey(" Dataset ", " Profile "), "dataset.profile");
  }

  private static PgTimeseriesSetupOptions sampleRegistry() {
    return new PgTimeseriesSetupOptions(
        "default",
        Map.of(
            "default", store("default", "ts_default"),
            "long", store("long", "ts_long")),
        Map.of("dataset.datasetprofile", "long"));
  }

  private static PgTimeseriesStoreOptions store(String name, String prefix) {
    return PgTimeseriesStoreOptions.builder()
        .name(name)
        .schema("public")
        .tablePrefix(prefix)
        .partmanPartitionInterval("1 day")
        .partmanPremake(4)
        .retentionMaxAgeSeconds(7776000)
        .maintenanceIntervalSeconds(3600)
        .poolMinConnections(1)
        .poolMaxConnections(12)
        .poolMaxInactiveTimeSeconds(120)
        .poolMaxAgeMinutes(120)
        .poolLeakTimeMinutes(15)
        .poolWaitTimeoutMillis(1000)
        .build();
  }
}
