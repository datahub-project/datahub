package com.linkedin.metadata.timeseries.postgres;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.config.postgres.PgTimeseriesSetupOptions;
import com.linkedin.metadata.config.postgres.PgTimeseriesStoreOptions;
import io.ebean.Database;
import java.util.Map;
import org.testng.annotations.Test;

public class PgTimeseriesStoreRegistryTest {

  @Test
  public void resolve_routesToNamedStoreDao() {
    PgTimeseriesStoreOptions def = store("default", "ts_default");
    PgTimeseriesStoreOptions lng = store("long", "ts_long");
    Database dbDefault = mock(Database.class);
    Database dbLong = mock(Database.class);
    PostgresTimeseriesAspectDao daoDefault = new PostgresTimeseriesAspectDao(dbDefault, def);
    PostgresTimeseriesAspectDao daoLong = new PostgresTimeseriesAspectDao(dbLong, lng);

    PgTimeseriesStoreRegistry registry =
        new PgTimeseriesStoreRegistry(
            new PgTimeseriesSetupOptions(
                "default",
                Map.of("default", def, "long", lng),
                Map.of("dataset.datasetprofile", "long")),
            Map.of(
                "default", new PgTimeseriesStoreRegistry.StoreHandle(def, dbDefault, daoDefault),
                "long", new PgTimeseriesStoreRegistry.StoreHandle(lng, dbLong, daoLong)));

    assertSame(registry.resolve("dataset", "datasetProfile").getDao(), daoLong);
    assertEquals(
        registry.resolve("dataset", "datasetProfile").getDao().qualifiedTable(),
        "public.ts_long_aspect");
    assertSame(registry.resolve("dataset", "operation").getDao(), daoDefault);
    assertSame(registry.getDefault().getDao(), daoDefault);
  }

  @Test
  public void constructor_requiresDefaultStoreHandle() {
    PgTimeseriesStoreOptions def = store("default", "ts_default");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PgTimeseriesStoreRegistry(
                new PgTimeseriesSetupOptions("default", Map.of("default", def), Map.of()),
                Map.of()));
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
