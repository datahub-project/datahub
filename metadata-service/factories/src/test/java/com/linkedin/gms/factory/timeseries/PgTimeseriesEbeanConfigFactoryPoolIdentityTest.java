package com.linkedin.gms.factory.timeseries;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PgTimeseriesStoreOptions;
import com.linkedin.metadata.timeseries.postgres.PgTimeseriesStoreRegistry;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectDao;
import io.ebean.Database;
import org.testng.annotations.Test;

public class PgTimeseriesEbeanConfigFactoryPoolIdentityTest {

  @Test
  public void poolIdentityEquals_treatsExplicitEbeanMatchingCredsAsSameAsInherited() {
    PgTimeseriesEbeanConfigFactory factory = new PgTimeseriesEbeanConfigFactory();
    // Mirror @Value defaults used by blank→ebean fallback in buildDataSourceConfig.
    setField(factory, "ebeanDriver", "org.postgresql.Driver");
    setField(factory, "ebeanUsername", "datahub");
    setField(factory, "ebeanPassword", "datahub");

    PgTimeseriesStoreOptions explicit =
        baseStore("a").toBuilder().poolUsername("datahub").poolPassword("datahub").build();
    PgTimeseriesStoreOptions inherited = baseStore("b"); // null username/password → ebean fallback

    assertTrue(factory.poolIdentityEquals(explicit, inherited));
  }

  @Test
  public void poolIdentityEquals_rejectsDifferentEffectivePasswords() {
    PgTimeseriesEbeanConfigFactory factory = new PgTimeseriesEbeanConfigFactory();
    setField(factory, "ebeanDriver", "org.postgresql.Driver");
    setField(factory, "ebeanUsername", "datahub");
    setField(factory, "ebeanPassword", "datahub");

    PgTimeseriesStoreOptions a =
        baseStore("a").toBuilder().poolUsername("datahub").poolPassword("other").build();
    PgTimeseriesStoreOptions b = baseStore("b");

    assertFalse(factory.poolIdentityEquals(a, b));
  }

  @Test
  public void shutdownDatabases_shutsDownEachDistinctDatabaseOnce() {
    Database shared = mock(Database.class);
    Database other = mock(Database.class);
    PgTimeseriesEbeanConfigFactory.shutdownDatabases(java.util.List.of(shared, shared, other));
    verify(shared, times(1)).shutdown();
    verify(other, times(1)).shutdown();
  }

  @Test
  public void shutdownRegistry_shutsDownEachDistinctDatabaseOnce() {
    PgTimeseriesStoreOptions a = baseStore("a");
    PgTimeseriesStoreOptions b = baseStore("b").toBuilder().poolUrl(a.getPoolUrl()).build();
    Database shared = mock(Database.class);
    Database other = mock(Database.class);
    PgTimeseriesStoreRegistry registry =
        new PgTimeseriesStoreRegistry(
            new com.linkedin.metadata.config.postgres.PgTimeseriesSetupOptions(
                "a", java.util.Map.of("a", a, "b", b, "c", baseStore("c")), java.util.Map.of()),
            java.util.Map.of(
                "a",
                new PgTimeseriesStoreRegistry.StoreHandle(
                    a, shared, new PostgresTimeseriesAspectDao(shared, a)),
                "b",
                new PgTimeseriesStoreRegistry.StoreHandle(
                    b, shared, new PostgresTimeseriesAspectDao(shared, b)),
                "c",
                new PgTimeseriesStoreRegistry.StoreHandle(
                    baseStore("c"),
                    other,
                    new PostgresTimeseriesAspectDao(other, baseStore("c")))));
    PgTimeseriesEbeanConfigFactory.shutdownRegistry(registry);
    verify(shared, times(1)).shutdown();
    verify(other, times(1)).shutdown();
  }

  private static PgTimeseriesStoreOptions baseStore(String name) {
    return PgTimeseriesStoreOptions.builder()
        .name(name)
        .schema("public")
        .tablePrefix("metadata_timeseries_" + name)
        .partmanPartitionInterval("1 day")
        .partmanPremake(4)
        .retentionMaxAgeSeconds(7776000)
        .maintenanceIntervalSeconds(3600)
        .poolUrl("jdbc:postgresql://localhost:5432/datahub")
        .poolMinConnections(1)
        .poolMaxConnections(12)
        .poolMaxInactiveTimeSeconds(120)
        .poolMaxAgeMinutes(120)
        .poolLeakTimeMinutes(15)
        .poolWaitTimeoutMillis(1000)
        .build();
  }

  private static void setField(Object target, String name, Object value) {
    try {
      java.lang.reflect.Field f = PgTimeseriesEbeanConfigFactory.class.getDeclaredField(name);
      f.setAccessible(true);
      f.set(target, value);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
