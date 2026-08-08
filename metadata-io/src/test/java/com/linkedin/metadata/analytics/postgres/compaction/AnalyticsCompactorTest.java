package com.linkedin.metadata.analytics.postgres.compaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionRequest;
import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionResult;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import io.ebean.Database;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AnalyticsCompactorTest {

  private PostgresAnalyticsStore store;
  private AnalyticsCompactor compactor;

  @BeforeMethod
  public void setUp() {
    store = mock(PostgresAnalyticsStore.class);
    PgAnalyticsStoreOptions options =
        PgAnalyticsStoreOptions.builder()
            .name("default")
            .schema("public")
            .tablePrefix("metadata_analytics")
            .partmanPartitionInterval("1 day")
            .partmanPremake(4)
            .forceOverwritePartmanConfig(false)
            .rawMaxAgeSeconds(1)
            .hourlyMaxAgeSeconds(1)
            .dailyMaxAgeSeconds(1)
            .monthlyMaxAgeSeconds(1)
            .inputLagSeconds(0)
            .maintenanceCronEnabled(false)
            .maintenanceIntervalSeconds(3600)
            .apiUsageFlushEnabled(false)
            .entityCountEnabled(false)
            .poolMinConnections(1)
            .poolMaxConnections(12)
            .poolMaxInactiveTimeSeconds(120)
            .poolMaxAgeMinutes(120)
            .poolLeakTimeMinutes(15)
            .poolWaitTimeoutMillis(1000)
            .build();
    PgAnalyticsStoreRegistry.StoreHandle handle =
        new PgAnalyticsStoreRegistry.StoreHandle(options, mock(Database.class), store);
    Map<String, PgAnalyticsStoreRegistry.StoreHandle> stores = new LinkedHashMap<>();
    stores.put("default", handle);
    PgAnalyticsStoreRegistry registry = mock(PgAnalyticsStoreRegistry.class);
    when(registry.allStores()).thenReturn(stores);
    compactor = new AnalyticsCompactor(registry);
  }

  @Test
  public void compact_respectsMaxHoursBudget() throws Exception {
    when(store.getSealedThrough(anyString(), anyString(), anyString())).thenReturn(null);
    when(store.isDayFullySealed(anyString(), any())).thenReturn(false);

    AnalyticsCompactionResult result =
        compactor.compact(
            AnalyticsCompactionRequest.builder()
                .maxHoursToSeal(2)
                .maxDaysToCompact(0)
                .maxMonthsToCompact(0)
                .maxWallClockMillis(60_000L)
                .build());

    assertEquals(result.getHoursSealed(), 2);
    assertTrue(result.isMoreWorkRemaining());
    verify(store, times(2)).materializeDatahubUsageHourlyFromRaw(any(Instant.class));
  }

  @Test
  public void compact_skipsAlreadySealedHours() throws Exception {
    when(store.getSealedThrough(anyString(), anyString(), anyString())).thenReturn(Instant.EPOCH);
    when(store.isDayFullySealed(anyString(), any())).thenReturn(false);

    AnalyticsCompactionResult result =
        compactor.compact(
            AnalyticsCompactionRequest.builder()
                .maxHoursToSeal(6)
                .maxDaysToCompact(0)
                .maxMonthsToCompact(0)
                .maxWallClockMillis(60_000L)
                .build());

    assertEquals(result.getHoursSealed(), 0);
    assertFalse(result.isMoreWorkRemaining());
    verify(store, never()).materializeDatahubUsageHourlyFromRaw(any(Instant.class));
  }
}
