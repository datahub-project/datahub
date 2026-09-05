package com.linkedin.metadata.analytics.postgres.compaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionRequest;
import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionResult;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import io.ebean.Database;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
    assertFalse(result.isFailed());
    verify(store, times(2)).materializeDatahubUsageHourlyFromRaw(any(Instant.class));
  }

  @Test
  public void compact_marksFailedWhenStoreThrowsSqlException() throws Exception {
    when(store.getLatestSealedHourStart(anyString())).thenReturn(null);
    when(store.getSealedThrough(anyString(), anyString(), anyString())).thenReturn(null);
    doThrow(new SQLException("seal failed"))
        .when(store)
        .materializeDatahubUsageHourlyFromRaw(any(Instant.class));

    AnalyticsCompactionResult result =
        compactor.compact(
            AnalyticsCompactionRequest.builder()
                .maxHoursToSeal(1)
                .maxDaysToCompact(0)
                .maxMonthsToCompact(0)
                .maxWallClockMillis(60_000L)
                .build());

    assertTrue(result.isFailed());
    assertTrue(result.isMoreWorkRemaining());
    assertNotNull(result.getMessage());
    assertTrue(result.getMessage().contains("seal failed"));
  }

  @Test
  public void compact_skipsAlreadySealedHours() throws Exception {
    Instant latestSealed = Instant.now().truncatedTo(ChronoUnit.HOURS).minus(2, ChronoUnit.HOURS);
    when(store.getSealedThrough(anyString(), anyString(), anyString())).thenReturn(Instant.EPOCH);
    when(store.getLatestSealedHourStart(anyString())).thenReturn(latestSealed);
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

  @Test
  public void compact_resealsHourWhenOnlyUsageFamilyWatermarked() throws Exception {
    when(store.getLatestSealedHourStart(anyString())).thenReturn(null);
    when(store.isDayFullySealed(anyString(), any())).thenReturn(false);
    when(store.getSealedThrough(anyString(), anyString(), anyString()))
        .thenAnswer(
            invocation -> {
              String family = invocation.getArgument(1);
              if ("datahub_usage".equals(family)) {
                return Instant.EPOCH;
              }
              return null;
            });

    AnalyticsCompactionResult result =
        compactor.compact(
            AnalyticsCompactionRequest.builder()
                .maxHoursToSeal(1)
                .maxDaysToCompact(0)
                .maxMonthsToCompact(0)
                .maxWallClockMillis(60_000L)
                .build());

    assertEquals(result.getHoursSealed(), 1);
    assertTrue(result.isMoreWorkRemaining());
    // Usage already sealed — rematerialize skipped; remaining family watermarks written.
    verify(store, never()).materializeDatahubUsageHourlyFromRaw(any(Instant.class));
    verify(store, times(2)).upsertWatermark(anyString(), anyString(), anyString(), any());
  }
}
