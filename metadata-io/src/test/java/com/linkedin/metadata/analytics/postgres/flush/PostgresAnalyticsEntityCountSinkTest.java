package com.linkedin.metadata.analytics.postgres.flush;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import io.ebean.Database;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class PostgresAnalyticsEntityCountSinkTest {

  @Test
  public void publish_replacesLatestRollupsAtomically() throws Exception {
    PostgresAnalyticsStore store = mock(PostgresAnalyticsStore.class);
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
            .entityCountEnabled(true)
            .poolMinConnections(1)
            .poolMaxConnections(12)
            .poolMaxInactiveTimeSeconds(120)
            .poolMaxAgeMinutes(120)
            .poolLeakTimeMinutes(15)
            .poolWaitTimeoutMillis(1000)
            .build();
    PgAnalyticsStoreRegistry.StoreHandle handle =
        new PgAnalyticsStoreRegistry.StoreHandle(options, mock(Database.class), store);
    PgAnalyticsStoreRegistry registry = mock(PgAnalyticsStoreRegistry.class);
    when(registry.resolve(AnalyticsMetricFamilies.SYSTEM_USAGE)).thenReturn(handle);

    Instant computedAt = Instant.parse("2024-06-15T10:30:00Z");
    KeyAspectEntityCountResult result =
        KeyAspectEntityCountResult.builder()
            .computedAt(computedAt)
            .requestedTypes(List.of("dataset", "chart"))
            .cacheHit(false)
            .counts(
                List.of(
                    KeyAspectEntityCountEntry.builder()
                        .entityType("dataset")
                        .keyAspect("datasetKey")
                        .activeCount(10)
                        .softDeletedCount(2)
                        .build(),
                    KeyAspectEntityCountEntry.builder()
                        .entityType("chart")
                        .keyAspect("chartKey")
                        .activeCount(3)
                        .softDeletedCount(0)
                        .build()))
            .build();

    new PostgresAnalyticsEntityCountSink(registry).publish(result);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PostgresAnalyticsStore.LatestRollupValue>> valuesCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(store)
        .replaceLatestRollups(
            eq(Instant.parse("2024-06-15T10:00:00Z")),
            eq(AnalyticsMetricFamilies.GRAIN_HOUR),
            eq(AnalyticsMetricFamilies.SYSTEM_USAGE),
            eq(
                List.of(
                    PostgresAnalyticsEntityCountSink.METRIC_ACTIVE,
                    PostgresAnalyticsEntityCountSink.METRIC_SOFT_DELETED)),
            valuesCaptor.capture());

    List<PostgresAnalyticsStore.LatestRollupValue> values = valuesCaptor.getValue();
    assertEquals(values.size(), 4);
    assertEquals(values.get(0).metricName(), PostgresAnalyticsEntityCountSink.METRIC_ACTIVE);
    assertEquals(values.get(0).groupDims(), Map.of("entity_type", "dataset"));
    assertEquals(values.get(0).value(), 10.0);
    assertEquals(values.get(1).metricName(), PostgresAnalyticsEntityCountSink.METRIC_SOFT_DELETED);
    assertEquals(values.get(1).value(), 2.0);
    assertEquals(values.get(2).groupDims(), Map.of("entity_type", "chart"));
    assertEquals(values.get(2).value(), 3.0);
    verify(store, org.mockito.Mockito.never())
        .upsertLatestRollup(
            any(), any(), any(), any(), any(), org.mockito.ArgumentMatchers.anyDouble());
  }
}
