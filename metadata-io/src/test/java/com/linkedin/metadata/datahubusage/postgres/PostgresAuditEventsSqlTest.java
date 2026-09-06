package com.linkedin.metadata.datahubusage.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchRequest;
import com.linkedin.metadata.datahubusage.postgres.PostgresAuditEventsSql.Cursor;
import com.linkedin.metadata.datahubusage.postgres.PostgresAuditEventsSql.FilterBinds;
import com.linkedin.metadata.datahubusage.postgres.PostgresAuditEventsSql.SqlPlan;
import com.linkedin.metadata.datahubusage.postgres.PostgresAuditEventsSql.TimeRange;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.testng.annotations.Test;

public class PostgresAuditEventsSqlTest {

  @Test
  public void defaultWindowIsLastDayWhenStartIsNegative() {
    long before = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
    TimeRange range = PostgresAuditEventsSql.resolveTimeRange(-1L, -1L);
    long after = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
    assertTrue(range.getStart().toEpochMilli() >= before - 50);
    assertTrue(range.getStart().toEpochMilli() <= after + 50);
    assertTrue(range.getEnd().isAfter(range.getStart()));
  }

  @Test
  public void filtersAlwaysRestrictToBackendDatahubUsage() {
    TimeRange range =
        new TimeRange(Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-02T00:00:00Z"));
    FilterBinds binds =
        PostgresAuditEventsSql.filterBinds(
            range, ExternalAuditEventsSearchRequest.builder().build());
    assertTrue(binds.getWhereSql().contains("metric_family = ?"));
    assertTrue(binds.getWhereSql().contains("usage_source = ?"));
    assertEquals(binds.getParams().get(0), AnalyticsMetricFamilies.DATAHUB_USAGE);
    assertEquals(binds.getParams().get(1), DataHubUsageEventConstants.BACKEND_SOURCE);
    assertFalse(binds.getWhereSql().contains("ANY(?)"));
  }

  @Test
  public void optionalTermFiltersBindAsTextArrays() {
    TimeRange range =
        new TimeRange(Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-02T00:00:00Z"));
    FilterBinds binds =
        PostgresAuditEventsSql.filterBinds(
            range,
            ExternalAuditEventsSearchRequest.builder()
                .eventTypes(List.of("LogInEvent"))
                .aspectTypes(List.of("ownership"))
                .entityTypes(List.of("dataset"))
                .actorUrns(List.of("urn:li:corpuser:datahub"))
                .build());
    assertTrue(binds.getWhereSql().contains("event_type = ANY(?)"));
    assertTrue(binds.getWhereSql().contains("aspect_name = ANY(?)"));
    assertTrue(binds.getWhereSql().contains("entity_type = ANY(?)"));
    assertTrue(binds.getWhereSql().contains("actor_urn = ANY(?)"));
  }

  @Test
  public void pagePlanAddsKeysetWhenCursorPresent() {
    TimeRange range =
        new TimeRange(Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-02T00:00:00Z"));
    FilterBinds binds =
        PostgresAuditEventsSql.filterBinds(
            range, ExternalAuditEventsSearchRequest.builder().build());
    Cursor cursor =
        new Cursor(
            Instant.parse("2026-01-01T12:00:00Z"),
            "LogInEvent",
            "urn:li:corpuser:datahub",
            "evt-1");
    SqlPlan plan =
        PostgresAuditEventsSql.pagePlan("public.metadata_analytics_event", binds, cursor, 11);
    assertTrue(plan.getSql().contains("event_time < ?"));
    assertTrue(plan.getSql().contains("ORDER BY event_time DESC"));
    assertEquals(plan.getParams().get(plan.getParams().size() - 1), 11);
    assertTrue(plan.getParams().contains(Timestamp.from(cursor.getEventTime())));
  }

  @Test
  public void cursorRoundTrip() {
    Cursor original =
        new Cursor(
            Instant.ofEpochMilli(1_700_000_000_000L), "LogInEvent", "urn:li:corpuser:a", "id");
    String encoded = PostgresAuditEventsSql.encodeCursor(original);
    Cursor decoded = PostgresAuditEventsSql.decodeCursor(encoded);
    assertEquals(decoded, original);
    assertNull(PostgresAuditEventsSql.decodeCursor(null));
    assertNull(PostgresAuditEventsSql.decodeCursor(" "));
  }
}
