package com.linkedin.metadata.datahubusage.postgres;

import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchRequest;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Compiles the Elasticsearch audit-search contract onto {@code {prefix}_event}: backend-only rows,
 * the same default 1-day window, and a keyset cursor in place of {@code search_after}.
 */
final class PostgresAuditEventsSql {

  private static final int TOTAL_HIT_CAP = 10_000;
  private static final String CURSOR_SEP = "\u001f";

  private PostgresAuditEventsSql() {}

  @Value
  static class TimeRange {
    Instant start;
    Instant end;
  }

  @Value
  static class Cursor {
    Instant eventTime;
    String eventType;
    String actorUrn;
    String eventId;
  }

  @Value
  static class SqlPlan {
    String sql;
    List<Object> params;
  }

  static int totalHitCap() {
    return TOTAL_HIT_CAP;
  }

  @Nonnull
  static TimeRange resolveTimeRange(long startTime, long endTime) {
    Instant start =
        startTime < 0 ? Instant.now().minus(1, ChronoUnit.DAYS) : Instant.ofEpochMilli(startTime);
    Instant end = endTime <= 0 ? Instant.now() : Instant.ofEpochMilli(endTime);
    return new TimeRange(start, end);
  }

  @Nonnull
  static SqlPlan countPlan(@Nonnull String table, @Nonnull FilterBinds filters) {
    return new SqlPlan(
        "SELECT COUNT(*)::bigint FROM " + table + " WHERE " + filters.whereSql, filters.params);
  }

  @Nonnull
  static SqlPlan pagePlan(
      @Nonnull String table, @Nonnull FilterBinds filters, @Nullable Cursor cursor, int fetchSize) {
    StringBuilder sql =
        new StringBuilder(
            "SELECT document::text, event_time, COALESCE(event_type, ''), COALESCE(actor_urn, ''),"
                + " event_id FROM "
                + table
                + " WHERE "
                + filters.whereSql);
    List<Object> params = new ArrayList<>(filters.params);
    if (cursor != null) {
      sql.append(" AND (")
          .append("event_time < ?")
          .append(" OR (event_time = ? AND COALESCE(event_type, '') > ?)")
          .append(
              " OR (event_time = ? AND COALESCE(event_type, '') = ? AND COALESCE(actor_urn, '') > ?)")
          .append(
              " OR (event_time = ? AND COALESCE(event_type, '') = ? AND COALESCE(actor_urn, '') = ?"
                  + " AND event_id > ?)")
          .append(")");
      Timestamp ts = Timestamp.from(cursor.getEventTime());
      params.add(ts);
      params.add(ts);
      params.add(cursor.getEventType());
      params.add(ts);
      params.add(cursor.getEventType());
      params.add(cursor.getActorUrn());
      params.add(ts);
      params.add(cursor.getEventType());
      params.add(cursor.getActorUrn());
      params.add(cursor.getEventId());
    }
    sql.append(
        " ORDER BY event_time DESC, COALESCE(event_type, '') ASC, COALESCE(actor_urn, '') ASC,"
            + " event_id ASC LIMIT ?");
    params.add(fetchSize);
    return new SqlPlan(sql.toString(), params);
  }

  @Nonnull
  static FilterBinds filterBinds(
      @Nonnull TimeRange range, @Nonnull ExternalAuditEventsSearchRequest request) {
    List<Object> params = new ArrayList<>();
    StringBuilder where = new StringBuilder("metric_family = ? AND usage_source = ?");
    params.add(AnalyticsMetricFamilies.DATAHUB_USAGE);
    params.add(DataHubUsageEventConstants.BACKEND_SOURCE);
    where.append(" AND event_time >= ? AND event_time < ?");
    params.add(Timestamp.from(range.getStart()));
    params.add(Timestamp.from(range.getEnd()));
    appendAny(where, params, "event_type", request.getEventTypes());
    appendAny(where, params, "aspect_name", request.getAspectTypes());
    appendAny(where, params, "entity_type", request.getEntityTypes());
    appendAny(where, params, "actor_urn", request.getActorUrns());
    return new FilterBinds(where.toString(), params);
  }

  @Value
  static class FilterBinds {
    String whereSql;
    List<Object> params;
  }

  private static void appendAny(
      StringBuilder where, List<Object> params, String column, @Nullable List<String> values) {
    if (values == null || values.isEmpty()) {
      return;
    }
    where.append(" AND ").append(column).append(" = ANY(?)");
    params.add(values.toArray(String[]::new));
  }

  @Nonnull
  static String encodeCursor(@Nonnull Cursor cursor) {
    String payload =
        cursor.getEventTime().toEpochMilli()
            + CURSOR_SEP
            + nullToEmpty(cursor.getEventType())
            + CURSOR_SEP
            + nullToEmpty(cursor.getActorUrn())
            + CURSOR_SEP
            + nullToEmpty(cursor.getEventId());
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }

  @Nullable
  static Cursor decodeCursor(@Nullable String scrollId) {
    if (scrollId == null || scrollId.isBlank()) {
      return null;
    }
    String payload = new String(Base64.getUrlDecoder().decode(scrollId), StandardCharsets.UTF_8);
    String[] parts = payload.split(CURSOR_SEP, -1);
    if (parts.length != 4) {
      throw new IllegalArgumentException("Invalid audit-events scrollId");
    }
    return new Cursor(Instant.ofEpochMilli(Long.parseLong(parts[0])), parts[1], parts[2], parts[3]);
  }

  @Nonnull
  private static String nullToEmpty(@Nullable String value) {
    return value == null ? "" : value;
  }
}
