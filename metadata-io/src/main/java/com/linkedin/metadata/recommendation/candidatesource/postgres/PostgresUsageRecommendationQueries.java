package com.linkedin.metadata.recommendation.candidatesource.postgres;

import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.postgres.jdbc.PostgresPreparedBinder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Reads {@code datahub_usage} facts from pgAnalytics for home/search recommendation modules. */
@Slf4j
public final class PostgresUsageRecommendationQueries {

  private PostgresUsageRecommendationQueries() {}

  @Nonnull
  public static List<String> recentEntityUrns(
      @Nonnull PostgresAnalyticsStore store,
      @Nonnull String actorUrn,
      @Nonnull String eventType,
      int limit,
      int lookbackDays) {
    String sql =
        "SELECT entity_urn FROM "
            + store.qualifiedEventTable()
            + " WHERE metric_family = ? AND actor_urn = ? AND event_type = ?"
            + " AND entity_urn IS NOT NULL"
            + lookbackPredicate(lookbackDays)
            + " GROUP BY entity_urn ORDER BY MAX(event_time) DESC LIMIT ?";
    List<Object> params = new ArrayList<>();
    params.add(AnalyticsMetricFamilies.DATAHUB_USAGE);
    params.add(actorUrn);
    params.add(eventType);
    addLookbackParam(params, lookbackDays);
    params.add(limit);
    return queryStrings(store, sql, params);
  }

  @Nonnull
  public static List<String> mostViewedEntityUrns(
      @Nonnull PostgresAnalyticsStore store,
      @Nonnull String eventType,
      int limit,
      @Nullable List<String> actorPeers,
      int lookbackDays) {
    StringBuilder sql =
        new StringBuilder(
            "SELECT entity_urn FROM "
                + store.qualifiedEventTable()
                + " WHERE metric_family = ? AND event_type = ? AND entity_urn IS NOT NULL"
                + lookbackPredicate(lookbackDays));
    List<Object> params = new ArrayList<>();
    params.add(AnalyticsMetricFamilies.DATAHUB_USAGE);
    params.add(eventType);
    addLookbackParam(params, lookbackDays);
    if (actorPeers != null && !actorPeers.isEmpty()) {
      sql.append(" AND actor_urn = ANY(?)");
      params.add(actorPeers.toArray(String[]::new));
    }
    sql.append(" GROUP BY entity_urn ORDER BY COUNT(*) DESC LIMIT ?");
    params.add(limit);
    return queryStrings(store, sql.toString(), params);
  }

  @Nonnull
  public static List<String> recentSearchQueries(
      @Nonnull PostgresAnalyticsStore store,
      @Nonnull String actorUrn,
      int limit,
      int lookbackDays) {
    String sql =
        "SELECT query FROM "
            + store.qualifiedEventTable()
            + " WHERE metric_family = ? AND actor_urn = ? AND event_type = ?"
            + " AND query IS NOT NULL AND btrim(query) <> '' AND query <> '*'"
            + " AND COALESCE((document->>'total')::numeric, 0) > 0"
            + lookbackPredicate(lookbackDays)
            + " GROUP BY query ORDER BY MAX(event_time) DESC LIMIT ?";
    List<Object> params = new ArrayList<>();
    params.add(AnalyticsMetricFamilies.DATAHUB_USAGE);
    params.add(actorUrn);
    params.add(
        com.linkedin.metadata.datahubusage.DataHubUsageEventType.SEARCH_RESULTS_VIEW_EVENT
            .getType());
    addLookbackParam(params, lookbackDays);
    params.add(limit);
    return queryStrings(store, sql, params);
  }

  @Nonnull
  private static String lookbackPredicate(int lookbackDays) {
    return lookbackDays > 0 ? " AND event_time >= ?" : "";
  }

  private static void addLookbackParam(@Nonnull List<Object> params, int lookbackDays) {
    if (lookbackDays > 0) {
      params.add(
          OffsetDateTime.ofInstant(
              Instant.now().minus(lookbackDays, ChronoUnit.DAYS), ZoneOffset.UTC));
    }
  }

  /**
   * Ebean's pool logs "Tried to close a dirty connection" when a connection is returned with an
   * open transaction. These are single-statement reads, so autoCommit keeps the checkout clean.
   */
  @Nonnull
  private static Connection readConnection(@Nonnull PostgresAnalyticsStore store)
      throws SQLException {
    Connection c = store.getDatabase().dataSource().getConnection();
    c.setAutoCommit(true);
    return c;
  }

  @Nonnull
  private static List<String> queryStrings(
      @Nonnull PostgresAnalyticsStore store, @Nonnull String sql, @Nonnull List<Object> params) {
    List<String> out = new ArrayList<>();
    try (Connection c = readConnection(store);
        PreparedStatement ps = c.prepareStatement(sql)) {
      PostgresPreparedBinder.bind(ps, params);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String value = rs.getString(1);
          if (value != null && !value.isBlank()) {
            out.add(value);
          }
        }
      }
    } catch (SQLException e) {
      log.error("pgAnalytics usage recommendation query failed", e);
    }
    return out;
  }
}
