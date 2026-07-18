package com.linkedin.metadata.datahubusage.postgres;

import com.datahub.util.exception.ESQueryException;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.datahubusage.RecentSearchRecommendationAccess;
import io.datahubproject.metadata.context.OperationContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Recent search-bar queries from partitioned PostgreSQL usage events ({@link
 * PostgresUsageEventsStore}).
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresRecentSearchRecommendationAccess implements RecentSearchRecommendationAccess {

  private static final String EXCLUDE_BACKEND_USAGE =
      " ("
          + PostgresUsageEventsStore.COL_USAGE_SOURCE
          + " IS NULL OR "
          + PostgresUsageEventsStore.COL_USAGE_SOURCE
          + " <> 'backend') ";

  /** Upper bound on distinct queries fetched before filtering in the recommendation source. */
  private static final int QUERY_FETCH_LIMIT = 10;

  private final PostgresUsageEventsStore store;
  private final int recommendationLookbackDays;

  private long lookbackCutoffMillis() {
    long daysMs = Math.max(1L, recommendationLookbackDays) * 86_400_000L;
    return System.currentTimeMillis() - daysMs;
  }

  @Override
  public boolean isDataAvailable(@Nonnull OperationContext opContext) {
    try {
      return store.hasAnyPartition();
    } catch (SQLException e) {
      log.warn("PostgreSQL recent-search availability probe failed: {}", e.toString());
      return false;
    }
  }

  @Override
  @Nonnull
  public List<String> recentSearchQueries(@Nonnull OperationContext opContext) {
    String actorUrn = opContext.getSessionActorContext().getActorUrn().toString();

    String sql =
        "SELECT "
            + PostgresUsageEventsStore.COL_QUERY
            + ", MAX("
            + PostgresUsageEventsStore.COL_TS
            + ") AS last_ts FROM "
            + store.qualifiedParentTable()
            + " WHERE "
            + PostgresUsageEventsStore.COL_TS
            + ">=?"
            + " AND "
            + EXCLUDE_BACKEND_USAGE
            + " AND "
            + PostgresUsageEventsStore.COL_ACTOR_URN
            + "=?"
            + " AND "
            + PostgresUsageEventsStore.COL_EVENT_TYPE
            + "=?"
            + " AND "
            + PostgresUsageEventsStore.COL_QUERY
            + " IS NOT NULL AND TRIM("
            + PostgresUsageEventsStore.COL_QUERY
            + ") <> '' "
            // Mirror OpenSearch: total exists and &gt; 0 (stored in document JSON).
            + " AND ("
            + PostgresUsageEventsStore.COL_DOCUMENT
            + " ->> 'total') IS NOT NULL "
            + " AND ("
            + PostgresUsageEventsStore.COL_DOCUMENT
            + " ->> 'total') ~ '^-?[0-9]+(\\.[0-9]+)?$' "
            + " AND ("
            + PostgresUsageEventsStore.COL_DOCUMENT
            + " ->> 'total')::double precision > 0 "
            + " GROUP BY "
            + PostgresUsageEventsStore.COL_QUERY
            + " ORDER BY last_ts DESC LIMIT ?";

    List<String> ordered = new ArrayList<>();
    try (Connection c = store.getDatabase().dataSource().getConnection()) {
      c.setReadOnly(true);
      try (PreparedStatement ps = c.prepareStatement(sql)) {
        int i = 1;
        ps.setLong(i++, lookbackCutoffMillis());
        ps.setString(i++, actorUrn);
        ps.setString(i++, DataHubUsageEventType.SEARCH_RESULTS_VIEW_EVENT.getType());
        ps.setInt(i, QUERY_FETCH_LIMIT);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            ordered.add(rs.getString(1));
          }
        }
      }
    } catch (SQLException e) {
      log.error("PostgreSQL query to get recently searched queries failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
    return ordered;
  }
}
