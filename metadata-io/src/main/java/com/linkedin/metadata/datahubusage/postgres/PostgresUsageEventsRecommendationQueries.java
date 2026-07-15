package com.linkedin.metadata.datahubusage.postgres;

import com.datahub.util.exception.ESQueryException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.datahubusage.UsageEventsRecommendationDataAccess;
import com.linkedin.metadata.datahubusage.UsageEventsRecommendationPeerActors;
import io.datahubproject.metadata.context.OperationContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Home-page recommendations sourced from partitioned usage events, bounded by {@link
 * com.linkedin.metadata.config.UsageEventsConfiguration#getRecommendationLookbackDays()}.
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresUsageEventsRecommendationQueries
    implements UsageEventsRecommendationDataAccess {

  private static final int DEFAULT_LIMIT = 10;

  /**
   * Mirrors product analytics default: exclude backend-derived events from user-facing
   * recommendations.
   */
  private static final String EXCLUDE_BACKEND_USAGE =
      " (usage_source IS NULL OR usage_source <> 'backend') ";

  private final PostgresUsageEventsStore store;

  private final int recommendationLookbackDays;

  private long lookbackCutoffMillis() {

    long daysMs = Math.max(1L, recommendationLookbackDays) * 86_400_000L;

    return System.currentTimeMillis() - daysMs;
  }

  /** True when at least one child partition exists (events may still be empty). */
  public boolean isRecommendationDataAvailable() {
    try {

      return store.hasAnyPartition();

    } catch (Exception e) {

      log.warn("usage-events reco availability probe failed: {}", e.toString());

      return false;
    }
  }

  @Override
  public boolean isDataAvailable(@Nonnull OperationContext opContext) {
    return isRecommendationDataAvailable();
  }

  @Override
  @Nonnull
  public List<String> recentlyViewedEntityUrns(@Nonnull OperationContext opContext) {
    try {
      return new ArrayList<>(
          entityUrnsOrderedByMaxTsForUser(
                  opContext.getSessionActorContext().getActorUrn(),
                  DataHubUsageEventType.ENTITY_VIEW_EVENT)
              .keySet());
    } catch (SQLException e) {
      log.error("PostgreSQL query to get most recently viewed entities failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  @Override
  @Nonnull
  public List<String> mostPopularEntityUrns(@Nonnull OperationContext opContext) {
    try {
      List<String> peerUrns = UsageEventsRecommendationPeerActors.peerActorUrns(opContext);
      return mostPopularEntityUrns(peerUrns);
    } catch (SQLException e) {
      log.error("PostgreSQL query to get most popular entities failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  @Override
  @Nonnull
  public List<String> recentlyEditedEntityUrns(@Nonnull OperationContext opContext) {
    try {
      return new ArrayList<>(
          entityUrnsOrderedByMaxTsForUser(
                  opContext.getSessionActorContext().getActorUrn(),
                  DataHubUsageEventType.ENTITY_ACTION_EVENT)
              .keySet());
    } catch (SQLException e) {
      log.error("PostgreSQL query to get most recently edited entities failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  public List<String> mostPopularEntityUrns(@Nullable List<String> peerActorUrns)
      throws SQLException {

    List<String> out = new ArrayList<>();

    StringBuilder sql = new StringBuilder();

    List<Object> binds = new ArrayList<>();

    sql.append("SELECT ").append(PostgresUsageEventsStore.COL_ENTITY_URN).append(" FROM ");

    sql.append(store.qualifiedParentTable());

    sql.append(" WHERE ");

    sql.append(PostgresUsageEventsStore.COL_TS).append(">=?");

    binds.add(lookbackCutoffMillis());

    sql.append(" AND ").append(EXCLUDE_BACKEND_USAGE);

    sql.append(" AND ").append(PostgresUsageEventsStore.COL_EVENT_TYPE).append("=?");

    binds.add(DataHubUsageEventType.ENTITY_VIEW_EVENT.getType());

    sql.append(" AND ");

    sql.append(PostgresUsageEventsStore.COL_ENTITY_URN).append(" IS NOT NULL");

    if (peerActorUrns != null && !peerActorUrns.isEmpty()) {

      sql.append(" AND ");

      sql.append(PostgresUsageEventsStore.COL_ACTOR_URN)
          .append(" IN (")
          .append("?,".repeat(peerActorUrns.size() - 1))
          .append("?)");

      binds.addAll(peerActorUrns);
    }

    sql.append(" GROUP BY ").append(PostgresUsageEventsStore.COL_ENTITY_URN);

    sql.append(" ORDER BY COUNT(*) DESC LIMIT ").append(DEFAULT_LIMIT);

    try (Connection c = store.getDatabase().dataSource().getConnection()) {

      c.setReadOnly(true);

      try (PreparedStatement ps = c.prepareStatement(sql.toString())) {

        int ix = 1;

        for (Object b : binds) {

          if (b instanceof Long l) {

            ps.setLong(ix++, l);

          } else {

            ps.setString(ix++, String.valueOf(b));
          }
        }

        try (ResultSet rs = ps.executeQuery()) {

          while (rs.next()) {

            String urn = rs.getString(1);

            if (urn != null && !urn.isBlank()) {

              out.add(urn);
            }
          }
        }
      }
    }

    return out;
  }

  public LinkedHashMap<String, Long> recentlyEditedUrnsOrdered(Urn userUrn) throws SQLException {
    return entityUrnsOrderedByMaxTsForUser(userUrn, DataHubUsageEventType.ENTITY_ACTION_EVENT);
  }

  private LinkedHashMap<String, Long> entityUrnsOrderedByMaxTsForUser(
      Urn userUrn, DataHubUsageEventType eventType) throws SQLException {

    LinkedHashMap<String, Long> ranked = new LinkedHashMap<>();

    String sql =
        "SELECT entity_urn, MAX(timestamp_ms) mx FROM "
            + store.qualifiedParentTable()
            + " WHERE timestamp_ms >= ? AND "
            + EXCLUDE_BACKEND_USAGE
            + " AND "
            + PostgresUsageEventsStore.COL_EVENT_TYPE
            + " = ? AND "
            + PostgresUsageEventsStore.COL_ACTOR_URN
            + " = ? AND entity_urn IS NOT NULL GROUP BY entity_urn ORDER BY mx DESC LIMIT "
            + DEFAULT_LIMIT;

    try (Connection c = store.getDatabase().dataSource().getConnection()) {

      c.setReadOnly(true);

      try (PreparedStatement ps = c.prepareStatement(sql)) {

        ps.setLong(1, lookbackCutoffMillis());

        ps.setString(2, eventType.getType());

        ps.setString(3, userUrn.toString());

        try (ResultSet rs = ps.executeQuery()) {

          while (rs.next()) {

            ranked.put(rs.getString(1), rs.getLong(2));
          }
        }
      }
    }

    return ranked;
  }
}
