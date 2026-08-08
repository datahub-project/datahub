package com.linkedin.metadata.analytics.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import com.linkedin.metadata.postgres.jdbc.PostgresPreparedBinder;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresAnalyticsStore {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Getter @Nonnull private final Database database;
  @Getter @Nonnull private final PgAnalyticsStoreOptions options;

  public PostgresAnalyticsStore(
      @Nonnull Database database, @Nonnull PgAnalyticsStoreOptions options) {
    this.database = database;
    this.options = options;
  }

  @Nonnull
  public String qualifiedEventTable() {
    return options.qualifiedEventTable();
  }

  @Nonnull
  public String qualifiedRollupTable() {
    return options.qualifiedRollupTable();
  }

  @Nonnull
  public String qualifiedDistinctSetTable() {
    return options.qualifiedDistinctSetTable();
  }

  @Nonnull
  public String qualifiedWatermarkTable() {
    return options.qualifiedWatermarkTable();
  }

  /**
   * Ebean pool connections default to {@code autoCommit=false}; raw JDBC writes must opt into
   * autocommit (same pattern as pgTimeseries DAOs) or changes roll back when the connection is
   * returned to the pool.
   */
  @Nonnull
  private Connection openWriteConnection() throws SQLException {
    Connection c = database.dataSource().getConnection();
    c.setAutoCommit(true);
    return c;
  }

  public void insertEvents(@Nonnull List<PostgresAnalyticsEventInsert> rows) throws SQLException {
    if (rows.isEmpty()) {
      return;
    }
    String sql =
        "INSERT INTO "
            + qualifiedEventTable()
            + " (event_time, metric_family, event_id, metric_name, event_type, actor_urn,"
            + " entity_urn, entity_type, usage_source, browser_id, query, section, action_type,"
            + " aspect_name, dimensions, document) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?::jsonb,?::jsonb)"
            + " ON CONFLICT (event_time, metric_family, event_id) DO NOTHING";
    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      try (PreparedStatement ps = c.prepareStatement(sql)) {
        for (PostgresAnalyticsEventInsert row : rows) {
          List<Object> params = new ArrayList<>();
          params.add(Timestamp.from(row.getEventTime()));
          params.add(row.getMetricFamily());
          params.add(row.getEventId());
          params.add(row.getMetricName());
          params.add(row.getEventType());
          params.add(row.getActorUrn());
          params.add(row.getEntityUrn());
          params.add(row.getEntityType());
          params.add(row.getUsageSource());
          params.add(row.getBrowserId());
          params.add(row.getQuery());
          params.add(row.getSection());
          params.add(row.getActionType());
          params.add(row.getAspectName());
          params.add(row.getDimensionsJson());
          params.add(row.getDocumentJson());
          PostgresPreparedBinder.bind(ps, params);
          ps.addBatch();
        }
        ps.executeBatch();
        c.commit();
      } catch (SQLException e) {
        c.rollback();
        throw e;
      }
    }
  }

  /** Last-wins gauge row for {@link #replaceLatestRollups}. */
  public record LatestRollupValue(
      @Nonnull String metricName, @Nonnull Map<String, String> groupDims, double value) {}

  /** Upsert-merge additive rollup into a UTC hour (or other grain) bucket. */
  public void mergeAdditiveRollup(
      @Nonnull Instant bucketStart,
      @Nonnull String grain,
      @Nonnull String metricFamily,
      @Nonnull String metricName,
      @Nonnull Map<String, String> groupDims,
      double valueSum,
      long valueCount)
      throws SQLException {
    try (Connection c = openWriteConnection()) {
      mergeAdditiveRollup(
          c, bucketStart, grain, metricFamily, metricName, groupDims, valueSum, valueCount);
    }
  }

  private void mergeAdditiveRollup(
      @Nonnull Connection c,
      @Nonnull Instant bucketStart,
      @Nonnull String grain,
      @Nonnull String metricFamily,
      @Nonnull String metricName,
      @Nonnull Map<String, String> groupDims,
      double valueSum,
      long valueCount)
      throws SQLException {
    String groupKey = PostgresAnalyticsGroupKey.of(groupDims);
    final String dimsJson;
    try {
      dimsJson = MAPPER.writeValueAsString(PostgresAnalyticsGroupKey.canonicalize(groupDims));
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new SQLException("Failed to serialize group_dims", e);
    }
    String sql =
        "INSERT INTO "
            + qualifiedRollupTable()
            + " (bucket_start, grain, metric_family, metric_name, merge_kind, group_key, group_dims,"
            + " value_sum, value_count) VALUES (?,?,?,?,?,?,?::jsonb,?,?)"
            + " ON CONFLICT (bucket_start, grain, metric_family, metric_name, merge_kind, group_key)"
            + " DO UPDATE SET value_sum = "
            + qualifiedRollupTable()
            + ".value_sum + EXCLUDED.value_sum,"
            + " value_count = "
            + qualifiedRollupTable()
            + ".value_count + EXCLUDED.value_count";
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      PostgresPreparedBinder.bind(
          ps,
          List.of(
              Timestamp.from(bucketStart),
              grain,
              metricFamily,
              metricName,
              AnalyticsMetricFamilies.MERGE_ADDITIVE,
              groupKey,
              dimsJson,
              valueSum,
              valueCount));
      ps.executeUpdate();
    }
  }

  /** Last-wins gauge upsert for merge_kind=latest. */
  public void upsertLatestRollup(
      @Nonnull Instant bucketStart,
      @Nonnull String grain,
      @Nonnull String metricFamily,
      @Nonnull String metricName,
      @Nonnull Map<String, String> groupDims,
      double value)
      throws SQLException {
    try (Connection c = openWriteConnection()) {
      upsertLatestRollup(c, bucketStart, grain, metricFamily, metricName, groupDims, value);
    }
  }

  private void upsertLatestRollup(
      @Nonnull Connection c,
      @Nonnull Instant bucketStart,
      @Nonnull String grain,
      @Nonnull String metricFamily,
      @Nonnull String metricName,
      @Nonnull Map<String, String> groupDims,
      double value)
      throws SQLException {
    String groupKey = PostgresAnalyticsGroupKey.of(groupDims);
    String dimsJson;
    try {
      dimsJson = MAPPER.writeValueAsString(PostgresAnalyticsGroupKey.canonicalize(groupDims));
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new SQLException("Failed to serialize group_dims", e);
    }
    String sql =
        "INSERT INTO "
            + qualifiedRollupTable()
            + " (bucket_start, grain, metric_family, metric_name, merge_kind, group_key, group_dims,"
            + " value_sum, value_count) VALUES (?,?,?,?,?,?,?::jsonb,?,1)"
            + " ON CONFLICT (bucket_start, grain, metric_family, metric_name, merge_kind, group_key)"
            + " DO UPDATE SET value_sum = EXCLUDED.value_sum, value_count = 1";
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      PostgresPreparedBinder.bind(
          ps,
          List.of(
              Timestamp.from(bucketStart),
              grain,
              metricFamily,
              metricName,
              AnalyticsMetricFamilies.MERGE_LATEST,
              groupKey,
              dimsJson,
              value));
      ps.executeUpdate();
    }
  }

  /**
   * Atomically replace latest-merge gauges for the given bucket and metric names: delete existing
   * rows for those metrics, then insert {@code values}. Clears omitted dimensions so inventory
   * snapshots cannot leave stale entity types.
   */
  public void replaceLatestRollups(
      @Nonnull Instant bucketStart,
      @Nonnull String grain,
      @Nonnull String metricFamily,
      @Nonnull List<String> metricNames,
      @Nonnull List<LatestRollupValue> values)
      throws SQLException {
    if (metricNames.isEmpty()) {
      return;
    }
    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      try {
        StringBuilder deleteSql =
            new StringBuilder(
                "DELETE FROM "
                    + qualifiedRollupTable()
                    + " WHERE bucket_start = ? AND grain = ? AND metric_family = ?"
                    + " AND merge_kind = ? AND metric_name IN (");
        for (int i = 0; i < metricNames.size(); i++) {
          if (i > 0) {
            deleteSql.append(',');
          }
          deleteSql.append('?');
        }
        deleteSql.append(')');
        try (PreparedStatement ps = c.prepareStatement(deleteSql.toString())) {
          List<Object> params = new ArrayList<>();
          params.add(Timestamp.from(bucketStart));
          params.add(grain);
          params.add(metricFamily);
          params.add(AnalyticsMetricFamilies.MERGE_LATEST);
          params.addAll(metricNames);
          PostgresPreparedBinder.bind(ps, params);
          ps.executeUpdate();
        }
        for (LatestRollupValue value : values) {
          upsertLatestRollup(
              c,
              bucketStart,
              grain,
              metricFamily,
              value.metricName(),
              value.groupDims(),
              value.value());
        }
        c.commit();
      } catch (SQLException e) {
        c.rollback();
        throw e;
      }
    }
  }

  public void upsertDistinctIdentities(
      @Nonnull Instant bucketStart,
      @Nonnull String grain,
      @Nonnull String metricFamily,
      @Nonnull String metricName,
      @Nonnull String actorClass,
      @Nonnull List<String> usageIdentities)
      throws SQLException {
    if (usageIdentities.isEmpty()) {
      return;
    }
    String sql =
        "INSERT INTO "
            + qualifiedDistinctSetTable()
            + " (bucket_start, grain, metric_family, metric_name, actor_class, usage_identity)"
            + " VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING";
    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      try (PreparedStatement ps = c.prepareStatement(sql)) {
        for (String identity : usageIdentities) {
          if (identity == null || identity.isBlank()) {
            continue;
          }
          PostgresPreparedBinder.bind(
              ps,
              List.of(
                  Timestamp.from(bucketStart),
                  grain,
                  metricFamily,
                  metricName,
                  actorClass,
                  identity));
          ps.addBatch();
        }
        ps.executeBatch();
        c.commit();
      } catch (SQLException e) {
        c.rollback();
        throw e;
      }
    }
    refreshDistinctRollupCardinality(bucketStart, grain, metricFamily, metricName, actorClass);
  }

  public void refreshDistinctRollupCardinality(
      @Nonnull Instant bucketStart,
      @Nonnull String grain,
      @Nonnull String metricFamily,
      @Nonnull String metricName,
      @Nonnull String actorClass)
      throws SQLException {
    Map<String, String> dims = Map.of("actor_class", actorClass);
    String groupKey = PostgresAnalyticsGroupKey.of(dims);
    String dimsJson;
    try {
      dimsJson = MAPPER.writeValueAsString(dims);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new SQLException(e);
    }
    String countSql =
        "SELECT COUNT(*) FROM "
            + qualifiedDistinctSetTable()
            + " WHERE bucket_start = ? AND grain = ? AND metric_family = ? AND metric_name = ?"
            + " AND actor_class = ?";
    long cardinality;
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(countSql)) {
      PostgresPreparedBinder.bind(
          ps, List.of(Timestamp.from(bucketStart), grain, metricFamily, metricName, actorClass));
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        cardinality = rs.getLong(1);
      }
    }
    String upsert =
        "INSERT INTO "
            + qualifiedRollupTable()
            + " (bucket_start, grain, metric_family, metric_name, merge_kind, group_key, group_dims,"
            + " value_sum, value_count) VALUES (?,?,?,?,?,?,?::jsonb,0,?)"
            + " ON CONFLICT (bucket_start, grain, metric_family, metric_name, merge_kind, group_key)"
            // Concurrent flushes can race with a stale lower COUNT(*); never regress cardinality.
            + " DO UPDATE SET value_count = GREATEST("
            + qualifiedRollupTable()
            + ".value_count, EXCLUDED.value_count)";
    try (Connection c = openWriteConnection();
        PreparedStatement ps = c.prepareStatement(upsert)) {
      PostgresPreparedBinder.bind(
          ps,
          List.of(
              Timestamp.from(bucketStart),
              grain,
              metricFamily,
              metricName,
              AnalyticsMetricFamilies.MERGE_DISTINCT,
              groupKey,
              dimsJson,
              cardinality));
      ps.executeUpdate();
    }
  }

  public void upsertWatermark(
      @Nonnull String layer,
      @Nonnull String metricFamily,
      @Nonnull String partitionKey,
      @Nonnull Instant sealedThrough)
      throws SQLException {
    String sql =
        "INSERT INTO "
            + qualifiedWatermarkTable()
            + " (layer, metric_family, partition_key, sealed_through, updated_at)"
            + " VALUES (?,?,?,?,now())"
            + " ON CONFLICT (layer, metric_family, partition_key)"
            + " DO UPDATE SET sealed_through = EXCLUDED.sealed_through, updated_at = now()";
    try (Connection c = openWriteConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      PostgresPreparedBinder.bind(
          ps, List.of(layer, metricFamily, partitionKey, Timestamp.from(sealedThrough)));
      ps.executeUpdate();
    }
  }

  @Nullable
  public Instant getSealedThrough(
      @Nonnull String layer, @Nonnull String metricFamily, @Nonnull String partitionKey)
      throws SQLException {
    String sql =
        "SELECT sealed_through FROM "
            + qualifiedWatermarkTable()
            + " WHERE layer = ? AND metric_family = ? AND partition_key = ?";
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      PostgresPreparedBinder.bind(ps, List.of(layer, metricFamily, partitionKey));
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        Timestamp ts = rs.getTimestamp(1);
        return ts == null ? null : ts.toInstant();
      }
    }
  }

  /**
   * Latest hour bucket start that has a watermark for {@code metricFamily}, derived from {@code
   * sealed_through - 1 hour}. Returns null when no hour watermark exists.
   */
  @Nullable
  public Instant getLatestSealedHourStart(@Nonnull String metricFamily) throws SQLException {
    String sql =
        "SELECT MAX(sealed_through) FROM "
            + qualifiedWatermarkTable()
            + " WHERE layer = ? AND metric_family = ?";
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      PostgresPreparedBinder.bind(ps, List.of(AnalyticsMetricFamilies.LAYER_HOUR, metricFamily));
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        Timestamp ts = rs.getTimestamp(1);
        if (ts == null) {
          return null;
        }
        return PostgresAnalyticsUtc.truncateToUtcHour(ts.toInstant().minusSeconds(1));
      }
    }
  }

  /**
   * Returns the subset of {@code partitionKeys} that have a watermark row for the given layer and
   * metric family. Uses a single connection/query.
   */
  @Nonnull
  public java.util.Set<String> getSealedPartitionKeys(
      @Nonnull String layer,
      @Nonnull String metricFamily,
      @Nonnull java.util.Collection<String> partitionKeys)
      throws SQLException {
    if (partitionKeys.isEmpty()) {
      return java.util.Set.of();
    }
    List<String> keys = new ArrayList<>(partitionKeys);
    StringBuilder placeholders = new StringBuilder();
    for (int i = 0; i < keys.size(); i++) {
      if (i > 0) {
        placeholders.append(',');
      }
      placeholders.append('?');
    }
    String sql =
        "SELECT partition_key FROM "
            + qualifiedWatermarkTable()
            + " WHERE layer = ? AND metric_family = ? AND partition_key IN ("
            + placeholders
            + ") AND sealed_through IS NOT NULL";
    java.util.Set<String> sealed = new java.util.HashSet<>();
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      List<Object> binds = new ArrayList<>();
      binds.add(layer);
      binds.add(metricFamily);
      binds.addAll(keys);
      PostgresPreparedBinder.bind(ps, binds);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          sealed.add(rs.getString(1));
        }
      }
    }
    return sealed;
  }

  /** True when every hour partition key for [dayStart, dayStart+1d) is sealed. */
  public boolean isDayFullySealed(@Nonnull String metricFamily, @Nonnull Instant dayStart)
      throws SQLException {
    Instant dayEnd = dayStart.plusSeconds(86400);
    List<String> hourKeys = new ArrayList<>(24);
    for (Instant h = dayStart; h.isBefore(dayEnd); h = h.plusSeconds(3600)) {
      hourKeys.add(PostgresAnalyticsUtc.partitionKeyHour(h));
    }
    return getSealedPartitionKeys(AnalyticsMetricFamilies.LAYER_HOUR, metricFamily, hourKeys).size()
        == hourKeys.size();
  }

  /**
   * Materialize datahub_usage event_count hourly rollups for a sealed hour from raw events (grouped
   * by event_type).
   */
  public void materializeDatahubUsageHourlyFromRaw(@Nonnull Instant hourStart) throws SQLException {
    Instant hourEnd = PostgresAnalyticsUtc.hourEndExclusive(hourStart);
    String sql =
        "SELECT event_type, COUNT(*) AS cnt FROM "
            + qualifiedEventTable()
            + " WHERE metric_family = ? AND event_time >= ? AND event_time < ?"
            + " AND (usage_source IS NULL OR usage_source <> 'backend')"
            + " GROUP BY event_type";
    List<Map.Entry<String, Long>> rows = new ArrayList<>();
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      PostgresPreparedBinder.bind(
          ps,
          List.of(
              AnalyticsMetricFamilies.DATAHUB_USAGE,
              Timestamp.from(hourStart),
              Timestamp.from(hourEnd)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String eventType = rs.getString(1);
          long cnt = rs.getLong(2);
          rows.add(Map.entry(eventType == null ? "" : eventType, cnt));
        }
      }
    }
    // Clear + rewrite additive product rollups for this hour in one transaction so a failure
    // cannot leave the sealed hour empty/partial while the watermark still points at it.
    String deleteSql =
        "DELETE FROM "
            + qualifiedRollupTable()
            + " WHERE bucket_start = ? AND grain = ? AND metric_family = ? AND metric_name = ?"
            + " AND merge_kind = ?";
    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      try {
        try (PreparedStatement ps = c.prepareStatement(deleteSql)) {
          PostgresPreparedBinder.bind(
              ps,
              List.of(
                  Timestamp.from(hourStart),
                  AnalyticsMetricFamilies.GRAIN_HOUR,
                  AnalyticsMetricFamilies.DATAHUB_USAGE,
                  "event_count",
                  AnalyticsMetricFamilies.MERGE_ADDITIVE));
          ps.executeUpdate();
        }
        for (Map.Entry<String, Long> e : rows) {
          Map<String, String> dims = new LinkedHashMap<>();
          if (!e.getKey().isEmpty()) {
            dims.put("event_type", e.getKey());
          }
          mergeAdditiveRollup(
              c,
              hourStart,
              AnalyticsMetricFamilies.GRAIN_HOUR,
              AnalyticsMetricFamilies.DATAHUB_USAGE,
              "event_count",
              dims,
              e.getValue().doubleValue(),
              e.getValue());
        }
        c.commit();
      } catch (SQLException e) {
        c.rollback();
        throw e;
      }
    }
  }

  /** Compact sealed hour additive/latest rollups into a day bucket. */
  public void compactHoursToDay(@Nonnull String metricFamily, @Nonnull Instant dayStart)
      throws SQLException {
    Instant dayEnd = dayStart.plusSeconds(86400);
    compactAdditiveGrain(
        metricFamily,
        AnalyticsMetricFamilies.GRAIN_HOUR,
        AnalyticsMetricFamilies.GRAIN_DAY,
        dayStart,
        dayEnd);
    compactLatestGrain(
        metricFamily,
        AnalyticsMetricFamilies.GRAIN_HOUR,
        AnalyticsMetricFamilies.GRAIN_DAY,
        dayStart,
        dayEnd);
    unionDistinctGrain(
        metricFamily,
        AnalyticsMetricFamilies.GRAIN_HOUR,
        AnalyticsMetricFamilies.GRAIN_DAY,
        dayStart,
        dayEnd);
  }

  public void compactDaysToMonth(@Nonnull String metricFamily, @Nonnull Instant monthStart)
      throws SQLException {
    java.time.YearMonth ym = java.time.YearMonth.from(monthStart.atZone(java.time.ZoneOffset.UTC));
    Instant monthEnd = ym.plusMonths(1).atDay(1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant();
    compactAdditiveGrain(
        metricFamily,
        AnalyticsMetricFamilies.GRAIN_DAY,
        AnalyticsMetricFamilies.GRAIN_MONTH,
        monthStart,
        monthEnd);
    compactLatestGrain(
        metricFamily,
        AnalyticsMetricFamilies.GRAIN_DAY,
        AnalyticsMetricFamilies.GRAIN_MONTH,
        monthStart,
        monthEnd);
    unionDistinctGrain(
        metricFamily,
        AnalyticsMetricFamilies.GRAIN_DAY,
        AnalyticsMetricFamilies.GRAIN_MONTH,
        monthStart,
        monthEnd);
  }

  private void compactAdditiveGrain(
      String metricFamily, String fromGrain, String toGrain, Instant bucketStart, Instant rangeEnd)
      throws SQLException {
    String sql =
        "SELECT metric_name, group_key, group_dims::text, SUM(value_sum), SUM(value_count) FROM "
            + qualifiedRollupTable()
            + " WHERE metric_family = ? AND grain = ? AND merge_kind = ? AND bucket_start >= ? AND"
            + " bucket_start < ? GROUP BY metric_name, group_key, group_dims::text";
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      PostgresPreparedBinder.bind(
          ps,
          List.of(
              metricFamily,
              fromGrain,
              AnalyticsMetricFamilies.MERGE_ADDITIVE,
              Timestamp.from(bucketStart),
              Timestamp.from(rangeEnd)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String metricName = rs.getString(1);
          String groupKey = rs.getString(2);
          String dimsText = rs.getString(3);
          double sum = rs.getDouble(4);
          long count = rs.getLong(5);
          upsertRollupExact(
              bucketStart,
              toGrain,
              metricFamily,
              metricName,
              AnalyticsMetricFamilies.MERGE_ADDITIVE,
              groupKey,
              dimsText,
              sum,
              count);
        }
      }
    }
  }

  private void compactLatestGrain(
      String metricFamily, String fromGrain, String toGrain, Instant bucketStart, Instant rangeEnd)
      throws SQLException {
    // Last sample in grain: pick row with max bucket_start per group.
    String sql =
        "SELECT DISTINCT ON (metric_name, group_key) metric_name, group_key, group_dims::text,"
            + " value_sum FROM "
            + qualifiedRollupTable()
            + " WHERE metric_family = ? AND grain = ? AND merge_kind = ? AND bucket_start >= ? AND"
            + " bucket_start < ? ORDER BY metric_name, group_key, bucket_start DESC";
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      PostgresPreparedBinder.bind(
          ps,
          List.of(
              metricFamily,
              fromGrain,
              AnalyticsMetricFamilies.MERGE_LATEST,
              Timestamp.from(bucketStart),
              Timestamp.from(rangeEnd)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          upsertRollupExact(
              bucketStart,
              toGrain,
              metricFamily,
              rs.getString(1),
              AnalyticsMetricFamilies.MERGE_LATEST,
              rs.getString(2),
              rs.getString(3),
              rs.getDouble(4),
              1);
        }
      }
    }
  }

  private void unionDistinctGrain(
      String metricFamily, String fromGrain, String toGrain, Instant bucketStart, Instant rangeEnd)
      throws SQLException {
    String insert =
        "INSERT INTO "
            + qualifiedDistinctSetTable()
            + " (bucket_start, grain, metric_family, metric_name, actor_class, usage_identity)"
            + " SELECT ?, ?, metric_family, metric_name, actor_class, usage_identity FROM "
            + qualifiedDistinctSetTable()
            + " WHERE metric_family = ? AND grain = ? AND bucket_start >= ? AND bucket_start < ?"
            + " ON CONFLICT DO NOTHING";
    try (Connection c = openWriteConnection();
        PreparedStatement ps = c.prepareStatement(insert)) {
      PostgresPreparedBinder.bind(
          ps,
          List.of(
              Timestamp.from(bucketStart),
              toGrain,
              metricFamily,
              fromGrain,
              Timestamp.from(bucketStart),
              Timestamp.from(rangeEnd)));
      ps.executeUpdate();
    }
    String metricsSql =
        "SELECT DISTINCT metric_name, actor_class FROM "
            + qualifiedDistinctSetTable()
            + " WHERE metric_family = ? AND grain = ? AND bucket_start = ?";
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(metricsSql)) {
      PostgresPreparedBinder.bind(ps, List.of(metricFamily, toGrain, Timestamp.from(bucketStart)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          refreshDistinctRollupCardinality(
              bucketStart, toGrain, metricFamily, rs.getString(1), rs.getString(2));
        }
      }
    }
  }

  private void upsertRollupExact(
      Instant bucketStart,
      String grain,
      String metricFamily,
      String metricName,
      String mergeKind,
      String groupKey,
      String dimsJson,
      double valueSum,
      long valueCount)
      throws SQLException {
    String sql =
        "INSERT INTO "
            + qualifiedRollupTable()
            + " (bucket_start, grain, metric_family, metric_name, merge_kind, group_key, group_dims,"
            + " value_sum, value_count) VALUES (?,?,?,?,?,?,?::jsonb,?,?)"
            + " ON CONFLICT (bucket_start, grain, metric_family, metric_name, merge_kind, group_key)"
            + " DO UPDATE SET value_sum = EXCLUDED.value_sum, value_count = EXCLUDED.value_count,"
            + " group_dims = EXCLUDED.group_dims";
    try (Connection c = openWriteConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      PostgresPreparedBinder.bind(
          ps,
          List.of(
              Timestamp.from(bucketStart),
              grain,
              metricFamily,
              metricName,
              mergeKind,
              groupKey == null ? "" : groupKey,
              dimsJson == null || dimsJson.isBlank() ? "{}" : dimsJson,
              valueSum,
              valueCount));
      ps.executeUpdate();
    }
  }
}
