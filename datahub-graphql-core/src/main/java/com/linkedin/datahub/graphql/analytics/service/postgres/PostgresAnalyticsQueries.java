package com.linkedin.datahub.graphql.analytics.service.postgres;

import static com.linkedin.metadata.Constants.DATAHUB_USAGE_EVENT_INDEX;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.BarSegment;
import com.linkedin.datahub.graphql.generated.Cell;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.generated.NamedLine;
import com.linkedin.datahub.graphql.generated.NumericDataPoint;
import com.linkedin.datahub.graphql.generated.Row;
import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsUtc;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** JDBC analytics backed by pgAnalytics ({@code analytics_event} + sealed rollups). */
@Slf4j
@RequiredArgsConstructor
public class PostgresAnalyticsQueries {

  private static final DateTimeFormatter ISO_BUCKET =
      DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

  private final PostgresAnalyticsStore store;
  private final IndexConvention indexConvention;

  private static String placeholders(int count) {

    List<String> p = new ArrayList<>();

    for (int i = 0; i < count; i++) {

      p.add("?");
    }

    return String.join(",", p);
  }

  private String tbl() {
    return store.qualifiedEventTable();
  }

  private String rollupTbl() {
    return store.qualifiedRollupTable();
  }

  public String usageIndexName() {

    return indexConvention.getIndexName(DATAHUB_USAGE_EVENT_INDEX);
  }

  private void guardUsage(String indexName) {

    if (!usageIndexName().equals(indexName)) {

      throw new IllegalArgumentException();
    }
  }

  /** WHERE fragment + JDBC params (positional). */
  private record WherePred(String predicate, List<Object> binds) {}

  private WherePred whereUsage(
      Optional<DateRange> range,
      Map<String, List<String>> must,
      Map<String, List<String>> mustNot) {

    List<String> parts = new ArrayList<>();

    List<Object> binds = new ArrayList<>();

    parts.add("metric_family = '" + AnalyticsMetricFamilies.DATAHUB_USAGE + "'");
    parts.add("(usage_source IS NULL OR usage_source <> 'backend')");

    range.ifPresent(
        dr -> {
          parts.add("event_time >= ?");
          parts.add("event_time < ?");
          binds.add(Timestamp.from(Instant.ofEpochMilli(Long.parseLong(dr.getStart()))));
          binds.add(Timestamp.from(Instant.ofEpochMilli(Long.parseLong(dr.getEnd()))));
        });

    applyTerms(parts, binds, must, true);

    applyTerms(parts, binds, mustNot, false);

    return new WherePred(String.join(" AND ", parts), binds);
  }

  private void applyTerms(
      List<String> parts, List<Object> binds, Map<String, List<String>> m, boolean positive) {

    m.forEach(
        (fk, vals) -> {
          if (vals == null || vals.isEmpty()) {

            return;
          }

          String col = mappedField(normalizeFk(fk));

          String qs = placeholders(vals.size());

          if (positive) {

            parts.add(col + " IN (" + qs + ")");

          } else {

            parts.add("( " + col + " IS NULL OR " + col + " NOT IN (" + qs + "))");
          }

          binds.addAll(vals);
        });
  }

  private static String normalizeFk(String fk) {

    String f = fk.endsWith(".keyword") ? fk.substring(0, fk.length() - ".keyword".length()) : fk;

    return f;
  }

  private static String mappedField(String f) {

    switch (f) {
      case DataHubUsageEventConstants.ACTOR_URN:
        return "actor_urn";

      case DataHubUsageEventConstants.ENTITY_URN:
        return "entity_urn";

      case DataHubUsageEventConstants.ENTITY_TYPE:
        return "entity_type";

      case DataHubUsageEventConstants.TYPE:
        return "event_type";

      case DataHubUsageEventConstants.QUERY:
        return "COALESCE(" + "query" + ",document->>'" + DataHubUsageEventConstants.QUERY + "')";

      case "browserId":
        return "COALESCE(" + "browser_id" + ",document->>'browserId')";

      case "removed":
        return "document->>'removed'";

      case "section":
        return "COALESCE(" + "section" + ",document->>'section')";

      case "actionType":
        return "COALESCE(" + "action_type" + ",document->>'actionType')";

      default:
        return "document->>'" + f.replace("'", "''") + "'";
    }
  }

  private int bind(PreparedStatement ps, int idx, WherePred wp) throws SQLException {

    int i = idx;

    for (Object v : wp.binds) {

      bindOne(ps, i++, v);
    }

    return i;
  }

  private static void bindOne(PreparedStatement ps, int idx, Object v) throws SQLException {
    if (v instanceof Long l) {
      ps.setLong(idx, l);
    } else if (v instanceof Timestamp ts) {
      ps.setTimestamp(idx, ts);
    } else {
      ps.setString(idx, String.valueOf(v));
    }
  }

  private Connection cn() throws SQLException {
    // Do not call setReadOnly(true): on PostgreSQL JDBC that begins a transaction, and under
    // parallel chart resolution a failed/abandoned checkout can leave pool connections
    // "idle in transaction", starving both analytics reads and the usage-event indexer.
    Connection c = store.getDatabase().dataSource().getConnection();
    try {
      if (!c.getAutoCommit()) {
        c.setAutoCommit(true);
      }
      // Charts zero-fill in UTC; keep session TZ aligned so any DATE_TRUNC on timestamptz matches.
      try (var st = c.createStatement()) {
        st.execute("SET TIME ZONE 'UTC'");
      }
      return c;
    } catch (SQLException e) {
      try {
        c.close();
      } catch (SQLException closeEx) {
        e.addSuppressed(closeEx);
      }
      throw e;
    }
  }

  static String truncate(DateInterval granularity) {

    return switch (granularity) {
      case SECOND -> "second";

      case MINUTE -> "minute";

      case HOUR -> "hour";

      case DAY -> "day";

      case WEEK -> "week";

      case MONTH -> "month";

      case YEAR -> "year";
    };
  }

  static String bucketStartMsSql(String truncated) {
    // Truncate in UTC wall-clock, then re-interpret as timestamptz UTC so EXTRACT(EPOCH) is
    // independent of the session TimeZone (matches expectedBucketKeys / zero-fill).
    return "CAST(EXTRACT(EPOCH FROM (DATE_TRUNC('"
        + truncated
        + "', event_time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'))*1000 AS BIGINT)";
  }

  /**
   * Bucket starts for {@code [dateRange.start, dateRange.end)} at the given grain, matching
   * Postgres {@code DATE_TRUNC} (UTC; weeks start Monday). Used to zero-fill sparse series so
   * charts match ES date_histogram {@code min_doc_count=0} behavior.
   */
  static List<String> expectedBucketKeys(DateRange dateRange, DateInterval granularity) {
    Instant start = Instant.ofEpochMilli(Long.parseLong(dateRange.getStart()));
    Instant end = Instant.ofEpochMilli(Long.parseLong(dateRange.getEnd()));
    Instant cursor = truncateBucketStart(start, granularity);
    List<String> keys = new ArrayList<>();
    while (cursor.isBefore(end)) {
      keys.add(ISO_BUCKET.format(cursor));
      cursor = nextBucketStart(cursor, granularity);
    }
    return keys;
  }

  static Instant truncateBucketStart(Instant instant, DateInterval granularity) {
    var zdt = instant.atZone(ZoneOffset.UTC);
    return switch (granularity) {
      case SECOND -> zdt.truncatedTo(ChronoUnit.SECONDS).toInstant();
      case MINUTE -> zdt.truncatedTo(ChronoUnit.MINUTES).toInstant();
      case HOUR -> zdt.truncatedTo(ChronoUnit.HOURS).toInstant();
      case DAY -> zdt.truncatedTo(ChronoUnit.DAYS).toInstant();
      case WEEK -> zdt.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
          .truncatedTo(ChronoUnit.DAYS)
          .toInstant();
      case MONTH -> zdt.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS).toInstant();
      case YEAR -> zdt.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS).toInstant();
    };
  }

  static Instant nextBucketStart(Instant cursor, DateInterval granularity) {
    return switch (granularity) {
      case SECOND -> cursor.plusSeconds(1);
      case MINUTE -> cursor.plus(1, ChronoUnit.MINUTES);
      case HOUR -> cursor.plus(1, ChronoUnit.HOURS);
      case DAY -> cursor.plus(1, ChronoUnit.DAYS);
      case WEEK -> cursor.plus(7, ChronoUnit.DAYS);
      case MONTH -> cursor.atZone(ZoneOffset.UTC).plusMonths(1).toInstant();
      case YEAR -> cursor.atZone(ZoneOffset.UTC).plusYears(1).toInstant();
    };
  }

  static TreeMap<String, Integer> fillEmptyBuckets(
      TreeMap<String, Integer> observed, List<String> expectedKeys) {
    TreeMap<String, Integer> filled = new TreeMap<>();
    for (String key : expectedKeys) {
      filled.put(key, observed.getOrDefault(key, 0));
    }
    return filled;
  }

  private static List<NamedLine> toNamedLines(
      Map<String, TreeMap<String, Integer>> seriesDim,
      boolean hasDimension,
      DateRange dateRange,
      DateInterval granularity) {
    List<String> expected = expectedBucketKeys(dateRange, granularity);
    if (!hasDimension) {
      TreeMap<String, Integer> filled =
          fillEmptyBuckets(seriesDim.getOrDefault("__", new TreeMap<>()), expected);
      List<NumericDataPoint> pts =
          filled.entrySet().stream()
              .map(en -> new NumericDataPoint(en.getKey(), en.getValue()))
              .collect(Collectors.toList());
      return ImmutableList.of(new NamedLine("total", pts));
    }
    if (seriesDim.isEmpty()) {
      return ImmutableList.of();
    }
    List<NamedLine> out = new ArrayList<>();
    for (Map.Entry<String, TreeMap<String, Integer>> e : seriesDim.entrySet()) {
      TreeMap<String, Integer> filled = fillEmptyBuckets(e.getValue(), expected);
      List<NumericDataPoint> pts =
          filled.entrySet().stream()
              .map(en -> new NumericDataPoint(en.getKey(), en.getValue()))
              .collect(Collectors.toList());
      out.add(new NamedLine(e.getKey(), pts));
    }
    return out;
  }

  public List<NamedLine> getTimeseriesChart(
      String indexName,
      DateRange dateRange,
      DateInterval granularity,
      Optional<String> dimension,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn) {

    return getTimeseriesChart(
        indexName,
        dateRange,
        granularity,
        dimension,
        filters,
        mustNotFilters,
        uniqueOn,
        DataHubUsageEventConstants.TIMESTAMP);
  }

  public List<NamedLine> getTimeseriesChart(
      String indexName,
      DateRange dateRange,
      DateInterval granularity,
      Optional<String> dimension,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn,
      @SuppressWarnings("unused") String dateRangeIgnored) {

    guardUsage(indexName);

    if (canUseRollups(granularity, dateRange)) {
      List<NamedLine> fromRollup =
          getTimeseriesFromRollup(
              dateRange, granularity, dimension, filters, mustNotFilters, uniqueOn);
      if (fromRollup != null) {
        return fromRollup;
      }
    }

    WherePred w = whereUsage(Optional.of(dateRange), filters, mustNotFilters);

    String trunc = truncate(granularity);

    String bx = bucketStartMsSql(trunc);

    Optional<String> dimSql = dimension.map(d -> mappedField(normalizeFk(d)));

    String countExpr =
        uniqueOn.map(u -> "COUNT(DISTINCT " + mappedField(normalizeFk(u)) + ")").orElse("COUNT(*)");

    String sql;

    if (dimSql.isEmpty()) {

      sql =
          "SELECT "
              + bx
              + " bx, "
              + countExpr
              + " cnt FROM "
              + tbl()
              + " WHERE "
              + w.predicate
              + " GROUP BY bx ORDER BY bx";

    } else {

      sql =
          "SELECT "
              + bx
              + " bx,"
              + dimSql.get()
              + " dx, "
              + countExpr
              + " cnt FROM "
              + tbl()
              + " WHERE "
              + w.predicate
              + " GROUP BY bx,dx ORDER BY bx";
    }

    Map<String, TreeMap<String, Integer>> seriesDim = new LinkedHashMap<>();

    try (Connection c = cn();
        PreparedStatement ps = c.prepareStatement(sql)) {

      bind(ps, 1, w);

      try (ResultSet rs = ps.executeQuery()) {

        while (rs.next()) {

          String lx = ISO_BUCKET.format(java.time.Instant.ofEpochMilli(rs.getLong("bx")));

          int cnt = rs.getInt("cnt");

          if (dimSql.isEmpty()) {

            seriesDim.computeIfAbsent("__", __ -> new TreeMap<>()).merge(lx, cnt, Integer::sum);

          } else {

            String d = rs.getString("dx");
            // Match ES terms aggregations: omit missing/blank dimensions (no synthetic N/A series).
            if (d == null || d.isBlank()) {
              continue;
            }

            seriesDim.computeIfAbsent(d, __ -> new TreeMap<>()).merge(lx, cnt, Integer::sum);
          }
        }
      }

      return toNamedLines(seriesDim, dimSql.isPresent(), dateRange, granularity);

    } catch (Exception ex) {

      log.error("Postgres analytics timeseries error", ex);

      return ImmutableList.of();
    }
  }

  public List<NamedBar> getBarChart(
      String indexName,
      Optional<DateRange> range,
      List<String> dimensions,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNot,
      Optional<String> uniqueOn,
      boolean showMissing) {

    guardUsage(indexName);

    if (!(dimensions.size() == 1 || dimensions.size() == 2)) {

      throw new IllegalArgumentException("Dimensions must have 1 or 2 specified: " + dimensions);
    }

    WherePred w = whereUsage(range, filters, mustNot);

    String agg =
        uniqueOn.map(u -> "COUNT(DISTINCT " + mappedField(normalizeFk(u)) + ")").orElse("COUNT(*)");

    try {

      if (dimensions.size() == 1) {

        String gexpr = mappedField(normalizeFk(dimensions.get(0)));

        String sql = barSelectSingle(gexpr, agg, showMissing, w.predicate);

        return readBars1(sql, w, showMissing);
      }

      String ge0 = mappedField(normalizeFk(dimensions.get(0)));

      String ge1 = mappedField(normalizeFk(dimensions.get(1)));

      String sql = barSelectDouble(ge0, ge1, agg, showMissing, w.predicate);

      return readBars2(sql, w, showMissing);

    } catch (Exception ex) {

      log.error("Postgres analytics bar chart error", ex);

      return ImmutableList.of();
    }
  }

  private String barSelectSingle(String expr, String agg, boolean showMissing, String pred) {

    String label = grpLabel(expr, showMissing);
    String where = pred + nonMissingSql(expr, showMissing);

    return "SELECT "
        + label
        + ", "
        + agg
        + " cnt FROM "
        + tbl()
        + " WHERE "
        + where
        + " GROUP BY grp ORDER BY cnt DESC LIMIT 500";
  }

  private String barSelectDouble(
      String e0, String e1, String agg, boolean showMissing, String pred) {

    String l0 = grpLabelAlias(e0, "g0", showMissing);

    String l1 = grpLabelAlias(e1, "g1", showMissing);
    String where = pred + nonMissingSql(e0, showMissing) + nonMissingSql(e1, showMissing);

    return "SELECT "
        + l0
        + ","
        + l1
        + ","
        + agg
        + " cnt FROM "
        + tbl()
        + " WHERE "
        + where
        + " GROUP BY g0,g1 ORDER BY cnt DESC LIMIT 500";
  }

  /** When {@code showMissing} is false, drop NULL/blank dimension values like ES terms. */
  private static String nonMissingSql(String expr, boolean showMissing) {
    if (showMissing) {
      return "";
    }
    return " AND (" + expr + ") IS NOT NULL AND NULLIF(trim((" + expr + ")::text), '') IS NOT NULL";
  }

  private static String grpLabel(String inner, boolean showMissing) {

    return grpLabelAlias(inner, "grp", showMissing);
  }

  private static String grpLabelAlias(String inner, String alias, boolean showMissing) {

    if (showMissing) {

      String naEsc = "'" + AnalyticsService.NA.replace("'", "''") + "'";

      return " COALESCE(NULLIF(trim(" + inner + "::text),'')," + naEsc + ") AS " + alias;
    }

    return inner + " AS " + alias;
  }

  private List<NamedBar> readBars1(String sql, WherePred w, boolean missing) throws SQLException {

    LinkedHashMap<String, Integer> m = new LinkedHashMap<>();

    try (Connection c = cn();
        PreparedStatement ps = c.prepareStatement(sql)) {

      bind(ps, 1, w);

      try (ResultSet rs = ps.executeQuery()) {

        while (rs.next()) {

          m.put(miss(rs.getString(1), missing), rs.getInt(2));
        }
      }
    }

    return m.entrySet().stream()
        .map(en -> wrapBar(en.getKey(), en.getValue()))
        .collect(Collectors.toList());
  }

  private NamedBar wrapBar(String label, Integer value) {

    return new NamedBar(
        label, ImmutableList.of(BarSegment.builder().setLabel("Count").setValue(value).build()));
  }

  private List<NamedBar> readBars2(String sql, WherePred w, boolean missing) throws SQLException {

    LinkedHashMap<String, List<BarSegment>> outer = new LinkedHashMap<>();

    try (Connection c = cn();
        PreparedStatement ps = c.prepareStatement(sql)) {

      bind(ps, 1, w);

      try (ResultSet rs = ps.executeQuery()) {

        while (rs.next()) {

          String o = miss(rs.getString(1), missing);

          String i = miss(rs.getString(2), missing);

          outer
              .computeIfAbsent(o, __ -> new ArrayList<>())
              .add(BarSegment.builder().setLabel(i).setValue(rs.getInt(3)).build());
        }
      }
    }

    return outer.entrySet().stream()
        .map(en -> new NamedBar(en.getKey(), en.getValue()))
        .collect(Collectors.toList());
  }

  private static String miss(@Nullable String v, boolean missing) {

    if (!missing) {

      return v == null ? "" : v;
    }

    if (v == null || v.isBlank()) {

      return AnalyticsService.NA;
    }

    return v;
  }

  public List<Row> getTopNTableChart(
      String index,
      Optional<DateRange> range,
      String groupField,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNot,
      Optional<String> uniqueOn,
      int maxRows,
      Function<String, Cell> makeCell) {

    guardUsage(index);

    WherePred w = whereUsage(range, filters, mustNot);

    String gf = mappedField(normalizeFk(groupField));

    String agg =
        uniqueOn.map(u -> "COUNT(DISTINCT " + mappedField(normalizeFk(u)) + ")").orElse("COUNT(*)");

    String sql =
        "SELECT "
            + gf
            + " gb , "
            + agg
            + " cnt FROM "
            + tbl()
            + " WHERE "
            + w.predicate
            + " GROUP BY gb ORDER BY cnt DESC LIMIT "
            + Math.max(1, maxRows);

    List<Row> rows = new ArrayList<>();

    try (Connection c = cn();
        PreparedStatement ps = c.prepareStatement(sql)) {

      bind(ps, 1, w);

      try (ResultSet rs = ps.executeQuery()) {

        while (rs.next()) {

          String gv = rs.getString(1);

          if (gv == null || gv.isBlank()) {
            // Match ES terms: omit missing group keys from top-N tables.
            continue;
          }

          int cnt = rs.getInt(2);

          rows.add(
              new Row(
                  ImmutableList.of(gv, String.valueOf(cnt)),
                  ImmutableList.of(
                      makeCell.apply(gv), Cell.builder().setValue(String.valueOf(cnt)).build())));
        }
      }

    } catch (Exception ex) {

      log.error("Postgres analytics top-N error", ex);

      return ImmutableList.of();
    }

    return rows;
  }

  public int getHighlights(
      String index,
      Optional<DateRange> range,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNot,
      Optional<String> uniqueOn) {

    guardUsage(index);

    WherePred w = whereUsage(range, filters, mustNot);

    String agg =
        uniqueOn.map(u -> "COUNT(DISTINCT " + mappedField(normalizeFk(u)) + ")").orElse("COUNT(*)");

    String sql = "SELECT " + agg + " FROM " + tbl() + " WHERE " + w.predicate;

    try (Connection c = cn();
        PreparedStatement ps = c.prepareStatement(sql)) {

      bind(ps, 1, w);

      try (ResultSet rs = ps.executeQuery()) {

        return rs.next() ? rs.getInt(1) : 0;
      }

    } catch (Exception ex) {

      log.error("Postgres analytics highlights error", ex);

      return 0;
    }
  }

  private boolean canUseRollups(DateInterval granularity, DateRange dateRange) {
    // WEEK/YEAR need rebucketed points; use raw until rollup aggregation matches.
    return switch (granularity) {
      case HOUR, DAY, MONTH -> isRangeAlignedToGrain(dateRange, rollupGrain(granularity));
      default -> false;
    };
  }

  private String rollupGrain(DateInterval granularity) {
    return switch (granularity) {
      case HOUR -> AnalyticsMetricFamilies.GRAIN_HOUR;
      case DAY, WEEK -> AnalyticsMetricFamilies.GRAIN_DAY;
      case MONTH, YEAR -> AnalyticsMetricFamilies.GRAIN_MONTH;
      default -> AnalyticsMetricFamilies.GRAIN_HOUR;
    };
  }

  private boolean isRangeAlignedToGrain(DateRange dateRange, String grain) {
    try {
      Instant start = Instant.ofEpochMilli(Long.parseLong(dateRange.getStart()));
      Instant end = Instant.ofEpochMilli(Long.parseLong(dateRange.getEnd()));
      if (AnalyticsMetricFamilies.GRAIN_HOUR.equals(grain)) {
        return start.equals(PostgresAnalyticsUtc.truncateToUtcHour(start))
            && end.equals(PostgresAnalyticsUtc.truncateToUtcHour(end));
      }
      if (AnalyticsMetricFamilies.GRAIN_DAY.equals(grain)) {
        return start.equals(PostgresAnalyticsUtc.truncateToUtcDay(start))
            && end.equals(PostgresAnalyticsUtc.truncateToUtcDay(end));
      }
      if (AnalyticsMetricFamilies.GRAIN_MONTH.equals(grain)) {
        return start.equals(PostgresAnalyticsUtc.truncateToUtcMonth(start))
            && end.equals(PostgresAnalyticsUtc.truncateToUtcMonth(end));
      }
      return false;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isRangeSealed(DateRange dateRange, String grain) {
    try {
      Instant start = Instant.ofEpochMilli(Long.parseLong(dateRange.getStart()));
      Instant end = Instant.ofEpochMilli(Long.parseLong(dateRange.getEnd()));
      List<String> keys = new ArrayList<>();
      String layer;
      if (AnalyticsMetricFamilies.GRAIN_HOUR.equals(grain)) {
        layer = AnalyticsMetricFamilies.LAYER_HOUR;
        Instant cursor = PostgresAnalyticsUtc.truncateToUtcHour(start);
        while (cursor.isBefore(end)) {
          keys.add(PostgresAnalyticsUtc.partitionKeyHour(cursor));
          cursor = cursor.plusSeconds(3600);
        }
      } else if (AnalyticsMetricFamilies.GRAIN_DAY.equals(grain)) {
        layer = AnalyticsMetricFamilies.LAYER_DAY;
        Instant cursor = PostgresAnalyticsUtc.truncateToUtcDay(start);
        while (cursor.isBefore(end)) {
          keys.add(PostgresAnalyticsUtc.partitionKeyDay(cursor));
          cursor = cursor.plusSeconds(86400);
        }
      } else {
        layer = AnalyticsMetricFamilies.LAYER_MONTH;
        Instant cursor = PostgresAnalyticsUtc.truncateToUtcMonth(start);
        while (cursor.isBefore(end)) {
          keys.add(PostgresAnalyticsUtc.partitionKeyMonth(cursor));
          cursor =
              java.time.YearMonth.from(cursor.atZone(ZoneOffset.UTC))
                  .plusMonths(1)
                  .atDay(1)
                  .atStartOfDay(ZoneOffset.UTC)
                  .toInstant();
        }
      }
      if (keys.isEmpty()) {
        return true;
      }
      return store.getSealedPartitionKeys(layer, AnalyticsMetricFamilies.DATAHUB_USAGE, keys).size()
          == keys.size();
    } catch (Exception e) {
      log.debug("Seal check failed; falling back to raw", e);
      return false;
    }
  }

  @Nullable
  private List<NamedLine> getTimeseriesFromRollup(
      DateRange dateRange,
      DateInterval granularity,
      Optional<String> dimension,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn) {
    if (uniqueOn.isPresent()) {
      return null;
    }
    // Hourly materialization only persists event_type group dims; other dimensions need raw.
    if (dimension.isPresent() && !"event_type".equals(normalizeFk(dimension.get()))) {
      return null;
    }
    String grain = rollupGrain(granularity);
    if (!isRangeSealed(dateRange, grain)) {
      return null;
    }
    if (!filters.isEmpty() || !mustNotFilters.isEmpty()) {
      return null;
    }
    try {
      Instant start = Instant.ofEpochMilli(Long.parseLong(dateRange.getStart()));
      Instant end = Instant.ofEpochMilli(Long.parseLong(dateRange.getEnd()));
      String dimExpr =
          dimension
              .map(d -> "group_dims->>'" + normalizeFk(d).replace("'", "''") + "'")
              .orElse(null);
      String sql;
      if (dimExpr == null) {
        sql =
            "SELECT CAST(EXTRACT(EPOCH FROM bucket_start)*1000 AS BIGINT) bx, SUM(value_sum) cnt"
                + " FROM "
                + rollupTbl()
                + " WHERE metric_family = ? AND metric_name = 'event_count' AND merge_kind = ?"
                + " AND grain = ? AND bucket_start >= ? AND bucket_start < ?"
                + " GROUP BY bx ORDER BY bx";
      } else {
        sql =
            "SELECT CAST(EXTRACT(EPOCH FROM bucket_start)*1000 AS BIGINT) bx, "
                + dimExpr
                + " dx, SUM(value_sum) cnt FROM "
                + rollupTbl()
                + " WHERE metric_family = ? AND metric_name = 'event_count' AND merge_kind = ?"
                + " AND grain = ? AND bucket_start >= ? AND bucket_start < ?"
                + " GROUP BY bx, dx ORDER BY bx";
      }
      Map<String, TreeMap<String, Integer>> seriesDim = new LinkedHashMap<>();
      try (Connection c = cn();
          PreparedStatement ps = c.prepareStatement(sql)) {
        ps.setString(1, AnalyticsMetricFamilies.DATAHUB_USAGE);
        ps.setString(2, AnalyticsMetricFamilies.MERGE_ADDITIVE);
        ps.setString(3, grain);
        ps.setTimestamp(4, Timestamp.from(start));
        ps.setTimestamp(5, Timestamp.from(end));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String lx = ISO_BUCKET.format(Instant.ofEpochMilli(rs.getLong("bx")));
            int cnt = (int) Math.round(rs.getDouble("cnt"));
            if (dimExpr == null) {
              seriesDim.computeIfAbsent("__", __ -> new TreeMap<>()).merge(lx, cnt, Integer::sum);
            } else {
              String d = rs.getString("dx");
              if (d == null || d.isBlank()) {
                continue;
              }
              seriesDim.computeIfAbsent(d, __ -> new TreeMap<>()).merge(lx, cnt, Integer::sum);
            }
          }
        }
      }
      return toNamedLines(seriesDim, dimExpr != null, dateRange, granularity);
    } catch (Exception e) {
      log.debug("Rollup timeseries failed; falling back to raw", e);
      return null;
    }
  }
}
