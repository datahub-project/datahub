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
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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

/** JDBC analytics backed by partitioned usage events ({@code postgres} backend). */
@Slf4j
@RequiredArgsConstructor
public class PostgresUsageEventsAnalyticsQueries {

  private static final DateTimeFormatter ISO_BUCKET =
      DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

  private final PostgresUsageEventsStore store;
  private final IndexConvention indexConvention;

  private static String placeholders(int count) {

    List<String> p = new ArrayList<>();

    for (int i = 0; i < count; i++) {

      p.add("?");
    }

    return String.join(",", p);
  }

  private String tbl() {

    return store.qualifiedParentTable();
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

    parts.add("(usage_source IS NULL OR usage_source <> 'backend')");

    range.ifPresent(
        dr -> {
          parts.add("timestamp_ms >= ?");

          parts.add("timestamp_ms < ?");

          binds.add(Long.parseLong(dr.getStart()));

          binds.add(Long.parseLong(dr.getEnd()));
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
        return PostgresUsageEventsStore.COL_ACTOR_URN;

      case DataHubUsageEventConstants.ENTITY_URN:
        return PostgresUsageEventsStore.COL_ENTITY_URN;

      case DataHubUsageEventConstants.ENTITY_TYPE:
        return PostgresUsageEventsStore.COL_ENTITY_TYPE;

      case DataHubUsageEventConstants.TYPE:
        return PostgresUsageEventsStore.COL_EVENT_TYPE;

      case DataHubUsageEventConstants.QUERY:
        return "COALESCE("
            + PostgresUsageEventsStore.COL_QUERY
            + ",document->>'"
            + DataHubUsageEventConstants.QUERY
            + "')";

      case "browserId":
        return "COALESCE(" + PostgresUsageEventsStore.COL_BROWSER_ID + ",document->>'browserId')";

      case "removed":
        return "document->>'removed'";

      case "section":
        return "COALESCE(" + PostgresUsageEventsStore.COL_SECTION + ",document->>'section')";

      case "actionType":
        return "COALESCE(" + PostgresUsageEventsStore.COL_ACTION_TYPE + ",document->>'actionType')";

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

    } else {

      ps.setString(idx, String.valueOf(v));
    }
  }

  private Connection cn() throws SQLException {

    Connection c = store.getDatabase().dataSource().getConnection();

    c.setReadOnly(true);

    return c;
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

    return "CAST(EXTRACT(EPOCH FROM DATE_TRUNC('"
        + truncated
        + "', TIMESTAMP WITH TIME ZONE 'epoch' + timestamp_ms * INTERVAL '1 ms'))*1000 AS BIGINT)";
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

    WherePred w = whereUsage(Optional.of(dateRange), filters, mustNotFilters);

    String trunc = truncate(granularity);

    String bx = bucketStartMsSql(trunc);

    Optional<String> dimSql = dimension.map(d -> mappedField(normalizeFk(d)));

    String countExpr =
        uniqueOn
            .map(u -> "COUNT(DISTINCT COALESCE(" + mappedField(normalizeFk(u)) + ",''))")
            .orElse("COUNT(*)");

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

            if (d == null || d.isBlank()) {

              d = AnalyticsService.NA;
            }

            seriesDim.computeIfAbsent(d, __ -> new TreeMap<>()).merge(lx, cnt, Integer::sum);
          }
        }
      }

      if (dimSql.isEmpty()) {

        List<NumericDataPoint> pts =
            seriesDim.getOrDefault("__", new TreeMap<>()).entrySet().stream()
                .map(en -> new NumericDataPoint(en.getKey(), en.getValue()))
                .collect(Collectors.toList());

        return ImmutableList.of(new NamedLine("total", pts));
      }

      List<NamedLine> out = new ArrayList<>();

      for (Map.Entry<String, TreeMap<String, Integer>> e : seriesDim.entrySet()) {

        List<NumericDataPoint> pts =
            e.getValue().entrySet().stream()
                .map(en -> new NumericDataPoint(en.getKey(), en.getValue()))
                .collect(Collectors.toList());

        out.add(new NamedLine(e.getKey(), pts));
      }

      return out;

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
        uniqueOn
            .map(u -> "COUNT(DISTINCT COALESCE(" + mappedField(normalizeFk(u)) + ",''))")
            .orElse("COUNT(*)");

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

    return "SELECT "
        + label
        + ", "
        + agg
        + " cnt FROM "
        + tbl()
        + " WHERE "
        + pred
        + " GROUP BY grp ORDER BY cnt DESC LIMIT 500";
  }

  private String barSelectDouble(
      String e0, String e1, String agg, boolean showMissing, String pred) {

    String l0 = grpLabelAlias(e0, "g0", showMissing);

    String l1 = grpLabelAlias(e1, "g1", showMissing);

    return "SELECT "
        + l0
        + ","
        + l1
        + ","
        + agg
        + " cnt FROM "
        + tbl()
        + " WHERE "
        + pred
        + " GROUP BY g0,g1 ORDER BY cnt DESC LIMIT 500";
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
        uniqueOn
            .map(u -> "COUNT(DISTINCT COALESCE(" + mappedField(normalizeFk(u)) + ",''))")
            .orElse("COUNT(*)");

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

          if (gv == null) {

            gv = "";
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
        uniqueOn
            .map(u -> "COUNT(DISTINCT COALESCE(" + mappedField(normalizeFk(u)) + ",''))")
            .orElse("COUNT(*)");

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
}
