package com.linkedin.metadata.timeseries.postgres;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.TimeseriesFieldCollectionSpec;
import com.linkedin.metadata.models.TimeseriesFieldSpec;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * PostgreSQL implementation of timeseries {@link GenericTable} aggregations (subset of {@link
 * com.linkedin.metadata.timeseries.elastic.query.ESAggregatedStatsDAO}).
 */
@Slf4j
public final class PostgresTimeseriesAggregatedStatsDao {

  private PostgresTimeseriesAggregatedStatsDao() {}

  static final String ES_NULL_VALUE = "NULL";

  public static GenericTable getAggregatedStats(
      @Nonnull Connection connection,
      @Nonnull String qualifiedTable,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nullable GroupingBucket[] groupingBuckets,
      @Nonnull TimeseriesFilterSqlBuilder.BuiltSql filterSql)
      throws SQLException {

    GroupingBucket[] buckets = groupingBuckets == null ? new GroupingBucket[0] : groupingBuckets;

    List<String> groupAliases = new ArrayList<>();
    List<String> groupSql = new ArrayList<>();
    List<String> stringGroupPathExprs = new ArrayList<>();
    int bi = 0;
    for (GroupingBucket b : buckets) {
      String alias = "g" + (bi++);
      groupAliases.add(alias);
      if (b.getType() == GroupingBucketType.DATE_GROUPING_BUCKET) {
        ZoneId z = zoneForBucket(b);
        String millisExpr = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql(b.getKey());
        groupSql.add(postgresDateBucketSql(b.getTimeWindowSize(), millisExpr, z) + " AS " + alias);
      } else if (b.getType() == GroupingBucketType.STRING_GROUPING_BUCKET) {
        String pathExpr = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql(b.getKey());
        // ES terms buckets omit missing values; parent (non-exploded) rows lack collection fields
        // such as userCounts.user, so exclude NULL/empty keys to avoid a phantom NULL group.
        stringGroupPathExprs.add(pathExpr);
        groupSql.add(pathExpr + " AS " + alias);
      } else {
        throw new UnsupportedOperationException("Unsupported grouping bucket type: " + b.getType());
      }
    }

    List<String> metricSql = new ArrayList<>();
    List<String> metricColumnNames = new ArrayList<>();
    List<AggregationType> metricAggTypes = new ArrayList<>();
    List<DataSchema.Type> metricMemberTypes = new ArrayList<>();
    for (AggregationSpec spec : aggregationSpecs) {
      String path = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql(spec.getFieldPath());
      String colName = getAggregationSpecAggDisplayName(spec);
      metricColumnNames.add(colName);
      metricAggTypes.add(spec.getAggregationType());
      metricMemberTypes.add(getTimeseriesFieldType(aspectSpec, spec.getFieldPath()));
      String sqlAlias = sqlSafeAlias(colName);
      switch (spec.getAggregationType()) {
        case SUM:
          metricSql.add(
              "SUM(CASE WHEN "
                  + path
                  + " IS NOT NULL AND "
                  + path
                  + " <> '' THEN ("
                  + path
                  + ")::double precision ELSE 0 END) AS "
                  + sqlAlias);
          break;
        case CARDINALITY:
          metricSql.add(
              "COUNT(DISTINCT CASE WHEN "
                  + path
                  + " IS NOT NULL AND "
                  + path
                  + " <> '' THEN "
                  + path
                  + " END) AS "
                  + sqlAlias);
          break;
        case LATEST:
          metricSql.add(
              "(ARRAY_AGG("
                  + path
                  + " ORDER BY event_time DESC NULLS LAST) FILTER (WHERE "
                  + path
                  + " IS NOT NULL AND trim("
                  + path
                  + ") <> ''))[1] AS "
                  + sqlAlias);
          break;
        default:
          throw new IllegalStateException(spec.getAggregationType().toString());
      }
    }

    String sql =
        buildAggregatedStatsSql(
            qualifiedTable,
            groupSql,
            groupAliases,
            metricSql,
            metricColumnNames,
            stringGroupPathExprs,
            buckets,
            aggregationSpecs,
            filterSql.getExpression());

    List<Object> params = new ArrayList<>();
    params.add(entityName);
    params.add(aspectName);
    params.addAll(filterSql.getParams());

    List<String> columnNames = new ArrayList<>();
    for (GroupingBucket gb : buckets) {
      columnNames.add(gb.getKey());
    }
    columnNames.addAll(metricColumnNames);

    List<String> columnTypes = new ArrayList<>();
    for (GroupingBucket gb : buckets) {
      columnTypes.add(bucketColumnType(aspectSpec, gb));
    }
    for (AggregationSpec spec : aggregationSpecs) {
      DataSchema.Type memberType = getTimeseriesFieldType(aspectSpec, spec.getFieldPath());
      switch (spec.getAggregationType()) {
        case SUM:
          columnTypes.add("double");
          break;
        case CARDINALITY:
          columnTypes.add("long");
          break;
        case LATEST:
          columnTypes.add(memberType.toString().toLowerCase());
          break;
        default:
          columnTypes.add("string");
      }
    }

    List<StringArray> rows = new ArrayList<>();
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      bindParams(ps, params);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          List<String> row = new ArrayList<>();
          int col = 1;
          for (int i = 0; i < groupAliases.size(); i++) {
            Object o = rs.getObject(col++);
            row.add(formatGroupCell(o));
          }
          for (int j = 0; j < metricColumnNames.size(); j++) {
            Object v = rs.getObject(col++);
            row.add(formatMetricCell(v, metricAggTypes.get(j), metricMemberTypes.get(j)));
          }
          rows.add(new StringArray(row));
        }
      }
    }

    rows = fillEmptyDateBuckets(rows, buckets, metricColumnNames.size());

    GenericTable table = new GenericTable();
    table.setColumnNames(new StringArray(columnNames));
    table.setColumnTypes(new StringArray(columnTypes));
    table.setRows(new StringArrayArray(rows));
    return table;
  }

  /**
   * Whether to gap-fill empty DATE grouping buckets between the first and last observed bucket.
   *
   * <p>Mirrors {@link
   * com.linkedin.metadata.timeseries.elastic.query.ESAggregatedStatsDAO#shouldIncludeEmptyDateBuckets}
   * ({@code min_doc_count=0} for DAY+). HOUR/MINUTE stay sparse to avoid huge result sets.
   */
  static boolean shouldIncludeEmptyDateBuckets(@Nullable TimeWindowSize timeWindowSize) {
    if (timeWindowSize == null || !timeWindowSize.hasUnit()) {
      return false;
    }
    CalendarInterval unit = timeWindowSize.getUnit();
    return unit == CalendarInterval.DAY
        || unit == CalendarInterval.WEEK
        || unit == CalendarInterval.MONTH
        || unit == CalendarInterval.QUARTER
        || unit == CalendarInterval.YEAR;
  }

  /**
   * Insert empty interstitial DATE buckets with {@link #ES_NULL_VALUE} metrics for the single-DATE
   * grouping path used by usage/operations DAY+ queries.
   */
  @Nonnull
  static List<StringArray> fillEmptyDateBuckets(
      @Nonnull List<StringArray> rows, @Nonnull GroupingBucket[] buckets, int metricColumnCount) {
    if (rows.isEmpty() || buckets.length != 1) {
      return rows;
    }
    GroupingBucket bucket = buckets[0];
    if (bucket.getType() != GroupingBucketType.DATE_GROUPING_BUCKET) {
      return rows;
    }
    TimeWindowSize timeWindowSize = bucket.getTimeWindowSize();
    if (!shouldIncludeEmptyDateBuckets(timeWindowSize)) {
      return rows;
    }

    ZoneId zone = zoneForBucket(bucket);
    CalendarInterval unit = timeWindowSize.getUnit();
    int multiple = timeWindowSize.hasMultiple() ? timeWindowSize.getMultiple() : 1;
    if (multiple < 1) {
      multiple = 1;
    }

    Map<Long, StringArray> byTimestamp = new LinkedHashMap<>();
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (StringArray row : rows) {
      if (row.isEmpty()) {
        continue;
      }
      long timestampMillis;
      try {
        timestampMillis = Long.parseLong(row.get(0));
      } catch (NumberFormatException e) {
        return rows;
      }
      byTimestamp.put(timestampMillis, row);
      min = Math.min(min, timestampMillis);
      max = Math.max(max, timestampMillis);
    }
    if (byTimestamp.isEmpty() || min > max) {
      return rows;
    }

    List<StringArray> filled = new ArrayList<>();
    long cursor = min;
    int safety = 0;
    final int maxBuckets = 100_000;
    while (cursor <= max) {
      StringArray existing = byTimestamp.get(cursor);
      if (existing != null) {
        filled.add(existing);
      } else {
        List<String> cells = new ArrayList<>(1 + metricColumnCount);
        cells.add(String.valueOf(cursor));
        for (int i = 0; i < metricColumnCount; i++) {
          cells.add(ES_NULL_VALUE);
        }
        filled.add(new StringArray(cells));
      }
      long next = nextBucketEpochMillis(cursor, unit, multiple, zone);
      if (next <= cursor || ++safety > maxBuckets) {
        log.warn(
            "Stopping date bucket gap-fill after {} steps (cursor={}, next={}, max={})",
            safety,
            cursor,
            next,
            max);
        break;
      }
      cursor = next;
    }
    return filled;
  }

  static long nextBucketEpochMillis(
      long epochMillis, @Nonnull CalendarInterval unit, int multiple, @Nonnull ZoneId zone) {
    ZonedDateTime zdt = Instant.ofEpochMilli(epochMillis).atZone(zone);
    ZonedDateTime next;
    switch (unit) {
      case DAY:
        next = zdt.plusDays(multiple);
        break;
      case WEEK:
        next = zdt.plusWeeks(multiple);
        break;
      case MONTH:
        next = zdt.plusMonths(multiple);
        break;
      case QUARTER:
        next = zdt.plusMonths(3L * multiple);
        break;
      case YEAR:
        next = zdt.plusYears(multiple);
        break;
      default:
        throw new IllegalArgumentException("Unsupported gap-fill calendar unit: " + unit);
    }
    return next.toInstant().toEpochMilli();
  }

  private static String formatGroupCell(@Nullable Object o) {
    if (o == null) {
      return ES_NULL_VALUE;
    }
    if (o instanceof Timestamp) {
      return String.valueOf(((Timestamp) o).getTime());
    }
    return String.valueOf(o);
  }

  /**
   * Match {@link com.linkedin.metadata.timeseries.elastic.query.ESAggregatedStatsDAO} cell
   * formatting: integral SUM values as long strings ({@code "6"} not {@code "6.0"}), cardinality as
   * long.
   */
  @Nonnull
  static String formatMetricCell(
      @Nullable Object value,
      @Nonnull AggregationType aggregationType,
      @Nonnull DataSchema.Type memberType) {
    if (value == null) {
      return ES_NULL_VALUE;
    }
    switch (aggregationType) {
      case SUM:
        switch (memberType) {
          case INT:
          case LONG:
            return String.valueOf(toLong(value));
          case DOUBLE:
          case FLOAT:
            return String.valueOf(toDouble(value));
          default:
            return String.valueOf(toDouble(value));
        }
      case CARDINALITY:
        return String.valueOf(toLong(value));
      case LATEST:
      default:
        return String.valueOf(value);
    }
  }

  private static long toLong(@Nonnull Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return new BigDecimal(value.toString().trim()).longValue();
  }

  private static double toDouble(@Nonnull Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    return Double.parseDouble(value.toString().trim());
  }

  private static String sqlSafeAlias(String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }

  /** Mirrors ES {@code MAX_TERM_BUCKETS} default for string terms aggregations. */
  static final int MAX_TERM_BUCKETS = 24 * 60;

  /**
   * Builds the aggregation SELECT (no bind params). Package-visible for unit tests covering ORDER
   * BY, per-parent string limits, and timezone truncation.
   */
  @Nonnull
  static String buildAggregatedStatsSql(
      @Nonnull String qualifiedTable,
      @Nonnull List<String> groupSql,
      @Nonnull List<String> groupAliases,
      @Nonnull List<String> metricSql,
      @Nonnull List<String> metricColumnNames,
      @Nonnull List<String> stringGroupPathExprs,
      @Nonnull GroupingBucket[] buckets,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nonnull String filterExpression) {
    StringBuilder inner = new StringBuilder("SELECT ");
    inner.append(String.join(", ", groupSql));
    if (!groupSql.isEmpty() && !metricSql.isEmpty()) {
      inner.append(", ");
    }
    inner.append(String.join(", ", metricSql));
    inner.append(" FROM ").append(qualifiedTable);
    inner.append(" WHERE entity_name = ? AND aspect_name = ? ");
    inner.append(" AND (").append(filterExpression).append(")");
    for (String pathExpr : stringGroupPathExprs) {
      inner
          .append(" AND ")
          .append(pathExpr)
          .append(" IS NOT NULL AND ")
          .append(pathExpr)
          .append(" <> ''");
    }
    if (groupSql.isEmpty()) {
      return inner.toString();
    }
    inner.append(" GROUP BY ");
    inner.append(String.join(", ", groupAliases));
    String orderBy = buildGroupOrderBy(buckets, groupAliases, metricColumnNames, aggregationSpecs);
    inner.append(" ORDER BY ");
    inner.append(orderBy);

    Integer stringGroupLimit = stringGroupingLimit(buckets);
    if (stringGroupLimit == null) {
      return inner.toString();
    }

    // ES applies terms size per parent bucket; wrap with ROW_NUMBER partitioned by parents of the
    // innermost STRING grouping (date→string production path).
    int lastStringIdx = lastStringGroupingIndex(buckets);
    List<String> parentAliases = groupAliases.subList(0, Math.max(0, lastStringIdx));
    List<String> selectCols = new ArrayList<>(groupAliases);
    for (String metricCol : metricColumnNames) {
      selectCols.add(sqlSafeAlias(metricCol));
    }
    String selectList = String.join(", ", selectCols);
    StringBuilder ranked = new StringBuilder("SELECT ");
    ranked.append(selectList);
    ranked.append(" FROM (SELECT ");
    ranked.append(selectList);
    ranked.append(", ROW_NUMBER() OVER (");
    if (!parentAliases.isEmpty()) {
      ranked.append("PARTITION BY ").append(String.join(", ", parentAliases)).append(" ");
    }
    ranked.append("ORDER BY ").append(orderBy);
    ranked.append(") AS _rn FROM (");
    ranked.append(inner);
    ranked.append(") _g) _r WHERE _rn <= ").append(stringGroupLimit);
    ranked.append(" ORDER BY ").append(orderBy);
    return ranked.toString();
  }

  /**
   * ORDER BY for grouped rows: string buckets honor {@code ascending} / {@code orderByMetric}; date
   * buckets keep group-key order.
   */
  @Nonnull
  static String buildGroupOrderBy(
      @Nonnull GroupingBucket[] buckets,
      @Nonnull List<String> groupAliases,
      @Nonnull List<String> metricColumnNames,
      @Nonnull AggregationSpec[] aggregationSpecs) {
    List<String> orderParts = new ArrayList<>();
    boolean anyString = false;
    for (int i = 0; i < buckets.length; i++) {
      GroupingBucket b = buckets[i];
      if (b.getType() != GroupingBucketType.STRING_GROUPING_BUCKET) {
        orderParts.add(groupAliases.get(i));
        continue;
      }
      anyString = true;
      boolean asc = !b.hasAscending() || b.isAscending();
      String dir = asc ? " ASC" : " DESC";
      if (b.hasOrderByMetric()
          && b.isOrderByMetric()
          && aggregationSpecs.length > 0
          && !metricColumnNames.isEmpty()) {
        orderParts.add(sqlSafeAlias(metricColumnNames.get(0)) + dir);
      } else {
        orderParts.add(groupAliases.get(i) + dir);
      }
    }
    if (!anyString) {
      return String.join(", ", groupAliases);
    }
    return String.join(", ", orderParts);
  }

  /**
   * Size for the innermost STRING grouping bucket (ES terms size within each parent). Defaults to
   * {@link #MAX_TERM_BUCKETS}.
   */
  @Nullable
  static Integer stringGroupingLimit(@Nonnull GroupingBucket[] buckets) {
    int lastStringIdx = lastStringGroupingIndex(buckets);
    if (lastStringIdx < 0) {
      return null;
    }
    GroupingBucket b = buckets[lastStringIdx];
    if (b.hasSize() && b.getSize() > 0) {
      return b.getSize();
    }
    return MAX_TERM_BUCKETS;
  }

  private static int lastStringGroupingIndex(@Nonnull GroupingBucket[] buckets) {
    for (int i = buckets.length - 1; i >= 0; i--) {
      if (buckets[i].getType() == GroupingBucketType.STRING_GROUPING_BUCKET) {
        return i;
      }
    }
    return -1;
  }

  private static String getAggregationSpecAggDisplayName(AggregationSpec aggregationSpec) {
    String prefix;
    switch (aggregationSpec.getAggregationType()) {
      case LATEST:
        prefix = "latest_";
        break;
      case SUM:
        prefix = "sum_";
        break;
      case CARDINALITY:
        prefix = "cardinality_";
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown AggregationSpec type " + aggregationSpec.getAggregationType());
    }
    return prefix + aggregationSpec.getFieldPath();
  }

  private static String bucketColumnType(AspectSpec aspectSpec, GroupingBucket gb) {
    DataSchema.Type t = getTimeseriesFieldType(aspectSpec, gb.getKey());
    switch (t) {
      case INT:
      case LONG:
      case DOUBLE:
      case FLOAT:
        return t.toString().toLowerCase();
      default:
        return "string";
    }
  }

  /** Resolved member type for a timeseries field path (parity with ESAggregatedStatsDAO). */
  @Nonnull
  static DataSchema.Type getTimeseriesFieldType(AspectSpec aspectSpec, String fieldPath) {
    if ("timestampMillis".equals(fieldPath) || "@timestamp".equals(fieldPath)) {
      return DataSchema.Type.LONG;
    }
    String[] memberParts = fieldPath.split("\\.");
    if (memberParts.length == 1) {
      TimeseriesFieldSpec ts = aspectSpec.getTimeseriesFieldSpecMap().get(memberParts[0]);
      if (ts != null) {
        return ts.getPegasusSchema().getType();
      }
      TimeseriesFieldCollectionSpec coll =
          aspectSpec.getTimeseriesFieldCollectionSpecMap().get(memberParts[0]);
      if (coll != null) {
        return coll.getPegasusSchema().getType();
      }
    } else if (memberParts.length == 2) {
      TimeseriesFieldCollectionSpec coll =
          aspectSpec.getTimeseriesFieldCollectionSpecMap().get(memberParts[0]);
      if (coll != null) {
        if (coll.getTimeseriesFieldCollectionAnnotation().getKey().equals(memberParts[1])) {
          return DataSchema.Type.STRING;
        }
        TimeseriesFieldSpec tsFieldSpec = coll.getTimeseriesFieldSpecMap().get(memberParts[1]);
        if (tsFieldSpec != null) {
          return tsFieldSpec.getPegasusSchema().getType();
        }
      }
    }
    return DataSchema.Type.STRING;
  }

  private static ZoneId zoneForBucket(GroupingBucket groupingBucket) {
    ZoneId zoneId = ZoneId.of("GMT");
    if (groupingBucket.getTimeZone() != null) {
      try {
        zoneId = ZoneId.of(groupingBucket.getTimeZone());
      } catch (Exception e) {
        log.error("Invalid timezone {}", groupingBucket.getTimeZone(), e);
      }
    }
    return zoneId;
  }

  /**
   * SQL expression that buckets a millisecond epoch field into the same calendar windows as {@link
   * com.linkedin.metadata.timeseries.elastic.query.ESAggregatedStatsDAO}, including {@code
   * TimeWindowSize.multiple}.
   *
   * <p>When {@code multiple > 1}, {@code date_trunc} alone would emit every unit boundary; gap-fill
   * then advances by {@code multiple} and can omit real rows. Align the truncated timestamp down to
   * a multiple of the unit so SQL grouping and {@link #fillEmptyDateBuckets} share the same keys.
   */
  @Nonnull
  static String postgresDateBucketSql(
      @Nonnull TimeWindowSize tws, @Nonnull String millisExpr, @Nonnull ZoneId zone) {
    String zoneEsc = zone.getId().replace("'", "''");
    String trunc = postgresDateTrunc(tws);
    String localTs =
        "to_timestamp(("
            + millisExpr
            + ")::double precision / 1000.0) AT TIME ZONE '"
            + zoneEsc
            + "'";
    String truncated = "date_trunc('" + trunc + "', " + localTs + ")";
    int multiple = tws.hasMultiple() ? tws.getMultiple() : 1;
    if (multiple < 1) {
      multiple = 1;
    }
    String bucketLocal =
        multiple == 1 ? truncated : alignDateTruncToMultiple(truncated, tws.getUnit(), multiple);
    // Second AT TIME ZONE converts the local wall-clock bucket start back to timestamptz.
    return bucketLocal + " AT TIME ZONE '" + zoneEsc + "'";
  }

  /**
   * Floors a {@code date_trunc}'d timestamp (without time zone) to a multiple of the calendar unit.
   */
  @Nonnull
  static String alignDateTruncToMultiple(
      @Nonnull String truncatedSql, @Nonnull CalendarInterval unit, int multiple) {
    switch (unit) {
      case SECOND:
        return truncatedSql
            + " - ((EXTRACT(EPOCH FROM "
            + truncatedSql
            + ")::bigint % "
            + multiple
            + ") * INTERVAL '1 second')";
      case MINUTE:
        return truncatedSql
            + " - (((EXTRACT(EPOCH FROM "
            + truncatedSql
            + ")::bigint / 60) % "
            + multiple
            + ") * INTERVAL '1 minute')";
      case HOUR:
        return truncatedSql
            + " - (((EXTRACT(EPOCH FROM "
            + truncatedSql
            + ")::bigint / 3600) % "
            + multiple
            + ") * INTERVAL '1 hour')";
      case DAY:
        return truncatedSql
            + " - (((EXTRACT(EPOCH FROM "
            + truncatedSql
            + ")::bigint / 86400) % "
            + multiple
            + ") * INTERVAL '1 day')";
      case WEEK:
        return truncatedSql
            + " - (((EXTRACT(EPOCH FROM "
            + truncatedSql
            + ")::bigint / 604800) % "
            + multiple
            + ") * INTERVAL '1 week')";
      case MONTH:
        return truncatedSql
            + " - make_interval(months => ((EXTRACT(YEAR FROM "
            + truncatedSql
            + ")::int * 12 + EXTRACT(MONTH FROM "
            + truncatedSql
            + ")::int - 1) % "
            + multiple
            + "))";
      case QUARTER:
        return truncatedSql
            + " - make_interval(months => (((EXTRACT(YEAR FROM "
            + truncatedSql
            + ")::int * 4 + EXTRACT(QUARTER FROM "
            + truncatedSql
            + ")::int - 1) % "
            + multiple
            + ") * 3))";
      case YEAR:
        return truncatedSql
            + " - make_interval(years => (EXTRACT(YEAR FROM "
            + truncatedSql
            + ")::int % "
            + multiple
            + "))";
      default:
        throw new IllegalArgumentException(
            "Unknown date grouping bucket time window size unit: " + unit);
    }
  }

  /** Maps {@link TimeWindowSize} unit to a PostgreSQL {@code date_trunc} field. */
  @Nonnull
  static String postgresDateTrunc(@Nonnull TimeWindowSize tws) {
    switch (tws.getUnit()) {
      case SECOND:
        return "second";
      case MINUTE:
        return "minute";
      case HOUR:
        return "hour";
      case DAY:
        return "day";
      case WEEK:
        return "week";
      case MONTH:
        return "month";
      case QUARTER:
        return "quarter";
      case YEAR:
        return "year";
      default:
        throw new IllegalArgumentException(
            "Unknown date grouping bucket time window size unit: " + tws.getUnit());
    }
  }

  /** SQL expression extracting text from {@code document} for dotted paths. */
  @Nonnull
  static String documentTextPathSql(@Nonnull String dottedField) {
    String[] segs = dottedField.split("\\.");
    if (segs.length == 1) {
      return "document->>'" + escapeIdentKey(segs[0]) + "'";
    }
    String arr =
        "ARRAY["
            + java.util.Arrays.stream(segs)
                .map(s -> "'" + escapeIdentKey(s) + "'")
                .collect(Collectors.joining(","))
            + "]::text[]";
    return "document #>> " + arr;
  }

  private static String escapeIdentKey(String s) {
    return s.replace("'", "''");
  }

  private static void bindParams(PreparedStatement ps, List<Object> params) throws SQLException {
    PostgresPreparedBinder.bind(ps, params);
  }
}
