package com.linkedin.metadata.timeseries.postgres;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.TimeseriesFieldCollectionSpec;
import com.linkedin.metadata.models.TimeseriesFieldSpec;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
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
    int bi = 0;
    for (GroupingBucket b : buckets) {
      String alias = "g" + (bi++);
      groupAliases.add(alias);
      if (b.getType() == GroupingBucketType.DATE_GROUPING_BUCKET) {
        ZoneId z = zoneForBucket(b);
        String millisExpr = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql(b.getKey());
        String trunc = postgresDateTrunc(b.getTimeWindowSize());
        groupSql.add(
            "date_trunc('"
                + trunc
                + "', to_timestamp(("
                + millisExpr
                + ")::double precision / 1000.0) AT TIME ZONE 'UTC' AT TIME ZONE '"
                + z.getId().replace("'", "''")
                + "') AS "
                + alias);
      } else if (b.getType() == GroupingBucketType.STRING_GROUPING_BUCKET) {
        groupSql.add(
            PostgresTimeseriesAggregatedStatsDao.documentTextPathSql(b.getKey()) + " AS " + alias);
      } else {
        throw new UnsupportedOperationException("Unsupported grouping bucket type: " + b.getType());
      }
    }

    List<String> metricSql = new ArrayList<>();
    List<String> metricColumnNames = new ArrayList<>();
    for (AggregationSpec spec : aggregationSpecs) {
      String path = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql(spec.getFieldPath());
      String colName = getAggregationSpecAggDisplayName(spec);
      metricColumnNames.add(colName);
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

    StringBuilder sql = new StringBuilder("SELECT ");
    sql.append(String.join(", ", groupSql));
    if (!groupSql.isEmpty() && !metricSql.isEmpty()) {
      sql.append(", ");
    }
    sql.append(String.join(", ", metricSql));
    sql.append(" FROM ").append(qualifiedTable);
    sql.append(" WHERE entity_name = ? AND aspect_name = ? ");
    sql.append(" AND (").append(filterSql.getExpression()).append(")");
    if (!groupSql.isEmpty()) {
      sql.append(" GROUP BY ");
      sql.append(String.join(", ", groupAliases));
      sql.append(" ORDER BY ");
      sql.append(String.join(", ", groupAliases));
    }

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
    try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
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
            row.add(v == null ? ES_NULL_VALUE : String.valueOf(v));
          }
          rows.add(new StringArray(row));
        }
      }
    }

    GenericTable table = new GenericTable();
    table.setColumnNames(new StringArray(columnNames));
    table.setColumnTypes(new StringArray(columnTypes));
    table.setRows(new StringArrayArray(rows));
    return table;
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

  private static String sqlSafeAlias(String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
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

  private static DataSchema.Type getTimeseriesFieldType(AspectSpec aspectSpec, String fieldPath) {
    if ("timestampMillis".equals(fieldPath)) {
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

  private static String postgresDateTrunc(TimeWindowSize tws) {
    switch (tws.getUnit()) {
      case MINUTE:
      case HOUR:
        return "hour";
      case DAY:
        return "day";
      case WEEK:
        return "week";
      case MONTH:
        return "month";
      default:
        return "day";
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
