package com.linkedin.metadata.datahubusage.postgres;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Partitioned PostgreSQL storage for usage events (append + partition lifecycle). Analytics and
 * audit query helpers live beside write/maintain methods.
 */
@Slf4j
public class PostgresUsageEventsStore {

  public static final String COL_ID = "id";
  public static final String COL_TS = "timestamp_ms";
  public static final String COL_EVENT_TYPE = "event_type";
  public static final String COL_USAGE_SOURCE = "usage_source";
  public static final String COL_ACTOR_URN = "actor_urn";
  public static final String COL_ENTITY_URN = "entity_urn";
  public static final String COL_ENTITY_TYPE = "entity_type";
  public static final String COL_BROWSER_ID = "browser_id";
  public static final String COL_QUERY = "query";
  public static final String COL_SECTION = "section";
  public static final String COL_ACTION_TYPE = "action_type";
  public static final String COL_ASPECT_NAME = "aspect_name";
  public static final String COL_DOCUMENT = "document";

  private final Database database;
  private final PostgresSqlSetupProperties postgresSqlSetupProperties;
  private final int retentionMonths;
  private final int partitionsAheadMonths;

  private final AtomicBoolean parentEnsured = new AtomicBoolean(false);

  public PostgresUsageEventsStore(
      @Nonnull Database database,
      @Nonnull PostgresSqlSetupProperties postgresSqlSetupProperties,
      int retentionMonths,
      int partitionsAheadMonths) {
    this.database = database;
    this.postgresSqlSetupProperties = postgresSqlSetupProperties;
    this.retentionMonths = Math.max(1, retentionMonths);
    this.partitionsAheadMonths = Math.max(1, partitionsAheadMonths);
  }

  public Database getDatabase() {
    return database;
  }

  public String qualifiedParentTable() {
    return postgresSqlSetupProperties.normalizedPostgresSchema() + "." + parentTableName();
  }

  private String parentTableName() {
    return postgresSqlSetupProperties.normalizedPgUsageEventsParentTableName();
  }

  /** Full maintenance: ensure future partitions exist and drop aged partitions past retention. */
  public void runPartitionMaintenance() throws SQLException {
    ensureParentTable();
    YearMonth nowYm = YearMonth.now(ZoneOffset.UTC);
    YearMonth earliestToKeep = nowYm.minusMonths(retentionMonths + 6);
    YearMonth farthestAhead = nowYm.plusMonths(partitionsAheadMonths);
    ensurePartitionsCoverRangeInclusive(earliestToKeep, farthestAhead);
    dropExpiredPartitions();
  }

  public void ensureParentTable() throws SQLException {
    if (parentEnsured.get()) {
      return;
    }
    synchronized (this) {
      if (parentEnsured.get()) {
        return;
      }
      parentEnsured.set(true);
    }
  }

  void ensurePartitionsCoverRangeInclusive(YearMonth inclusiveStartYm, YearMonth inclusiveEndYm)
      throws SQLException {
    ensureParentTable();
    YearMonth lo = inclusiveStartYm.isBefore(inclusiveEndYm) ? inclusiveStartYm : inclusiveEndYm;
    YearMonth hi = inclusiveStartYm.isBefore(inclusiveEndYm) ? inclusiveEndYm : inclusiveStartYm;
    for (YearMonth ym = lo; !ym.isAfter(hi); ym = ym.plusMonths(1)) {
      ensurePartitionMonth(ym);
    }
  }

  public void ensurePartitionForInstantRange(long minTimestampMs, long maxTimestampMs)
      throws SQLException {
    YearMonth minYm =
        PostgresUsageEventsUtcTime.yearMonthFromUtcMillis(minTimestampMs).minusMonths(1);
    YearMonth maxYm =
        PostgresUsageEventsUtcTime.yearMonthFromUtcMillis(maxTimestampMs).plusMonths(1);
    ensurePartitionsCoverRangeInclusive(minYm, maxYm);
  }

  private void ensurePartitionMonth(YearMonth ym) throws SQLException {
    long fromMs = PostgresUsageEventsUtcTime.startOfUtcMonthMillis(ym);
    long toExclusive = PostgresUsageEventsUtcTime.endExclusiveUtcMonthMillis(ym);
    String partName =
        PostgresUsageEventsTable.partitionTableName(
            parentTableName(), ym.getYear(), ym.getMonthValue());
    String qualifiedPart = postgresSqlSetupProperties.normalizedPostgresSchema() + "." + partName;
    String parent = qualifiedParentTable();
    String sql =
        "CREATE TABLE IF NOT EXISTS "
            + qualifiedPart
            + " PARTITION OF "
            + parent
            + " FOR VALUES FROM ("
            + fromMs
            + ") TO ("
            + toExclusive
            + ")";
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement()) {
      st.execute(sql);
      c.commit();
      log.debug("Ensured partition {} for ym={}", qualifiedPart, ym);
    }
  }

  /** Drop detached monthly partitions wholly before retention boundary. */
  public void dropExpiredPartitions() throws SQLException {
    ensureParentTable();
    long watermark =
        PostgresUsageEventsUtcTime.minimumRetainedUtcMonthStartMillisUtcNow(retentionMonths);
    List<String> toDrop = new ArrayList<>();
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT c.relname AS part_name FROM pg_inherits i "
                    + "JOIN pg_class AS c ON c.oid = i.inhrelid "
                    + "JOIN pg_class AS p ON p.oid = i.inhparent "
                    + "JOIN pg_namespace n ON n.oid = p.relnamespace "
                    + "WHERE p.relname = '"
                    + parentTableName()
                    + "' "
                    + "AND n.nspname = '"
                    + postgresSqlSetupProperties.normalizedPostgresSchema()
                    + "'")) {
      while (rs.next()) {
        String rel = rs.getString("part_name");
        YearMonth ym = parsePartitionYearMonth(rel, parentTableName());
        if (ym == null) {
          continue;
        }
        long partEndExclusive = PostgresUsageEventsUtcTime.endExclusiveUtcMonthMillis(ym);
        if (partEndExclusive <= watermark) {
          toDrop.add(rel);
        }
      }
    }
    if (toDrop.isEmpty()) {
      return;
    }
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement()) {
      String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
      for (String rel : toDrop) {
        String q = schema + "." + rel;
        st.execute("DROP TABLE IF EXISTS " + q);
        log.warn("Dropped expired usage-events partition {}", q);
      }
      c.commit();
    }
  }

  private static YearMonth parsePartitionYearMonth(String relName, String parentTable) {
    String prefix = parentTable + PostgresUsageEventsTable.PARTITION_SUFFIX_PREFIX;
    if (!relName.startsWith(prefix)) {
      return null;
    }
    String token = relName.substring(prefix.length());
    if (token.length() != 6) {
      return null;
    }
    try {
      int year = Integer.parseInt(token.substring(0, 4));
      int month = Integer.parseInt(token.substring(4, 6));
      return YearMonth.of(year, month);
    } catch (Exception e) {
      return null;
    }
  }

  public boolean hasAnyPartition() throws SQLException {
    ensureParentTable();
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT EXISTS (SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid "
                    + "JOIN pg_class p ON p.oid=i.inhparent JOIN pg_namespace n ON n.oid=p.relnamespace "
                    + "WHERE p.relname='"
                    + parentTableName()
                    + "' AND n.nspname='"
                    + postgresSqlSetupProperties.normalizedPostgresSchema()
                    + "')")) {
      if (rs.next()) {
        return rs.getBoolean(1);
      }
    }
    return false;
  }

  public void insertBatch(@Nonnull List<PostgresUsageEventInsertRow> rows) throws SQLException {
    if (rows.isEmpty()) {
      return;
    }
    ensureParentTable();
    long minTs =
        rows.stream().mapToLong(PostgresUsageEventInsertRow::getTimestampMs).min().getAsLong();
    long maxTs =
        rows.stream().mapToLong(PostgresUsageEventInsertRow::getTimestampMs).max().getAsLong();
    ensurePartitionForInstantRange(minTs, maxTs);
    String sql =
        "INSERT INTO "
            + qualifiedParentTable()
            + " ("
            + COL_ID
            + ", "
            + COL_TS
            + ", "
            + COL_EVENT_TYPE
            + ", "
            + COL_USAGE_SOURCE
            + ", "
            + COL_ACTOR_URN
            + ", "
            + COL_ENTITY_URN
            + ", "
            + COL_ENTITY_TYPE
            + ", "
            + COL_BROWSER_ID
            + ", "
            + COL_QUERY
            + ", "
            + COL_SECTION
            + ", "
            + COL_ACTION_TYPE
            + ", "
            + COL_ASPECT_NAME
            + ", "
            + COL_DOCUMENT
            + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?::jsonb) ON CONFLICT ("
            + COL_TS
            + ", "
            + COL_ID
            + ") DO NOTHING";
    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      try (PreparedStatement ps = c.prepareStatement(sql)) {
        for (PostgresUsageEventInsertRow row : rows) {
          bindInsertRow(ps, row);
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

  private static void bindInsertRow(PreparedStatement ps, PostgresUsageEventInsertRow row)
      throws SQLException {
    int idx = 1;
    ps.setString(idx++, row.getId());
    ps.setLong(idx++, row.getTimestampMs());
    setString(ps, idx++, row.getEventType());
    setString(ps, idx++, row.getUsageSource());
    setString(ps, idx++, row.getActorUrn());
    setString(ps, idx++, row.getEntityUrn());
    setString(ps, idx++, row.getEntityType());
    setString(ps, idx++, row.getBrowserId());
    setString(ps, idx++, row.getQuery());
    setString(ps, idx++, row.getSection());
    setString(ps, idx++, row.getActionType());
    setString(ps, idx++, row.getAspectName());
    ps.setString(idx, row.getDocumentJson());
  }

  private static void setString(PreparedStatement ps, int idx, @Nullable String v)
      throws SQLException {
    if (v == null) {
      ps.setNull(idx, Types.VARCHAR);
    } else {
      ps.setString(idx, v);
    }
  }

  /** Bind positional parameters onto a JDBC statement. */
  @FunctionalInterface
  public interface SqlBinder {
    void bind(PreparedStatement ps, Map<String, Object> context) throws SQLException;
  }

  /** SELECT COUNT(*) for audit/export matching filters. */
  public int countMatching(Connection c, String sql, SqlBinder binder, Map<String, Object> ctx)
      throws SQLException {
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      binder.bind(ps, ctx);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? (int) rs.getLong(1) : 0;
      }
    }
  }

  /** First column long (e.g. COUNT DISTINCT). */
  public long scalarLong(Connection c, String sql, SqlBinder binder, Map<String, Object> ctx)
      throws SQLException {
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      binder.bind(ps, ctx);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    }
  }

  public List<long[]> groupedLongPairs(
      Connection c, String sql, SqlBinder binder, Map<String, Object> ctx) throws SQLException {
    List<long[]> pairs = new ArrayList<>();
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      binder.bind(ps, ctx);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          pairs.add(new long[] {rs.getLong(1), rs.getLong(2)});
        }
      }
    }
    return pairs;
  }

  public List<Object[]> groupedObjectTriple(
      Connection c, String sql, SqlBinder binder, Map<String, Object> ctx) throws SQLException {
    List<Object[]> triples = new ArrayList<>();
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      binder.bind(ps, ctx);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          triples.add(new Object[] {rs.getString(1), rs.getString(2), rs.getLong(3)});
        }
      }
    }
    return triples;
  }

  public List<String> stringList(
      Connection c, String sql, SqlBinder binder, Map<String, Object> ctx) throws SQLException {
    List<String> list = new ArrayList<>();
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      binder.bind(ps, ctx);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          list.add(rs.getString(1));
        }
      }
    }
    return list;
  }
}
