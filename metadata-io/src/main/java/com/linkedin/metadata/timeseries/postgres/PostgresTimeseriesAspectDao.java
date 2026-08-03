package com.linkedin.metadata.timeseries.postgres;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.timeseries.write.AbstractTimeseriesAspectWriteSink.TimeseriesAspectRowPayload;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * JDBC access for SqlSetup {@code {prefix}_aspect_row} (pgTimeseries) — shared by {@code
 * PostgresTimeseriesAspectWriteSink} (ES dual-write) and {@link PostgresTimeseriesAspectService}
 * (primary store).
 */
@RequiredArgsConstructor
public final class PostgresTimeseriesAspectDao {

  @Nonnull private final Database database;
  @Nonnull private final PostgresSqlSetupProperties postgresSqlSetupProperties;

  @Nonnull
  public String qualifiedTable() {
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgTimeseriesTablePrefix();
    return schema + "." + prefix + "_aspect_row";
  }

  public void upsert(@Nonnull TimeseriesAspectRowPayload row) throws SQLException {
    String table = qualifiedTable();
    String sql = buildUpsertSql(table);
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        bindRow(ps, row);
        ps.executeUpdate();
      }
    }
  }

  public void deleteByMessageId(
      @Nonnull String entityName, @Nonnull String aspectName, @Nonnull String messageId)
      throws SQLException {
    String table = qualifiedTable();
    String sql =
        "DELETE FROM " + table + " WHERE entity_name = ? AND aspect_name = ? AND message_id = ?";
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, entityName);
        ps.setString(2, aspectName);
        ps.setString(3, messageId);
        ps.executeUpdate();
      }
    }
  }

  private static String buildUpsertSql(String qualifiedTable) {
    return "INSERT INTO "
        + qualifiedTable
        + " (entity_name, aspect_name, urn, message_id, event_time, "
        + "run_id, event_granularity, partition_spec, event, system_metadata, document) "
        + "VALUES (?,?,?,?,?,?,?,?::jsonb,?::jsonb,?::jsonb,?::jsonb) "
        + "ON CONFLICT (entity_name, aspect_name, message_id, event_time) DO UPDATE SET "
        + "urn = EXCLUDED.urn, event_time = EXCLUDED.event_time, "
        + "run_id = EXCLUDED.run_id, event_granularity = EXCLUDED.event_granularity, "
        + "partition_spec = EXCLUDED.partition_spec, event = EXCLUDED.event, "
        + "system_metadata = EXCLUDED.system_metadata, document = EXCLUDED.document";
  }

  private static void bindRow(PreparedStatement ps, TimeseriesAspectRowPayload row)
      throws SQLException {
    int i = 1;
    ps.setString(i++, row.getEntityName());
    ps.setString(i++, row.getAspectName());
    ps.setString(i++, row.getUrn());
    ps.setString(i++, row.getMessageId());
    ps.setObject(
        i++,
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(row.getTimestampMillis()), ZoneOffset.UTC));
    if (row.getRunId() != null) {
      ps.setString(i++, row.getRunId());
    } else {
      ps.setNull(i++, Types.VARCHAR);
    }
    if (row.getEventGranularity() != null) {
      ps.setString(i++, row.getEventGranularity());
    } else {
      ps.setNull(i++, Types.VARCHAR);
    }
    setJsonb(ps, i++, row.getPartitionSpecJson());
    setJsonb(ps, i++, row.getEventJson());
    setJsonb(ps, i++, row.getSystemMetadataJson());
    setJsonb(ps, i, row.getDocumentJson());
  }

  private static void setJsonb(PreparedStatement ps, int index, String json) throws SQLException {
    if (json == null) {
      ps.setNull(index, Types.OTHER);
      return;
    }
    ps.setObject(index, json, Types.OTHER);
  }
}
