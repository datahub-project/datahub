package com.linkedin.metadata.sqlsetup.postgres.pgqueue;

import com.linkedin.metadata.config.postgres.PgQueueResolvedTopicCatalogEntry;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Shared pgQueue SqlSetup helpers used from datahub-upgrade orchestration. */
@Slf4j
public final class PgQueueSqlSetupSupport {

  private PgQueueSqlSetupSupport() {}

  public static int queryMaxTopicRetentionMaxAgeSeconds(
      @Nonnull Connection connection, @Nonnull String schema, @Nonnull String tablePrefix)
      throws SQLException {
    String qualified =
        PostgresSqlUtils.quotePgIdentifier(schema)
            + "."
            + PostgresSqlUtils.quotePgIdentifier(tablePrefix + "_topic");
    String sql = "SELECT COALESCE(MAX(retention_max_age_seconds), 0) FROM " + qualified;
    try (Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    }
  }

  public static void upsertTopicCatalog(
      @Nonnull Connection connection, @Nonnull PgQueueSetupOptions options) throws SQLException {
    if (options.getResolvedTopicCatalog().isEmpty()) {
      return;
    }
    String schema = options.getSchema();
    String tablePrefix = options.getTablePrefix();
    String qualified =
        PostgresSqlUtils.quotePgIdentifier(schema)
            + "."
            + PostgresSqlUtils.quotePgIdentifier(tablePrefix + "_topic");
    String qualifiedMessage =
        PostgresSqlUtils.quotePgIdentifier(schema)
            + "."
            + PostgresSqlUtils.quotePgIdentifier(tablePrefix + "_message");
    String qualifiedContentType =
        PostgresSqlUtils.quotePgIdentifier(schema)
            + "."
            + PostgresSqlUtils.quotePgIdentifier(tablePrefix + "_content_type");
    String sql =
        "INSERT INTO "
            + qualified
            + " AS ptopic (topic_name, partition_count, retention_max_age_seconds, "
            + "max_rows_per_topic, max_total_payload_bytes, default_content_type_id, aggressive_retention) "
            + "VALUES (?,?,?,?,?,(SELECT id FROM "
            + qualifiedContentType
            + " WHERE mime = ? LIMIT 1),?) ON CONFLICT (topic_name) DO UPDATE SET "
            + "partition_count = GREATEST(1, EXCLUDED.partition_count, ptopic.partition_count, "
            + "COALESCE((SELECT MAX(m.partition_id) FROM "
            + qualifiedMessage
            + " m WHERE m.topic_id = ptopic.id), -1) + 1), "
            + "retention_max_age_seconds = EXCLUDED.retention_max_age_seconds, "
            + "max_rows_per_topic = EXCLUDED.max_rows_per_topic, "
            + "max_total_payload_bytes = EXCLUDED.max_total_payload_bytes, "
            + "default_content_type_id = EXCLUDED.default_content_type_id, "
            + "aggressive_retention = EXCLUDED.aggressive_retention";
    for (PgQueueResolvedTopicCatalogEntry entry : options.getResolvedTopicCatalog()) {
      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setString(1, entry.getTopicName());
        ps.setInt(2, entry.getPartitionCount());
        ps.setInt(3, entry.getRetentionMaxAgeSeconds());
        ps.setLong(4, entry.getMaxRowsPerTopic());
        ps.setLong(5, entry.getMaxTotalPayloadBytesPerTopic());
        ps.setString(6, options.getTopicDefaultContentTypeMime());
        ps.setBoolean(7, entry.isAggressiveRetention());
        ps.executeUpdate();
      }
      log.info(
          "pgQueue topic catalog upsert: {} -> retention_max_age_seconds={}",
          entry.getTopicName(),
          entry.getRetentionMaxAgeSeconds());
    }
  }

  @Nullable
  public static String resolvePgPartmanExtensionSchema(@Nonnull Connection connection)
      throws SQLException {
    try (Statement st = connection.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT n.nspname FROM pg_extension e "
                    + "JOIN pg_namespace n ON n.oid = e.extnamespace "
                    + "WHERE e.extname = 'pg_partman' LIMIT 1")) {
      if (!rs.next()) {
        return null;
      }
      return rs.getString(1);
    }
  }

  @Nonnull
  public static String partmanRetentionUpdateSql(
      @Nonnull String partmanExtensionSchema,
      @Nonnull String schema,
      @Nullable String partmanRetentionIntervalText,
      @Nonnull String tablePrefix) {
    if (partmanRetentionIntervalText == null || partmanRetentionIntervalText.isEmpty()) {
      return "";
    }
    String escRetention = partmanRetentionIntervalText.replace("'", "''");
    String escSchema = schema.replace("'", "''");
    return "  UPDATE "
        + PostgresSqlUtils.quotePgIdentifier(partmanExtensionSchema)
        + ".part_config\n"
        + "  SET retention = '"
        + escRetention
        + "',\n"
        + "      retention_keep_table = false,\n"
        + "      retention_keep_index = false\n"
        + "  WHERE parent_table = '"
        + escSchema
        + "."
        + tablePrefix
        + "_message';\n";
  }

  @Nonnull
  public static String sanitizePartmanIntervalLiteral(@Nonnull String partmanPartitionInterval) {
    return partmanPartitionInterval.replace("'", "''");
  }

  @Nonnull
  public static String buildRetentionPartmanTail(
      @Nonnull String partmanExtensionSchema, @Nonnull String schema, @Nonnull String tablePrefix) {
    return "    PERFORM "
        + PostgresSqlUtils.quotePgIdentifier(partmanExtensionSchema)
        + ".run_maintenance('"
        + schema.replace("'", "''")
        + "."
        + tablePrefix
        + "_message');\n";
  }
}
