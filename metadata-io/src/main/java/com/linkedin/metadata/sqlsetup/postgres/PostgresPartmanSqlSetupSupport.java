package com.linkedin.metadata.sqlsetup.postgres;

import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Shared pg_partman helpers for SqlSetup (pgQueue, pgTimeseries, usage events). */
public final class PostgresPartmanSqlSetupSupport {

  private PostgresPartmanSqlSetupSupport() {}

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
      @Nonnull String parentTableSuffix) {
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
        + parentTableSuffix
        + "';\n";
  }

  @Nonnull
  public static String sanitizePartmanIntervalLiteral(@Nonnull String partmanPartitionInterval) {
    return partmanPartitionInterval.replace("'", "''");
  }

  @Nonnull
  public static String buildRetentionPartmanTail(
      @Nonnull String partmanExtensionSchema,
      @Nonnull String schema,
      @Nonnull String parentTableSuffix) {
    return "    PERFORM "
        + PostgresSqlUtils.quotePgIdentifier(partmanExtensionSchema)
        + ".run_maintenance('"
        + schema.replace("'", "''")
        + "."
        + parentTableSuffix
        + "');\n";
  }

  /** Maps intervalSeconds to a pg_cron schedule (minute/hour granularity). */
  @Nonnull
  public static String toPgCronSchedule(int intervalSeconds) {
    int sec = Math.max(60, intervalSeconds);
    if (sec % 86400 == 0) {
      int days = sec / 86400;
      days = Math.max(1, Math.min(31, days));
      return days == 1 ? "0 0 * * *" : ("0 0 */" + days + " * *");
    }
    if (sec % 3600 == 0) {
      int hours = Math.max(1, Math.min(23, sec / 3600));
      return "0 */" + hours + " * * *";
    }
    int minutes = Math.max(1, Math.min(59, sec / 60));
    return "*/" + minutes + " * * * *";
  }
}
