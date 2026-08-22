package com.linkedin.metadata.sqlsetup.postgres;

import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Shared pg_partman helpers for SqlSetup (pgQueue, pgTimeseries). */
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

  /** Clears {@code part_config.retention} so partman stops dropping old partitions. */
  @Nonnull
  public static String partmanRetentionClearSql(
      @Nonnull String partmanExtensionSchema,
      @Nonnull String schema,
      @Nonnull String parentTableSuffix) {
    String escSchema = schema.replace("'", "''");
    return "  UPDATE "
        + PostgresSqlUtils.quotePgIdentifier(partmanExtensionSchema)
        + ".part_config\n"
        + "  SET retention = NULL\n"
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

  /**
   * Maps intervalSeconds to a pg_cron schedule (minute/hour/day granularity).
   *
   * <p>Only intervals that map cleanly are accepted: multiples of 60 seconds up to 59 minutes,
   * multiples of 3600 up to 23 hours, or multiples of 86400 up to 31 days. Values below 60 are
   * treated as 60 seconds ({@code every minute}). Non-representable values (e.g. 90 minutes) throw.
   */
  @Nonnull
  public static String toPgCronSchedule(int intervalSeconds) {
    int sec = Math.max(60, intervalSeconds);
    if (sec % 86400 == 0) {
      int days = sec / 86400;
      if (days < 1 || days > 31) {
        throw new IllegalArgumentException(
            "intervalSeconds="
                + intervalSeconds
                + " day cadence must be between 1 and 31 days inclusive");
      }
      return days == 1 ? "0 0 * * *" : ("0 0 */" + days + " * *");
    }
    if (sec % 3600 == 0) {
      int hours = sec / 3600;
      if (hours < 1 || hours > 23) {
        throw new IllegalArgumentException(
            "intervalSeconds="
                + intervalSeconds
                + " hour cadence must be between 1 and 23 hours inclusive");
      }
      return "0 */" + hours + " * * *";
    }
    if (sec % 60 == 0) {
      int minutes = sec / 60;
      if (minutes >= 1 && minutes <= 59) {
        return "*/" + minutes + " * * * *";
      }
    }
    throw new IllegalArgumentException(
        "intervalSeconds="
            + intervalSeconds
            + " cannot be represented as a pg_cron schedule; use a multiple of 60 (1–59 min),"
            + " 3600 (1–23 h), or 86400 (1–31 d)");
  }
}
