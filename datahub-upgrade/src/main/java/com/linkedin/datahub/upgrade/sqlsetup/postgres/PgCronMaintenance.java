package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nonnull;

/**
 * Helpers for registering pg_cron jobs using fully qualified {@code <cronSchema>.*} references on
 * the PostgreSQL database that holds pg_cron metadata (see {@code postgresql.conf} {@code
 * cron.database_name}, typically {@code postgres}). Jobs may execute in another database via {@code
 * cron.schedule_in_database}.
 *
 * <p>Job names should be built with {@link #buildScopedCronJobName} so multiple DataHub instances
 * sharing one pg_cron registry do not overwrite each other's schedules.
 */
public final class PgCronMaintenance {

  /** Role prefix for {@link #buildScopedCronJobName} (pgQueue {@code _apply_retention} cron). */
  public static final String PGQUEUE_CRON_ROLE = "datahub_pgqueue_apply_retention";

  /** Role prefix for {@link #buildScopedCronJobName} (pgTimeseries partman maintenance cron). */
  public static final String PGTIMESERIES_CRON_ROLE = "datahub_pgtimeseries_partman_maint";

  /**
   * When the readable scoped name exceeds this length, it is replaced by {@code <role>__h<12 hex>}
   * where the hash is SHA-256 of the full key (stable, bounded size for pg_cron metadata).
   */
  static final int MAX_SCOPED_CRON_JOB_NAME_LENGTH = 100;

  private PgCronMaintenance() {}

  /**
   * Builds a deterministic pg_cron {@code jobname} from the target database (as in {@code
   * schedule_in_database}), application schema, and table prefix so jobs do not collide across
   * tenants on a shared pg_cron registry.
   *
   * <p>Format when under the length cap: {@code <role>__<database>__<schema>__<prefix>} (segments
   * lower-cased; non-alphanumeric characters in {@code jobDatabaseName} become underscores). When
   * over the cap: {@code <role>__h<12 hex digits>} where the hash is the first six bytes of SHA-256
   * of the full string that would have been used.
   *
   * @param rolePrefix {@link #PGQUEUE_CRON_ROLE} or {@link #PGTIMESERIES_CRON_ROLE}
   * @param jobDatabaseName JDBC catalog / PostgreSQL database name (must be non-empty)
   * @param schema normalized application schema (letters, digits, underscore)
   * @param tablePrefix normalized table prefix (letters, digits, underscore)
   */
  @Nonnull
  public static String buildScopedCronJobName(
      @Nonnull String rolePrefix,
      @Nonnull String jobDatabaseName,
      @Nonnull String schema,
      @Nonnull String tablePrefix) {
    if (jobDatabaseName == null || jobDatabaseName.isBlank()) {
      throw new IllegalArgumentException(
          "jobDatabaseName must be non-empty for scoped pg_cron job registration");
    }
    if (schema == null || schema.isBlank()) {
      throw new IllegalArgumentException("schema must be non-empty for scoped pg_cron job name");
    }
    if (tablePrefix == null || tablePrefix.isBlank()) {
      throw new IllegalArgumentException(
          "tablePrefix must be non-empty for scoped pg_cron job name");
    }
    String db = sanitizeCronSegment(jobDatabaseName);
    String sch = sanitizeCronSegment(schema);
    String pre = sanitizeCronSegment(tablePrefix);
    String fullKey = rolePrefix + "__" + db + "__" + sch + "__" + pre;
    if (fullKey.length() <= MAX_SCOPED_CRON_JOB_NAME_LENGTH) {
      return fullKey;
    }
    return rolePrefix + "__h" + sha256First12Hex(fullKey);
  }

  static String sanitizeCronSegment(String raw) {
    String lower = raw.trim().toLowerCase();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < lower.length(); i++) {
      char c = lower.charAt(i);
      if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
        sb.append(c);
      } else {
        sb.append('_');
      }
    }
    String s = sb.toString();
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Cron job name segment became empty after sanitization");
    }
    return s;
  }

  static String sha256First12Hex(String input) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(12);
      for (int i = 0; i < 6; i++) {
        hex.append(String.format("%02x", digest[i]));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  /**
   * Removes any existing job named {@code jobName}, then registers {@code
   * cron.schedule_in_database(jobName, schedule, command, jobDatabaseName)} so {@code commandSql}
   * runs in {@code jobDatabaseName} (the application database from {@code ebean.url}).
   *
   * <p>{@code cronRegistryConnection} must use the pg_cron registry database (same as {@code
   * cron.database_name}).
   */
  public static void replaceCronJobInDatabase(
      @Nonnull Connection cronRegistryConnection,
      @Nonnull String cronSchema,
      @Nonnull String jobName,
      @Nonnull String schedule,
      @Nonnull String commandSql,
      @Nonnull String jobDatabaseName)
      throws SQLException {
    unscheduleJobIfExists(cronRegistryConnection, cronSchema, jobName);

    String cs = cronSchema.replace("'", "''");
    String jobEsc = jobName.replace("'", "''");
    String scheduleEsc = schedule.replace("'", "''");
    String commandEsc = commandSql.replace("'", "''");
    String dbEsc = jobDatabaseName.replace("'", "''");
    try (Statement st = cronRegistryConnection.createStatement()) {
      st.execute(
          "SELECT "
              + cs
              + ".schedule_in_database('"
              + jobEsc
              + "', '"
              + scheduleEsc
              + "', '"
              + commandEsc
              + "', '"
              + dbEsc
              + "')");
    }
  }

  static void unscheduleJobIfExists(
      Connection cronRegistryConnection, String cronSchema, String jobName) throws SQLException {
    String cs = cronSchema.replace("'", "''");
    String jobEsc = jobName.replace("'", "''");
    try (Statement st = cronRegistryConnection.createStatement()) {
      st.execute(
          "DO $cronunsched$ DECLARE jid bigint; BEGIN "
              + "SELECT jobid INTO jid FROM "
              + cs
              + ".job WHERE jobname = '"
              + jobEsc
              + "' LIMIT 1; "
              + "IF jid IS NOT NULL THEN PERFORM "
              + cs
              + ".unschedule(jid); END IF; "
              + "END $cronunsched$;");
    }
  }

  public static boolean isExtensionInstalled(Connection connection, String extensionName)
      throws SQLException {
    String safe = extensionName.replace("'", "''");
    try (Statement st = connection.createStatement();
        ResultSet rs =
            st.executeQuery("SELECT 1 FROM pg_extension WHERE extname = '" + safe + "' LIMIT 1")) {
      return rs.next();
    }
  }
}
