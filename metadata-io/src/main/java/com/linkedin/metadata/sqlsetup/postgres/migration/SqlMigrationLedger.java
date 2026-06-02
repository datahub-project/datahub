package com.linkedin.metadata.sqlsetup.postgres.migration;

import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupSession;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import javax.annotation.Nonnull;

/** JDBC access to per-module schema migration ledger tables. */
final class SqlMigrationLedger {

  private SqlMigrationLedger() {}

  static void ensureLedgerTable(
      @Nonnull Connection connection, @Nonnull String schema, @Nonnull String ledgerTableName)
      throws SQLException {
    PostgresSqlSetupSession.ensureSchemaAndSearchPath(connection, schema);
    String qualified =
        PostgresSqlUtils.quotePgIdentifier(schema)
            + "."
            + PostgresSqlUtils.quotePgIdentifier(ledgerTableName);
    String ddl =
        "CREATE TABLE IF NOT EXISTS "
            + qualified
            + " ("
            + "version_rank int PRIMARY KEY,"
            + "version text NOT NULL UNIQUE,"
            + "description text NOT NULL,"
            + "type text NOT NULL CHECK (type IN ('VERSIONED', 'REPEATABLE')),"
            + "script_name text NOT NULL,"
            + "checksum text NOT NULL,"
            + "installed_on timestamptz NOT NULL DEFAULT now(),"
            + "execution_time_ms int NOT NULL,"
            + "success boolean NOT NULL"
            + ")";
    try (Statement st = connection.createStatement()) {
      st.execute(ddl);
    }
  }

  @Nonnull
  static Optional<LedgerRow> findByVersion(
      @Nonnull Connection connection,
      @Nonnull String schema,
      @Nonnull String ledgerTableName,
      @Nonnull String version)
      throws SQLException {
    String qualified =
        PostgresSqlUtils.quotePgIdentifier(schema)
            + "."
            + PostgresSqlUtils.quotePgIdentifier(ledgerTableName);
    String sql =
        "SELECT version, checksum, type FROM "
            + qualified
            + " WHERE version = ? AND success = true";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setString(1, version);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }
        return Optional.of(
            new LedgerRow(
                rs.getString("version"),
                rs.getString("checksum"),
                SqlMigrationType.valueOf(rs.getString("type"))));
      }
    }
  }

  static void insert(
      @Nonnull Connection connection,
      @Nonnull String schema,
      @Nonnull String ledgerTableName,
      @Nonnull SqlMigrationScript script,
      @Nonnull String checksum,
      int executionTimeMs)
      throws SQLException {
    String qualified =
        PostgresSqlUtils.quotePgIdentifier(schema)
            + "."
            + PostgresSqlUtils.quotePgIdentifier(ledgerTableName);
    String sql =
        "INSERT INTO "
            + qualified
            + " (version_rank, version, description, type, script_name, checksum, execution_time_ms, success)"
            + " VALUES (?,?,?,?,?,?,?,true)"
            + " ON CONFLICT (version) DO UPDATE SET"
            + " version_rank = EXCLUDED.version_rank,"
            + " description = EXCLUDED.description,"
            + " type = EXCLUDED.type,"
            + " script_name = EXCLUDED.script_name,"
            + " checksum = EXCLUDED.checksum,"
            + " installed_on = now(),"
            + " execution_time_ms = EXCLUDED.execution_time_ms,"
            + " success = true";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setInt(1, script.getVersionRank());
      ps.setString(2, script.getVersion());
      ps.setString(3, script.getDescription());
      ps.setString(4, script.getType().name());
      ps.setString(5, script.getScriptName());
      ps.setString(6, checksum);
      ps.setInt(7, executionTimeMs);
      ps.executeUpdate();
    }
  }

  record LedgerRow(
      @Nonnull String version, @Nonnull String checksum, @Nonnull SqlMigrationType type) {}
}
