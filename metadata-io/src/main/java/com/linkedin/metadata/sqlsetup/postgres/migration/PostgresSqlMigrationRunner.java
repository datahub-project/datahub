package com.linkedin.metadata.sqlsetup.postgres.migration;

import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupSession;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Flyway-inspired Postgres SQL migration runner for DataHub SqlSetup modules. */
@Slf4j
public final class PostgresSqlMigrationRunner {

  private PostgresSqlMigrationRunner() {}

  @Nonnull
  public static SqlMigrationResult migrate(
      @Nonnull Connection connection, @Nonnull SqlMigrationModule module)
      throws SqlMigrationException {
    SqlMigrationResult result = new SqlMigrationResult();
    boolean previousAutoCommit;
    try {
      previousAutoCommit = connection.getAutoCommit();
    } catch (SQLException e) {
      throw new SqlMigrationException("Failed to read connection autoCommit", e);
    }

    try {
      connection.setAutoCommit(false);
      if (module.getPreMigrate() != null) {
        module.getPreMigrate().run(connection);
      }

      PostgresSqlSetupSession.ensureSchemaAndSearchPath(connection, module.getTargetSchema());
      SqlMigrationLedger.ensureLedgerTable(
          connection, module.getTargetSchema(), module.getLedgerTableName());
      acquireAdvisoryLock(connection, module.getMigrationNamespace());

      List<SqlMigrationScript> scripts =
          SqlMigrationScriptLoader.discover(module.getClassLoader(), module.getClasspathLocation());

      for (SqlMigrationScript script : scripts) {
        applyScript(connection, module, script, result);
      }

      if (module.getPostMigrate() != null) {
        module.getPostMigrate().run(connection);
      }

      connection.commit();
      return result;
    } catch (SQLException e) {
      rollbackQuietly(connection);
      throw new SqlMigrationException(
          "Migration failed for namespace " + module.getMigrationNamespace(), e);
    } catch (IOException e) {
      rollbackQuietly(connection);
      throw new SqlMigrationException(
          "Failed to load migration scripts for " + module.getMigrationNamespace(), e);
    } finally {
      try {
        connection.setAutoCommit(previousAutoCommit);
      } catch (SQLException e) {
        log.warn("Failed to restore autoCommit", e);
      }
    }
  }

  private static void applyScript(
      @Nonnull Connection connection,
      @Nonnull SqlMigrationModule module,
      @Nonnull SqlMigrationScript script,
      @Nonnull SqlMigrationResult result)
      throws SQLException, SqlMigrationException, IOException {
    String raw =
        PostgresSqlUtils.loadClasspathSql(module.getClassLoader(), script.getClasspathLocation());
    String sql = PostgresSqlUtils.applyTokenReplacements(raw, module.getTokenReplacements());
    PostgresSqlUtils.assertNoUnreplacedTokens(sql);

    String checksum = sha256Hex(sql);

    Optional<SqlMigrationLedger.LedgerRow> existing =
        SqlMigrationLedger.findByVersion(
            connection, module.getTargetSchema(), module.getLedgerTableName(), script.getVersion());

    if (existing.isPresent()) {
      SqlMigrationLedger.LedgerRow row = existing.get();
      if (script.getType() == SqlMigrationType.VERSIONED) {
        if (!row.checksum().equals(checksum)) {
          throw new SqlMigrationException(
              "VERSIONED migration "
                  + script.getVersion()
                  + " was already applied with a different checksum ("
                  + row.checksum()
                  + " vs "
                  + checksum
                  + ")");
        }
        result.recordSkipped(script.getVersion(), script.getScriptName());
        return;
      }
      if (row.checksum().equals(checksum)) {
        result.recordSkipped(script.getVersion(), script.getScriptName());
        return;
      }
    }

    long start = System.currentTimeMillis();
    PostgresSqlUtils.executeSql(connection, sql);
    int elapsed = (int) Math.min(Integer.MAX_VALUE, System.currentTimeMillis() - start);

    SqlMigrationLedger.insert(
        connection,
        module.getTargetSchema(),
        module.getLedgerTableName(),
        script,
        checksum,
        elapsed);
    result.recordApplied(script.getVersion(), script.getScriptName());
    log.info(
        "Applied migration {} ({}) for namespace {}",
        script.getVersion(),
        script.getScriptName(),
        module.getMigrationNamespace());
  }

  private static void acquireAdvisoryLock(@Nonnull Connection connection, @Nonnull String namespace)
      throws SQLException {
    try (PreparedStatement ps =
        connection.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?))")) {
      ps.setString(1, namespace);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new SQLException("pg_advisory_xact_lock returned no row");
        }
      }
    }
  }

  @Nonnull
  private static String sha256Hex(@Nonnull String text) throws SqlMigrationException {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new SqlMigrationException("SHA-256 not available", e);
    }
  }

  private static void rollbackQuietly(@Nonnull Connection connection) {
    try {
      connection.rollback();
    } catch (SQLException e) {
      log.warn("Rollback after migration failure failed", e);
    }
  }
}
