package com.linkedin.metadata.sqlsetup.postgres.migration;

import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupSession;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.List;
import javax.annotation.Nonnull;

/** Records a versioned migration as applied without executing its SQL. */
public final class SqlMigrationVersionSkip {

  private SqlMigrationVersionSkip() {}

  /**
   * Marks {@code scriptFileName} as successfully applied in the ledger when absent, so the runner
   * will not execute it on subsequent migrate calls.
   */
  public static void skipVersionedScriptIfAbsent(
      @Nonnull Connection connection,
      @Nonnull String schema,
      @Nonnull String ledgerTableName,
      @Nonnull String classpathLocation,
      @Nonnull String scriptFileName)
      throws SQLException, IOException, SqlMigrationException {
    PostgresSqlSetupSession.ensureSchemaAndSearchPath(connection, schema);
    SqlMigrationLedger.ensureLedgerTable(connection, schema, ledgerTableName);
    List<SqlMigrationScript> scripts =
        SqlMigrationScriptLoader.discover(
            SqlMigrationVersionSkip.class.getClassLoader(), classpathLocation);
    SqlMigrationScript target =
        scripts.stream()
            .filter(s -> s.getScriptName().equals(scriptFileName))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "No migration script named "
                            + scriptFileName
                            + " under "
                            + classpathLocation));
    if (target.getType() != SqlMigrationType.VERSIONED) {
      throw new IllegalArgumentException(
          "Only VERSIONED scripts can be skipped: " + scriptFileName);
    }
    if (SqlMigrationLedger.findByVersion(connection, schema, ledgerTableName, target.getVersion())
        .isPresent()) {
      return;
    }
    String raw =
        PostgresSqlUtils.loadClasspathSql(
            SqlMigrationVersionSkip.class.getClassLoader(), target.getClasspathLocation());
    String checksum = sha256Hex(raw);
    SqlMigrationLedger.insert(connection, schema, ledgerTableName, target, checksum, 0);
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
}
