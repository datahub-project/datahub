package com.linkedin.metadata.sqlsetup.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Shared PostgreSQL extension helpers for SqlSetup schema steps. */
@Slf4j
public final class PostgresSqlSetupExtensions {

  private PostgresSqlSetupExtensions() {}

  public static void maybeCreateExtension(
      @Nonnull Connection connection,
      @Nonnull String extensionName,
      boolean want,
      @Nonnull Set<String> allowedNames)
      throws SQLException {
    if (!want) {
      return;
    }
    if (!allowedNames.contains(extensionName)) {
      throw new IllegalArgumentException("Unsupported extension name: " + extensionName);
    }
    if (!isExtensionAvailable(connection, extensionName)) {
      log.warn(
          "Extension {} is not listed in pg_available_extensions; skipping CREATE EXTENSION.",
          extensionName);
      return;
    }
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE EXTENSION IF NOT EXISTS " + extensionName);
      log.info("CREATE EXTENSION IF NOT EXISTS {} attempted.", extensionName);
    } catch (SQLException e) {
      log.warn(
          "CREATE EXTENSION {} skipped or failed (non-fatal for SqlSetup): {}",
          extensionName,
          e.getMessage());
    }
  }

  public static boolean isExtensionAvailable(
      @Nonnull Connection connection, @Nonnull String extensionName) throws SQLException {
    String safe = extensionName.replace("'", "''");
    try (Statement st = connection.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT 1 FROM pg_available_extensions WHERE name = '" + safe + "' LIMIT 1")) {
      return rs.next();
    }
  }

  public static boolean isExtensionInstalled(
      @Nonnull Connection connection, @Nonnull String extensionName) throws SQLException {
    String safe = extensionName.replace("'", "''");
    try (Statement st = connection.createStatement();
        ResultSet rs =
            st.executeQuery("SELECT 1 FROM pg_extension WHERE extname = '" + safe + "' LIMIT 1")) {
      return rs.next();
    }
  }
}
