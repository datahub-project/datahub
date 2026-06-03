package com.linkedin.metadata.sqlsetup.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nonnull;

/**
 * Session setup for SqlSetup DDL that uses unqualified PostgreSQL identifiers: ensures the feature
 * schema exists and sets {@code search_path} so {@code CREATE TABLE foo ...} resolves into the
 * intended schema.
 */
public final class PostgresSqlSetupSession {

  private PostgresSqlSetupSession() {}

  /**
   * Runs {@code CREATE SCHEMA IF NOT EXISTS} for {@code schema}, then {@code SET search_path TO
   * schema, public} so subsequent statements may use unqualified names.
   */
  public static void ensureSchemaAndSearchPath(
      @Nonnull Connection connection, @Nonnull String schema) throws SQLException {
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
      st.execute("SET search_path TO " + schema + ", public");
    }
  }
}
