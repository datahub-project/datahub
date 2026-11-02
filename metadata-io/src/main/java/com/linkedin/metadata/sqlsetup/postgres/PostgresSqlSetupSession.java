package com.linkedin.metadata.sqlsetup.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nonnull;

/**
 * Session setup for SqlSetup DDL that uses unqualified PostgreSQL identifiers: ensures the feature
 * schema exists (optional) and sets {@code search_path} so {@code CREATE TABLE foo ...} resolves
 * into the intended schema.
 *
 * <p>{@code SET search_path} is session-scoped. Callers must run subsequent unqualified DDL on the
 * same {@link Connection} (or call this again on each new connection) — otherwise tables land in
 * {@code public}.
 *
 * <p>PostgreSQL silently drops missing schemas (and schemas without {@code USAGE}) from {@code
 * search_path}, falling through to {@code public}. This class verifies the effective {@code
 * current_schema()} after setting the path so misconfiguration fails instead of creating metadata
 * in the wrong namespace.
 */
public final class PostgresSqlSetupSession {

  private PostgresSqlSetupSession() {}

  /**
   * Runs {@code CREATE SCHEMA IF NOT EXISTS} for {@code schema}, then {@code SET search_path TO
   * schema, public} so subsequent statements may use unqualified names.
   */
  public static void ensureSchemaAndSearchPath(
      @Nonnull Connection connection, @Nonnull String schema) throws SQLException {
    ensureSchemaAndSearchPath(connection, schema, true);
  }

  /**
   * Optionally creates {@code schema}, then sets {@code search_path} so subsequent unqualified DDL
   * resolves into that schema.
   *
   * @param createSchema when {@code true}, runs {@code CREATE SCHEMA IF NOT EXISTS}; when {@code
   *     false}, requires the schema to already exist (pre-provisioned) and be usable ({@code
   *     USAGE})
   */
  public static void ensureSchemaAndSearchPath(
      @Nonnull Connection connection, @Nonnull String schema, boolean createSchema)
      throws SQLException {
    if (!createSchema) {
      requireSchemaExists(connection, schema);
    }
    try (Statement st = connection.createStatement()) {
      if (createSchema) {
        st.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
      }
      st.execute("SET search_path TO " + schema + ", public");
    }
    requireEffectiveSchema(connection, schema);
  }

  /**
   * Sets {@code search_path} for an already-prepared schema without attempting {@code CREATE
   * SCHEMA}. Use on pool connections after the schema was ensured (or pre-provisioned).
   */
  public static void setSearchPath(@Nonnull Connection connection, @Nonnull String schema)
      throws SQLException {
    ensureSchemaAndSearchPath(connection, schema, false);
  }

  private static void requireSchemaExists(@Nonnull Connection connection, @Nonnull String schema)
      throws SQLException {
    try (PreparedStatement ps =
        connection.prepareStatement("SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = ?")) {
      ps.setString(1, schema);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new SQLException(
              "PostgreSQL schema '"
                  + schema
                  + "' does not exist. Pre-provision the schema and grant USAGE/CREATE to the"
                  + " deploy role, or set CREATE_SCHEMA=true so SqlSetup can create it.");
        }
      }
    }
  }

  /**
   * Confirms {@code current_schema()} is the configured schema. Catches missing schemas and missing
   * {@code USAGE} — both cause PostgreSQL to skip the name in {@code search_path} and fall through
   * to {@code public}.
   */
  private static void requireEffectiveSchema(@Nonnull Connection connection, @Nonnull String schema)
      throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT current_schema()");
        ResultSet rs = ps.executeQuery()) {
      if (!rs.next()) {
        throw new SQLException(
            "Failed to resolve current_schema() after setting search_path to '" + schema + "'.");
      }
      String effective = rs.getString(1);
      if (effective == null || !schema.equals(effective)) {
        throw new SQLException(
            "PostgreSQL search_path did not resolve to schema '"
                + schema
                + "' (effective current_schema()='"
                + effective
                + "'). Ensure the schema exists and the deploy role has USAGE on it;"
                + " otherwise unqualified DDL would land in public.");
      }
    }
  }
}
