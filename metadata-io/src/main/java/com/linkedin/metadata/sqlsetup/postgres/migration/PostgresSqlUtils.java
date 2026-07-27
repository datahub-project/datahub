package com.linkedin.metadata.sqlsetup.postgres.migration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/** Shared JDBC and classpath SQL helpers for Postgres SqlSetup migrations. */
public final class PostgresSqlUtils {

  private static final Pattern UNREPLACED_TOKEN = Pattern.compile("__[A-Z0-9_]+__");

  private PostgresSqlUtils() {}

  @Nonnull
  public static String quotePgIdentifier(@Nonnull String ident) {
    if (ident.isEmpty()) {
      throw new IllegalArgumentException("PostgreSQL identifier required");
    }
    StringBuilder sb = new StringBuilder();
    sb.append('"');
    for (int i = 0; i < ident.length(); i++) {
      char c = ident.charAt(i);
      if (c == '"') {
        sb.append("\"\"");
      } else {
        sb.append(c);
      }
    }
    sb.append('"');
    return sb.toString();
  }

  @Nonnull
  public static String loadClasspathSql(@Nonnull ClassLoader classLoader, @Nonnull String location)
      throws IOException {
    String normalized = location.startsWith("/") ? location.substring(1) : location;
    try (InputStream in = classLoader.getResourceAsStream(normalized)) {
      if (in == null) {
        throw new IOException("Classpath SQL not found: " + location);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
    }
  }

  @Nonnull
  public static String applyTokenReplacements(
      @Nonnull String sql, @Nonnull Map<String, String> replacements) {
    String result = sql;
    for (Map.Entry<String, String> e : replacements.entrySet()) {
      result = result.replace(e.getKey(), e.getValue());
    }
    return result;
  }

  public static void assertNoUnreplacedTokens(@Nonnull String sql) throws SqlMigrationException {
    Matcher m = UNREPLACED_TOKEN.matcher(sql);
    if (m.find()) {
      throw new SqlMigrationException("Unreplaced token remains in SQL: " + m.group());
    }
  }

  public static void executeSql(@Nonnull Connection connection, @Nonnull String sql)
      throws SQLException {
    if (sql.isEmpty()) {
      return;
    }
    try (Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(sql);
      for (; ; ) {
        if (hasResultSet) {
          try (ResultSet rs = statement.getResultSet()) {
            while (rs.next()) {
              // drain
            }
          }
        } else if (statement.getUpdateCount() == -1) {
          break;
        }
        hasResultSet = statement.getMoreResults();
      }
    }
  }
}
