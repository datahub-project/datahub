package com.linkedin.datahub.upgrade.sqlsetup;

import java.net.URI;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;

/** Utility class for parsing JDBC URLs using java.net.URI. */
@Slf4j
public class JdbcUrlParser {

  public static class JdbcInfo {
    public final DatabaseType databaseType;
    public final String host;
    public final int port;
    public final String database;

    public JdbcInfo(DatabaseType databaseType, String host, int port, String database) {
      this.databaseType = databaseType;
      this.host = host;
      this.port = port;
      this.database = database;
    }
  }

  /** Parse a JDBC URL and extract connection information using java.net.URI. */
  public static JdbcInfo parseJdbcUrl(String jdbcUrl) {
    if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
      throw new IllegalArgumentException("JDBC URL cannot be null or empty");
    }

    try {
      return parseWithUri(jdbcUrl);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid JDBC URL: " + jdbcUrl, e);
    }
  }

  /**
   * Create a JDBC URL without the database name, useful for connecting to the server only. This is
   * helpful when you need to create databases or perform server-level operations. Preserves query
   * parameters from the original URL (e.g., IAM authentication, SSL settings).
   */
  public static String createUrlWithoutDatabase(String jdbcUrl) {
    JdbcInfo info = parseJdbcUrl(jdbcUrl);

    // Extract original JDBC scheme (e.g., "mariadb", "mysql", "postgresql") from the URL
    // This is important because IAM authentication might require a specific driver (e.g., mariadb
    // for MySQL)
    String originalScheme = extractJdbcScheme(jdbcUrl);

    // Extract query parameters from original URL to preserve them
    String queryParams = "";
    int queryIndex = jdbcUrl.indexOf('?');
    if (queryIndex != -1) {
      queryParams = jdbcUrl.substring(queryIndex); // includes the '?'
    }

    return String.format("jdbc:%s://%s:%d/%s", originalScheme, info.host, info.port, queryParams);
  }

  /**
   * Extract the JDBC scheme from a JDBC URL (the part between "jdbc:" and "://"). Examples:
   * "mysql", "mariadb", "postgresql"
   */
  private static String extractJdbcScheme(String jdbcUrl) {
    if (!jdbcUrl.startsWith("jdbc:")) {
      throw new IllegalArgumentException("Invalid JDBC URL: must start with 'jdbc:'");
    }
    int schemeEnd = jdbcUrl.indexOf("://");
    if (schemeEnd == -1) {
      throw new IllegalArgumentException("Invalid JDBC URL: missing '://' separator");
    }
    return jdbcUrl.substring(5, schemeEnd); // Extract between "jdbc:" and "://"
  }

  /** Parse using java.net.URI. */
  private static JdbcInfo parseWithUri(String jdbcUrl) throws URISyntaxException {
    // Remove jdbc: prefix for URI parsing
    String uriString = jdbcUrl.startsWith("jdbc:") ? jdbcUrl.substring(5) : jdbcUrl;

    // Handle nested schemes like aws-wrapper:mysql://
    // Extract the innermost scheme for parsing
    String parseableUri = uriString;
    if (uriString.startsWith("aws-wrapper:")) {
      // Strip aws-wrapper: prefix to get the actual JDBC URL
      parseableUri = uriString.substring(12); // Length of "aws-wrapper:"
    }

    // Validate port format before URI parsing
    validatePortFormat(parseableUri);

    URI uri = new URI(parseableUri);

    String scheme = uri.getScheme();
    String host = uri.getHost() != null ? uri.getHost() : "localhost";
    int port = uri.getPort() != -1 ? uri.getPort() : getDefaultPort(scheme);

    // Validate port range
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException(
          "Invalid port number: " + port + ". Port must be between 1 and 65535.");
    }

    // Handle database name - remove leading slash and query params
    String database = uri.getPath();
    if (database != null && database.length() > 1) {
      database = database.substring(1); // Remove leading slash
      // Handle multiple slashes by taking the first non-empty segment
      String[] segments = database.split("/");
      for (String segment : segments) {
        if (!segment.isEmpty()) {
          database = segment;
          break;
        }
      }
      if (database.contains("?")) {
        database = database.substring(0, database.indexOf("?"));
      }
    } else {
      database = "datahub";
    }

    // Convert scheme to DatabaseType enum
    DatabaseType databaseType = DatabaseType.fromJdbcScheme(scheme);
    return new JdbcInfo(databaseType, host, port, database);
  }

  /** Validate that the port in the URI string is numeric. */
  private static void validatePortFormat(String uriString) {
    // Look for port pattern in the URI
    // Pattern: scheme://[user:pass@]host:port/path
    int schemeEnd = uriString.indexOf("://");
    if (schemeEnd == -1) {
      return; // No scheme, skip validation
    }

    int hostStart = schemeEnd + 3;

    // Skip credentials if present (user:pass@)
    int atIndex = uriString.indexOf("@", hostStart);
    if (atIndex != -1) {
      hostStart = atIndex + 1;
    }

    int portStart = uriString.indexOf(":", hostStart);
    if (portStart == -1) {
      return; // No port specified, skip validation
    }

    int portEnd = uriString.indexOf("/", portStart);
    if (portEnd == -1) {
      portEnd = uriString.length();
    }

    String portStr = uriString.substring(portStart + 1, portEnd);
    try {
      Integer.parseInt(portStr);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid port format: " + portStr + ". Port must be a valid integer.");
    }
  }

  /** Get default port for database type. */
  private static int getDefaultPort(String scheme) {
    if (scheme == null) {
      return 3306;
    }
    try {
      DatabaseType databaseType = DatabaseType.fromJdbcScheme(scheme);
      return getDefaultPort(databaseType);
    } catch (IllegalArgumentException e) {
      return 3306; // Default to MySQL port for unknown schemes
    }
  }

  /** Get default port for database type enum. */
  private static int getDefaultPort(DatabaseType databaseType) {
    switch (databaseType) {
      case MYSQL:
        return 3306;
      case POSTGRES:
        return 5432;
      default:
        return 3306; // Default to MySQL port
    }
  }
}
