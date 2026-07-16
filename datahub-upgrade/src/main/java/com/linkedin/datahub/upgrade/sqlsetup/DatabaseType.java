package com.linkedin.datahub.upgrade.sqlsetup;

/** Supported database types for SqlSetup operations. */
public enum DatabaseType {
  POSTGRES("postgresql"),
  MYSQL("mysql");

  private final String value;

  DatabaseType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  /**
   * Parse a string value to DatabaseType enum.
   *
   * @param value the string value to parse
   * @return the corresponding DatabaseType enum
   * @throws IllegalArgumentException if the value is not supported
   */
  public static DatabaseType fromString(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Database type cannot be null");
    }

    String normalizedValue = value.toLowerCase().trim();
    for (DatabaseType type : values()) {
      if (type.value.equals(normalizedValue)) {
        return type;
      }
    }

    // Handle legacy "postgres" value
    if ("postgres".equals(normalizedValue)) {
      return POSTGRES;
    }

    throw new IllegalArgumentException(
        String.format(
            "Unsupported database type '%s'. Only PostgreSQL and MySQL variants are supported.",
            value));
  }

  /**
   * Detect database type from JDBC URL scheme.
   *
   * @param scheme the JDBC URL scheme
   * @return the corresponding DatabaseType enum
   * @throws IllegalArgumentException if the scheme is not supported
   */
  public static DatabaseType fromJdbcScheme(String scheme) {
    if (scheme == null) {
      throw new IllegalArgumentException("JDBC scheme cannot be null");
    }

    String normalizedScheme = scheme.toLowerCase().trim();
    switch (normalizedScheme) {
      case "postgresql":
      case "google-cloud-sql-postgresql":
        return POSTGRES;
      case "mysql":
      case "mariadb":
      case "google-cloud-sql-mysql":
        return MYSQL;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported JDBC scheme '%s'. Only PostgreSQL and MySQL variants are supported.",
                scheme));
    }
  }

  /**
   * Detect database type from JDBC URL content using fallback detection.
   *
   * @param jdbcUrl the JDBC URL to analyze
   * @return the corresponding DatabaseType enum
   * @throws IllegalArgumentException if the URL doesn't contain recognizable database identifiers
   */
  public static DatabaseType fromJdbcUrlContent(String jdbcUrl) {
    if (jdbcUrl == null) {
      throw new IllegalArgumentException("JDBC URL cannot be null");
    }

    String urlLower = jdbcUrl.toLowerCase();
    if (urlLower.contains("postgres")) {
      return POSTGRES;
    } else if (urlLower.contains("mysql") || urlLower.contains("mariadb")) {
      return MYSQL;
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Cannot detect database type from JDBC URL '%s'. Only PostgreSQL and MySQL variants are supported.",
              jdbcUrl));
    }
  }
}
