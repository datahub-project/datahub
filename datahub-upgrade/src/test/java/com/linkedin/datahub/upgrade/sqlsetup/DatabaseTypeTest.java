package com.linkedin.datahub.upgrade.sqlsetup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;

/** Unit tests for DatabaseType enum. */
public class DatabaseTypeTest {

  @Test
  public void testFromStringValidPostgresql() {
    // Test standard postgresql value
    assertEquals(DatabaseType.fromString("postgresql"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("PostgreSQL"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("POSTGRESQL"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("  postgresql  "), DatabaseType.POSTGRES);
  }

  @Test
  public void testFromStringValidMysql() {
    // Test standard mysql value
    assertEquals(DatabaseType.fromString("mysql"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromString("MySQL"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromString("MYSQL"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromString("  mysql  "), DatabaseType.MYSQL);
  }

  @Test
  public void testFromStringLegacyPostgres() {
    // Test legacy "postgres" value (should map to POSTGRES)
    assertEquals(DatabaseType.fromString("postgres"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("Postgres"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("POSTGRES"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("  postgres  "), DatabaseType.POSTGRES);
  }

  @Test
  public void testFromStringNullValue() {
    // Test null value
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString(null));
    assertEquals(exception.getMessage(), "Database type cannot be null");
  }

  @Test
  public void testFromStringEmptyValue() {
    // Test empty string
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString(""));
    assertEquals(
        exception.getMessage(),
        "Unsupported database type ''. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromStringWhitespaceOnly() {
    // Test whitespace-only string
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString("   "));
    assertEquals(
        exception.getMessage(),
        "Unsupported database type '   '. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromStringInvalidValue() {
    // Test invalid database type
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString("oracle"));
    assertEquals(
        exception.getMessage(),
        "Unsupported database type 'oracle'. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromStringInvalidValueWithWhitespace() {
    // Test invalid database type with whitespace
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString("  oracle  "));
    assertEquals(
        exception.getMessage(),
        "Unsupported database type '  oracle  '. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromStringCaseInsensitive() {
    // Test various case combinations
    assertEquals(DatabaseType.fromString("PostgreSQL"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("postgresql"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("POSTGRESQL"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("Postgresql"), DatabaseType.POSTGRES);

    assertEquals(DatabaseType.fromString("MySQL"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromString("mysql"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromString("MYSQL"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromString("Mysql"), DatabaseType.MYSQL);
  }

  @Test
  public void testFromStringTrimming() {
    // Test that whitespace is properly trimmed
    assertEquals(DatabaseType.fromString("  postgresql  "), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("\tpostgresql\t"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("\npostgresql\n"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("  mysql  "), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromString("\tmysql\t"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromString("\nmysql\n"), DatabaseType.MYSQL);
  }

  @Test
  public void testFromStringSpecialCharacters() {
    // Test strings with special characters that should fail
    IllegalArgumentException exception1 =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString("postgresql!"));
    assertEquals(
        exception1.getMessage(),
        "Unsupported database type 'postgresql!'. Only PostgreSQL and MySQL variants are supported.");

    IllegalArgumentException exception2 =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString("mysql@"));
    assertEquals(
        exception2.getMessage(),
        "Unsupported database type 'mysql@'. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromStringPartialMatches() {
    // Test strings that partially match but shouldn't be accepted
    IllegalArgumentException exception1 =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString("postgresqlx"));
    assertEquals(
        exception1.getMessage(),
        "Unsupported database type 'postgresqlx'. Only PostgreSQL and MySQL variants are supported.");

    IllegalArgumentException exception2 =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString("xmysql"));
    assertEquals(
        exception2.getMessage(),
        "Unsupported database type 'xmysql'. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testGetValue() {
    // Test getValue() method
    assertEquals(DatabaseType.POSTGRES.getValue(), "postgresql");
    assertEquals(DatabaseType.MYSQL.getValue(), "mysql");
  }

  @Test
  public void testEnumValues() {
    // Test that enum has expected values
    DatabaseType[] values = DatabaseType.values();
    assertEquals(values.length, 2);
    assertEquals(values[0], DatabaseType.POSTGRES);
    assertEquals(values[1], DatabaseType.MYSQL);
  }

  // Tests for fromJdbcScheme() method

  @Test
  public void testFromJdbcSchemeValidPostgresql() {
    // Test standard postgresql scheme
    assertEquals(DatabaseType.fromJdbcScheme("postgresql"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("PostgreSQL"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("POSTGRESQL"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("  postgresql  "), DatabaseType.POSTGRES);
  }

  @Test
  public void testFromJdbcSchemeValidGoogleCloudSqlPostgresql() {
    // Test Google Cloud SQL PostgreSQL scheme
    assertEquals(DatabaseType.fromJdbcScheme("google-cloud-sql-postgresql"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("Google-Cloud-SQL-PostgreSQL"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("GOOGLE-CLOUD-SQL-POSTGRESQL"), DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcScheme("  google-cloud-sql-postgresql  "), DatabaseType.POSTGRES);
  }

  @Test
  public void testFromJdbcSchemeValidMysql() {
    // Test standard mysql scheme
    assertEquals(DatabaseType.fromJdbcScheme("mysql"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("MySQL"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("MYSQL"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("  mysql  "), DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcSchemeValidMariadb() {
    // Test MariaDB scheme
    assertEquals(DatabaseType.fromJdbcScheme("mariadb"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("MariaDB"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("MARIADB"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("  mariadb  "), DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcSchemeValidGoogleCloudSqlMysql() {
    // Test Google Cloud SQL MySQL scheme
    assertEquals(DatabaseType.fromJdbcScheme("google-cloud-sql-mysql"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("Google-Cloud-SQL-MySQL"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("GOOGLE-CLOUD-SQL-MYSQL"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("  google-cloud-sql-mysql  "), DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcSchemeNullValue() {
    // Test null scheme
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme(null));
    assertEquals(exception.getMessage(), "JDBC scheme cannot be null");
  }

  @Test
  public void testFromJdbcSchemeEmptyValue() {
    // Test empty scheme
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme(""));
    assertEquals(
        exception.getMessage(),
        "Unsupported JDBC scheme ''. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromJdbcSchemeWhitespaceOnly() {
    // Test whitespace-only scheme
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("   "));
    assertEquals(
        exception.getMessage(),
        "Unsupported JDBC scheme '   '. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromJdbcSchemeInvalidValue() {
    // Test invalid JDBC scheme
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("oracle"));
    assertEquals(
        exception.getMessage(),
        "Unsupported JDBC scheme 'oracle'. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromJdbcSchemeInvalidValueWithWhitespace() {
    // Test invalid JDBC scheme with whitespace
    IllegalArgumentException exception =
        expectThrows(
            IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("  oracle  "));
    assertEquals(
        exception.getMessage(),
        "Unsupported JDBC scheme '  oracle  '. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromJdbcSchemeCaseInsensitive() {
    // Test various case combinations
    assertEquals(DatabaseType.fromJdbcScheme("PostgreSQL"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("postgresql"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("POSTGRESQL"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("Postgresql"), DatabaseType.POSTGRES);

    assertEquals(DatabaseType.fromJdbcScheme("MySQL"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("mysql"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("MYSQL"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("Mysql"), DatabaseType.MYSQL);

    assertEquals(DatabaseType.fromJdbcScheme("MariaDB"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("mariadb"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("MARIADB"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("Mariadb"), DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcSchemeTrimming() {
    // Test that whitespace is properly trimmed
    assertEquals(DatabaseType.fromJdbcScheme("  postgresql  "), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("\tpostgresql\t"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("\npostgresql\n"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("  mysql  "), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("\tmysql\t"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("\nmysql\n"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("  mariadb  "), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("\tmariadb\t"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("\nmariadb\n"), DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcSchemeSpecialCharacters() {
    // Test schemes with special characters that should fail
    IllegalArgumentException exception1 =
        expectThrows(
            IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("postgresql!"));
    assertEquals(
        exception1.getMessage(),
        "Unsupported JDBC scheme 'postgresql!'. Only PostgreSQL and MySQL variants are supported.");

    IllegalArgumentException exception2 =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("mysql@"));
    assertEquals(
        exception2.getMessage(),
        "Unsupported JDBC scheme 'mysql@'. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromJdbcSchemePartialMatches() {
    // Test schemes that partially match but shouldn't be accepted
    IllegalArgumentException exception1 =
        expectThrows(
            IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("postgresqlx"));
    assertEquals(
        exception1.getMessage(),
        "Unsupported JDBC scheme 'postgresqlx'. Only PostgreSQL and MySQL variants are supported.");

    IllegalArgumentException exception2 =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("xmysql"));
    assertEquals(
        exception2.getMessage(),
        "Unsupported JDBC scheme 'xmysql'. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromJdbcSchemeOtherDatabaseTypes() {
    // Test other common database schemes that should fail
    IllegalArgumentException exception1 =
        expectThrows(
            IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("sqlserver"));
    assertEquals(
        exception1.getMessage(),
        "Unsupported JDBC scheme 'sqlserver'. Only PostgreSQL and MySQL variants are supported.");

    IllegalArgumentException exception2 =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("sqlite"));
    assertEquals(
        exception2.getMessage(),
        "Unsupported JDBC scheme 'sqlite'. Only PostgreSQL and MySQL variants are supported.");

    IllegalArgumentException exception3 =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("h2"));
    assertEquals(
        exception3.getMessage(),
        "Unsupported JDBC scheme 'h2'. Only PostgreSQL and MySQL variants are supported.");
  }

  // Tests for fromJdbcUrlContent() method

  @Test
  public void testFromJdbcUrlContentValidPostgresql() {
    // Test URLs containing "postgres"
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:postgresql://localhost:5432/mydb"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:postgresql://localhost:5432/mydb"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:postgresql://localhost:5432/mydb"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("postgresql://localhost:5432/mydb"), DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:google-cloud-sql-postgresql://localhost:5432/mydb"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("postgres://localhost:5432/mydb"), DatabaseType.POSTGRES);
  }

  @Test
  public void testFromJdbcUrlContentValidMysql() {
    // Test URLs containing "mysql"
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mysql://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mysql://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mysql://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("mysql://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:google-cloud-sql-mysql://localhost:3306/mydb"),
        DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcUrlContentValidMariadb() {
    // Test URLs containing "mariadb"
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mariadb://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mariadb://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mariadb://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("mariadb://localhost:3306/mydb"), DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcUrlContentCaseInsensitive() {
    // Test case insensitive detection
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:PostgreSQL://localhost:5432/mydb"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:POSTGRESQL://localhost:5432/mydb"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:Postgresql://localhost:5432/mydb"),
        DatabaseType.POSTGRES);

    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:MySQL://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:MYSQL://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:Mysql://localhost:3306/mydb"), DatabaseType.MYSQL);

    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:MariaDB://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:MARIADB://localhost:3306/mydb"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:Mariadb://localhost:3306/mydb"), DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcUrlContentPartialMatches() {
    // Test URLs with partial matches that should still work
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:postgresql://localhost:5432/postgres"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mysql://localhost:3306/mysql"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mariadb://localhost:3306/mariadb"),
        DatabaseType.MYSQL);

    // Test URLs with database names containing the keywords
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:postgresql://localhost:5432/postgresql_db"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mysql://localhost:3306/mysql_db"),
        DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mariadb://localhost:3306/mariadb_db"),
        DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcUrlContentNullValue() {
    // Test null URL
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcUrlContent(null));
    assertEquals(exception.getMessage(), "JDBC URL cannot be null");
  }

  @Test
  public void testFromJdbcUrlContentEmptyValue() {
    // Test empty URL
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcUrlContent(""));
    assertEquals(
        exception.getMessage(),
        "Cannot detect database type from JDBC URL ''. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromJdbcUrlContentInvalidValue() {
    // Test URL without recognizable database identifiers
    IllegalArgumentException exception =
        expectThrows(
            IllegalArgumentException.class,
            () -> DatabaseType.fromJdbcUrlContent("jdbc:oracle://localhost:1521/mydb"));
    assertEquals(
        exception.getMessage(),
        "Cannot detect database type from JDBC URL 'jdbc:oracle://localhost:1521/mydb'. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromJdbcUrlContentOtherDatabaseTypes() {
    // Test other common database URLs that should fail
    IllegalArgumentException exception1 =
        expectThrows(
            IllegalArgumentException.class,
            () -> DatabaseType.fromJdbcUrlContent("jdbc:sqlserver://localhost:1433/mydb"));
    assertEquals(
        exception1.getMessage(),
        "Cannot detect database type from JDBC URL 'jdbc:sqlserver://localhost:1433/mydb'. Only PostgreSQL and MySQL variants are supported.");

    IllegalArgumentException exception2 =
        expectThrows(
            IllegalArgumentException.class,
            () -> DatabaseType.fromJdbcUrlContent("jdbc:sqlite:memory:mydb"));
    assertEquals(
        exception2.getMessage(),
        "Cannot detect database type from JDBC URL 'jdbc:sqlite:memory:mydb'. Only PostgreSQL and MySQL variants are supported.");

    IllegalArgumentException exception3 =
        expectThrows(
            IllegalArgumentException.class,
            () -> DatabaseType.fromJdbcUrlContent("jdbc:h2:mem:mydb"));
    assertEquals(
        exception3.getMessage(),
        "Cannot detect database type from JDBC URL 'jdbc:h2:mem:mydb'. Only PostgreSQL and MySQL variants are supported.");
  }

  @Test
  public void testFromJdbcUrlContentComplexUrls() {
    // Test complex URLs with additional parameters
    assertEquals(
        DatabaseType.fromJdbcUrlContent(
            "jdbc:postgresql://localhost:5432/mydb?user=test&password=secret"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent(
            "jdbc:mysql://localhost:3306/mydb?useSSL=false&serverTimezone=UTC"),
        DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent(
            "jdbc:mariadb://localhost:3306/mydb?useSSL=false&serverTimezone=UTC"),
        DatabaseType.MYSQL);

    // Test URLs with connection pooling
    assertEquals(
        DatabaseType.fromJdbcUrlContent(
            "jdbc:postgresql://localhost:5432/mydb?pool=true&maxConnections=10"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent(
            "jdbc:mysql://localhost:3306/mydb?pool=true&maxConnections=10"),
        DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcUrlContentCloudUrls() {
    // Test cloud database URLs
    assertEquals(
        DatabaseType.fromJdbcUrlContent(
            "jdbc:postgresql://my-postgres-instance.amazonaws.com:5432/mydb"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mysql://my-mysql-instance.amazonaws.com:3306/mydb"),
        DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent(
            "jdbc:google-cloud-sql-postgresql://my-project:us-central1:my-instance/mydb"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent(
            "jdbc:google-cloud-sql-mysql://my-project:us-central1:my-instance/mydb"),
        DatabaseType.MYSQL);
  }
}
