package com.linkedin.metadata.config.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class JdbcUrlParserTest {

  @Test
  public void testParseStandardMysqlUrl() {
    String url = "jdbc:mysql://localhost:3306/datahub";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.databaseType, DatabaseType.MYSQL);
    assertEquals(info.host, "localhost");
    assertEquals(info.port, 3306);
    assertEquals(info.database, "datahub");
  }

  @Test
  public void testParseStandardPostgresUrl() {
    String url = "jdbc:postgresql://localhost:5432/datahub";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.databaseType, DatabaseType.POSTGRES);
    assertEquals(info.host, "localhost");
    assertEquals(info.port, 5432);
    assertEquals(info.database, "datahub");
    assertEquals(info.currentSchema, null);
  }

  @Test
  public void testParsePostgresUrlWithCurrentSchema() {
    String url = "jdbc:postgresql://localhost:5432/datahub?currentSchema=app&sslmode=disable";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.database, "datahub");
    assertEquals(info.currentSchema, "app");
  }

  @Test
  public void testParseUrlWithQueryParameters() {
    String url =
        "jdbc:mysql://mysql:3306/datahub?verifyServerCertificate=false&useSSL=true&useUnicode=yes&characterEncoding=UTF-8";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.databaseType, DatabaseType.MYSQL);
    assertEquals(info.host, "mysql");
    assertEquals(info.port, 3306);
    assertEquals(info.database, "datahub");
  }

  @Test
  public void testParseUrlWithCredentials() {
    String url = "jdbc:mysql://user:pass@localhost:3306/testdb";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.databaseType, DatabaseType.MYSQL);
    assertEquals(info.host, "localhost");
    assertEquals(info.port, 3306);
    assertEquals(info.database, "testdb");
  }

  @Test
  public void testParseUrlWithoutPort() {
    String url = "jdbc:mysql://testhost/testdb";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.databaseType, DatabaseType.MYSQL);
    assertEquals(info.host, "testhost");
    assertEquals(info.port, 3306); // Default MySQL port
    assertEquals(info.database, "testdb");
  }

  @Test
  public void testParseUrlWithoutDatabase() {
    String url = "jdbc:mysql://localhost:3306/";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.databaseType, DatabaseType.MYSQL);
    assertEquals(info.host, "localhost");
    assertEquals(info.port, 3306);
    assertEquals(info.database, "datahub"); // Default database name
  }

  @Test
  public void testParseUrlWithComplexDatabaseName() {
    String url = "jdbc:mysql://localhost:3306/test-db_123";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.databaseType, DatabaseType.MYSQL);
    assertEquals(info.host, "localhost");
    assertEquals(info.port, 3306);
    assertEquals(info.database, "test-db_123");
  }

  @Test
  public void testParseUrlWithMultipleSlashes() {
    String url = "jdbc:mysql://localhost:3306//testdb";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.databaseType, DatabaseType.MYSQL);
    assertEquals(info.host, "localhost");
    assertEquals(info.port, 3306);
    assertEquals(info.database, "testdb");
  }

  @Test
  public void testParseMalformedUrl() {
    String url = "jdbc:mysql://localhost:3306";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.databaseType, DatabaseType.MYSQL);
    assertEquals(info.host, "localhost");
    assertEquals(info.port, 3306);
    assertEquals(info.database, "datahub"); // Default fallback
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNullUrl() {
    JdbcUrlParser.parseJdbcUrl(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseEmptyUrl() {
    JdbcUrlParser.parseJdbcUrl("");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseUrlWithInvalidPort() {
    String url = "jdbc:mysql://testhost:invalid/testdb";
    JdbcUrlParser.parseJdbcUrl(url);
  }

  @Test
  public void testParsePostgresUrlWithoutPort() {
    String url = "jdbc:postgresql://testhost/testdb";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);

    assertEquals(info.databaseType, DatabaseType.POSTGRES);
    assertEquals(info.host, "testhost");
    assertEquals(info.port, 5432); // Default PostgreSQL port
    assertEquals(info.database, "testdb");
  }

  @Test
  public void testCreateUrlWithoutDatabase() {
    String url = "jdbc:mysql://localhost:3306/datahub";
    String urlWithoutDb = JdbcUrlParser.createUrlWithoutDatabase(url);
    assertEquals(urlWithoutDb, "jdbc:mysql://localhost:3306/");
  }

  @Test
  public void testCreateUrlWithoutDatabasePostgres() {
    String url = "jdbc:postgresql://testhost:5432/testdb";
    String urlWithoutDb = JdbcUrlParser.createUrlWithoutDatabase(url);
    assertEquals(urlWithoutDb, "jdbc:postgresql://testhost:5432/");
  }

  @Test
  public void testCreateUrlWithoutDatabaseWithQueryParams() {
    String url = "jdbc:mysql://mysql:3306/datahub?verifyServerCertificate=false&useSSL=true";
    String urlWithoutDb = JdbcUrlParser.createUrlWithoutDatabase(url);
    assertEquals(
        urlWithoutDb, "jdbc:mysql://mysql:3306/?verifyServerCertificate=false&useSSL=true");
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = "JDBC URL cannot be null or empty")
  public void testParseWhitespaceOnlyUrlThrowsException() {
    JdbcUrlParser.parseJdbcUrl("   ");
  }

  @Test
  public void testParseAwsWrapperMysqlUrl() {
    String url = "jdbc:aws-wrapper:mysql://localhost:3306/datahub";
    JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(url);
    assertEquals(info.databaseType, DatabaseType.MYSQL);
    assertEquals(info.host, "localhost");
    assertEquals(info.port, 3306);
    assertEquals(info.database, "datahub");
  }

  @Test
  public void testParseMariadbUrl() {
    JdbcUrlParser.JdbcInfo info =
        JdbcUrlParser.parseJdbcUrl("jdbc:mariadb://db.example.com:3307/mydb");
    assertEquals(info.databaseType, DatabaseType.MYSQL);
    assertEquals(info.database, "mydb");
    assertEquals(info.port, 3307);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseUrlWithPortOutOfRange() {
    JdbcUrlParser.parseJdbcUrl("jdbc:mysql://localhost:70000/datahub");
  }

  @Test
  public void extractQueryParameterIgnoreCase_decodesValues() {
    assertNull(JdbcUrlParser.extractQueryParameterIgnoreCase(null, "currentSchema"));
    assertNull(JdbcUrlParser.extractQueryParameterIgnoreCase("", "currentSchema"));
    assertEquals(
        JdbcUrlParser.extractQueryParameterIgnoreCase(
            "CurrentSchema=app&ssl=true", "currentSchema"),
        "app");
    assertEquals(
        JdbcUrlParser.extractQueryParameterIgnoreCase(
            "ssl=true&currentSchema=queue%2Fmain", "currentSchema"),
        "queue/main");
  }

  @Test
  public void applyPostgresMetadataSchema_appendsCurrentSchema() {
    assertEquals(
        JdbcUrlParser.applyPostgresMetadataSchema(
            "jdbc:postgresql://localhost:5432/datahub", "dhub"),
        "jdbc:postgresql://localhost:5432/datahub?currentSchema=dhub");
    assertEquals(
        JdbcUrlParser.applyPostgresMetadataSchema(
            "jdbc:postgresql://localhost:5432/datahub?sslmode=disable", "Dhub"),
        "jdbc:postgresql://localhost:5432/datahub?sslmode=disable&currentSchema=dhub");
  }

  @Test
  public void applyPostgresMetadataSchema_leavesDefaultAndNonPostgresUntouched() {
    String postgres = "jdbc:postgresql://localhost:5432/datahub";
    String mysql = "jdbc:mysql://localhost:3306/datahub";
    assertEquals(JdbcUrlParser.applyPostgresMetadataSchema(postgres, "public"), postgres);
    assertEquals(JdbcUrlParser.applyPostgresMetadataSchema(postgres, " PUBLIC "), postgres);
    assertEquals(JdbcUrlParser.applyPostgresMetadataSchema(mysql, "dhub"), mysql);
    assertEquals(JdbcUrlParser.applyPostgresMetadataSchema(mysql, "bad-name"), mysql);
    assertEquals(JdbcUrlParser.applyPostgresMetadataSchema(null, "dhub"), null);
  }

  @Test
  public void applyPostgresMetadataSchema_keepsMatchingCurrentSchema() {
    String url = "jdbc:postgresql://localhost:5432/datahub?currentSchema=dhub&sslmode=disable";
    assertEquals(JdbcUrlParser.applyPostgresMetadataSchema(url, "dhub"), url);
  }

  @Test
  public void applyPostgresMetadataSchema_normalizesCaseOfExistingCurrentSchema() {
    assertEquals(
        JdbcUrlParser.applyPostgresMetadataSchema(
            "jdbc:postgresql://localhost:5432/datahub?CurrentSchema=Dhub", "dhub"),
        "jdbc:postgresql://localhost:5432/datahub?currentSchema=dhub");
  }

  @Test
  public void applyPostgresMetadataSchema_keepsSearchPathListWhenFirstEntryMatches() {
    String url = "jdbc:postgresql://localhost:5432/datahub?currentSchema=dhub,public";
    assertEquals(JdbcUrlParser.applyPostgresMetadataSchema(url, "dhub"), url);
  }

  @Test
  public void applyPostgresMetadataSchema_rejectsSearchPathListWithDifferentFirstEntry() {
    try {
      JdbcUrlParser.applyPostgresMetadataSchema(
          "jdbc:postgresql://localhost:5432/datahub?currentSchema=public,dhub", "dhub");
      throw new AssertionError("expected search path starting at public to fail");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("public,dhub"));
    }
  }

  @Test
  public void applyPostgresMetadataSchema_rejectsPublicSchemaWhenUrlUsesAnotherSchema() {
    try {
      JdbcUrlParser.applyPostgresMetadataSchema(
          "jdbc:postgresql://localhost:5432/datahub?currentSchema=other", "public");
      throw new AssertionError("expected public schema to reject a different currentSchema");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("other"));
    }
  }

  @Test
  public void applyPostgresMetadataSchema_rejectsConflictingCurrentSchema() {
    try {
      JdbcUrlParser.applyPostgresMetadataSchema(
          "jdbc:postgresql://localhost:5432/datahub?currentSchema=other", "dhub");
      throw new AssertionError("expected conflicting currentSchema to fail");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("other"));
    }
  }

  @Test
  public void applyPostgresMetadataSchema_rejectsInvalidIdentifier() {
    try {
      JdbcUrlParser.applyPostgresMetadataSchema(
          "jdbc:postgresql://localhost:5432/datahub", "bad-name");
      throw new AssertionError("expected invalid schema identifier to fail");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("identifier"));
    }
  }

  @Test
  public void createUrlWithoutDatabase_preservesMariadbScheme() {
    String url = "jdbc:mariadb://host:3306/db?useSSL=false";
    assertEquals(
        JdbcUrlParser.createUrlWithoutDatabase(url), "jdbc:mariadb://host:3306/?useSSL=false");
  }
}
