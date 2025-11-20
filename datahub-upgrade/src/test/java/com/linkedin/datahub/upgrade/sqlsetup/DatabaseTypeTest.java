package com.linkedin.datahub.upgrade.sqlsetup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;

/** Unit tests for DatabaseType enum. */
public class DatabaseTypeTest {

  @Test
  public void testFromString_ValidPostgres() {
    assertEquals(DatabaseType.fromString("postgresql"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("postgres"), DatabaseType.POSTGRES); // Legacy
    assertEquals(DatabaseType.fromString("POSTGRESQL"), DatabaseType.POSTGRES); // Case-insensitive
  }

  @Test
  public void testFromString_ValidMysql() {
    assertEquals(DatabaseType.fromString("mysql"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromString("MYSQL"), DatabaseType.MYSQL);
  }

  @Test
  public void testFromString_TrimsWhitespace() {
    assertEquals(DatabaseType.fromString("  postgresql  "), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromString("  mysql  "), DatabaseType.MYSQL);
  }

  @Test
  public void testFromString_ThrowsOnNull() {
    expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString(null));
  }

  @Test
  public void testFromString_ThrowsOnInvalid() {
    expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString("oracle"));
    expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromString(""));
  }

  @Test
  public void testFromJdbcScheme_ValidSchemes() {
    assertEquals(DatabaseType.fromJdbcScheme("postgresql"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("mysql"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("mariadb"), DatabaseType.MYSQL);
    assertEquals(DatabaseType.fromJdbcScheme("google-cloud-sql-postgresql"), DatabaseType.POSTGRES);
    assertEquals(DatabaseType.fromJdbcScheme("google-cloud-sql-mysql"), DatabaseType.MYSQL);
  }

  @Test
  public void testFromJdbcScheme_ThrowsOnInvalid() {
    expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme("oracle"));
    expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcScheme(null));
  }

  @Test
  public void testFromJdbcUrlContent_DetectsFromUrl() {
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:postgresql://localhost:5432/db"),
        DatabaseType.POSTGRES);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mysql://localhost:3306/db"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:mariadb://localhost:3306/db"), DatabaseType.MYSQL);
    assertEquals(
        DatabaseType.fromJdbcUrlContent("jdbc:google-cloud-sql-postgresql://localhost:5432/db"),
        DatabaseType.POSTGRES);
  }

  @Test
  public void testFromJdbcUrlContent_ThrowsOnUnrecognized() {
    expectThrows(
        IllegalArgumentException.class,
        () -> DatabaseType.fromJdbcUrlContent("jdbc:oracle://localhost:1521/db"));
    expectThrows(IllegalArgumentException.class, () -> DatabaseType.fromJdbcUrlContent(null));
  }

  @Test
  public void testGetValue() {
    assertEquals(DatabaseType.POSTGRES.getValue(), "postgresql");
    assertEquals(DatabaseType.MYSQL.getValue(), "mysql");
  }

  @Test
  public void testEnumValues() {
    DatabaseType[] values = DatabaseType.values();
    assertEquals(values.length, 2);
    assertEquals(values[0], DatabaseType.POSTGRES);
    assertEquals(values[1], DatabaseType.MYSQL);
  }
}
