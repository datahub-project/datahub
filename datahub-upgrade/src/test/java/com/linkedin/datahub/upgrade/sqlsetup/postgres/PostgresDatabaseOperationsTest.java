package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PostgresDatabaseOperationsTest {

  private PostgresDatabaseOperations ops;
  private Method escapeIdentifier;
  private Method escapeStringLiteral;

  @BeforeMethod
  public void setUp() throws Exception {
    ops = new PostgresDatabaseOperations();
    escapeIdentifier =
        PostgresDatabaseOperations.class.getDeclaredMethod(
            "escapePostgresIdentifier", String.class);
    escapeIdentifier.setAccessible(true);
    escapeStringLiteral =
        PostgresDatabaseOperations.class.getDeclaredMethod(
            "escapePostgresStringLiteral", String.class);
    escapeStringLiteral.setAccessible(true);
  }

  // --- escapePostgresIdentifier ---

  @Test
  public void testEscapeIdentifier_normalName() throws Exception {
    String result = (String) escapeIdentifier.invoke(ops, "my_table");
    assertEquals(result, "\"my_table\"");
  }

  @Test
  public void testEscapeIdentifier_withDoubleQuotes() throws Exception {
    String result = (String) escapeIdentifier.invoke(ops, "table\"name");
    assertEquals(result, "\"table\"\"name\"");
  }

  @Test
  public void testEscapeIdentifier_emptyString() throws Exception {
    String result = (String) escapeIdentifier.invoke(ops, "");
    assertEquals(result, "\"\"");
  }

  // --- escapePostgresStringLiteral ---

  @Test
  public void testEscapeStringLiteral_normalString() throws Exception {
    String result = (String) escapeStringLiteral.invoke(ops, "hello");
    assertEquals(result, "'hello'");
  }

  @Test
  public void testEscapeStringLiteral_withSingleQuotes() throws Exception {
    String result = (String) escapeStringLiteral.invoke(ops, "it's");
    assertEquals(result, "'it''s'");
  }

  @Test
  public void testEscapeStringLiteral_sqlInjectionPayload() throws Exception {
    String result = (String) escapeStringLiteral.invoke(ops, "'; DROP TABLE users; --");
    assertEquals(result, "'''; DROP TABLE users; --'");
    // After removing properly escaped '' pairs from the inner content, no stray ' should remain.
    String inner = result.substring(1, result.length() - 1);
    assertFalse(
        inner.replace("''", "").contains("'"),
        "Unescaped single quote would allow SQL injection breakout");
  }

  // --- createIamUserSql ---

  @Test
  public void testCreateIamUserSql_generatesExpectedShape() {
    String sql = ops.createIamUserSql("app_user", "arn:aws:iam::123456:role/rds-role");

    assertTrue(sql.contains("CREATE USER \"app_user\" WITH LOGIN"));
    assertTrue(sql.contains("GRANT rds_iam TO \"app_user\""));
    assertTrue(sql.contains("pg_catalog.pg_roles WHERE rolname = 'app_user'"));
  }

  @Test
  public void testCreateIamUserSql_escapesSpecialChars() {
    String sql = ops.createIamUserSql("user\"inject", "some-role");

    assertTrue(sql.contains("\"user\"\"inject\""));
    assertFalse(sql.contains("\"user\"inject\""));
  }

  // --- createTraditionalUserSql ---

  @Test
  public void testCreateTraditionalUserSql_generatesExpectedShape() {
    String sql = ops.createTraditionalUserSql("datahub", "secret123");

    assertTrue(sql.contains("CREATE USER \"datahub\" WITH PASSWORD 'secret123'"));
    assertTrue(sql.contains("pg_catalog.pg_roles WHERE rolname = 'datahub'"));
  }

  @Test
  public void testCreateTraditionalUserSql_escapesPassword() {
    String sql = ops.createTraditionalUserSql("user", "pass'word");

    assertTrue(sql.contains("WITH PASSWORD 'pass''word'"));
    assertFalse(
        sql.contains("'pass'word'"), "Unescaped single quote in password would break SQL syntax");
  }

  // --- grantPrivilegesSql ---

  @Test
  public void testGrantPrivilegesSql_generatesExpectedShape() {
    String sql = ops.grantPrivilegesSql("app_user", "datahub_db");

    assertEquals(sql, "GRANT ALL PRIVILEGES ON DATABASE \"datahub_db\" TO \"app_user\";");
  }

  @Test
  public void testGrantPrivilegesSql_escapesIdentifiers() {
    String sql = ops.grantPrivilegesSql("user\"x", "db\"y");

    assertTrue(sql.contains("\"user\"\"x\""));
    assertTrue(sql.contains("\"db\"\"y\""));
  }

  // --- grantCdcPrivilegesSql ---

  @Test
  public void testGrantCdcPrivilegesSql_containsExpectedStatements() {
    List<String> statements = ops.grantCdcPrivilegesSql("cdc_user", "datahub_db", "public");

    assertEquals(statements.size(), 10);
    assertTrue(statements.get(0).contains("ALTER USER \"cdc_user\" WITH REPLICATION"));
    assertTrue(statements.get(1).contains("GRANT CONNECT ON DATABASE \"datahub_db\""));
    assertTrue(statements.get(2).contains("GRANT USAGE ON SCHEMA \"public\""));
    assertTrue(statements.get(3).contains("GRANT CREATE ON DATABASE \"datahub_db\""));
    assertTrue(statements.get(4).contains("GRANT SELECT ON ALL TABLES IN SCHEMA \"public\""));
    assertTrue(statements.get(5).contains("ALTER DEFAULT PRIVILEGES IN SCHEMA \"public\""));
    assertTrue(statements.get(6).contains("ALTER USER \"cdc_user\" WITH SUPERUSER"));
    assertTrue(
        statements.get(7).contains("ALTER TABLE \"public\".\"metadata_aspect_v2\" OWNER TO"));
    assertTrue(
        statements
            .get(8)
            .contains("ALTER TABLE \"public\".\"metadata_aspect_v2\" REPLICA IDENTITY FULL"));
    assertTrue(
        statements
            .get(9)
            .contains(
                "CREATE PUBLICATION dbz_publication FOR TABLE \"public\".\"metadata_aspect_v2\""));
  }

  @Test
  public void testGrantCdcPrivilegesSql_nullSchemaDefaultsToPublic() {
    List<String> statements = ops.grantCdcPrivilegesSql("cdc_user", "datahub_db", null);

    assertTrue(statements.get(2).contains("GRANT USAGE ON SCHEMA \"public\""));
  }

  @Test
  public void testGrantCdcPrivilegesSql_blankSchemaDefaultsToPublic() {
    List<String> statements = ops.grantCdcPrivilegesSql("cdc_user", "datahub_db", "   ");

    assertTrue(statements.get(2).contains("GRANT USAGE ON SCHEMA \"public\""));
  }

  @Test
  public void testGrantCdcPrivilegesSql_customSchema() {
    List<String> statements = ops.grantCdcPrivilegesSql("cdc_user", "datahub_db", "my_schema");

    assertTrue(statements.get(2).contains("GRANT USAGE ON SCHEMA \"my_schema\""));
    assertTrue(statements.get(7).contains("\"my_schema\".\"metadata_aspect_v2\""));
  }
}
