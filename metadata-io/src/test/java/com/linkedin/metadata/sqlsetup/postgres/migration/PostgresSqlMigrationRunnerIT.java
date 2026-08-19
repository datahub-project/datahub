package com.linkedin.metadata.sqlsetup.postgres.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupSession;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PostgresSqlMigrationRunnerIT {

  private PostgreSQLContainer<?> postgres;
  private PostgresTestUtils.IntegrationNamespace ns;

  @BeforeClass
  public void setUp() {
    postgres = PostgresTestUtils.startPostgres();
    ns = PostgresTestUtils.newIntegrationNamespace("sql_migration");
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (postgres != null) {
      // shared container; no stop
    }
  }

  @Test
  public void migrateAppliesVersionedAndRepeatable() throws Exception {
    SqlMigrationModule module =
        SqlMigrationModule.builder()
            .migrationNamespace("testfixture")
            .targetSchema(ns.getSchema())
            .classpathLocation("sqlsetup/testfixture/migrations")
            .ledgerTableName(ns.getTablePrefix() + "_schema_migration")
            .tokenReplacement("__PREFIX__", ns.getTablePrefix())
            .build();

    try (Connection connection = postgres.createConnection("")) {
      connection.setAutoCommit(true);
      SqlMigrationResult first = PostgresSqlMigrationRunner.migrate(connection, module);
      assertEquals(first.getApplied().size(), 3);
      assertTrue(first.getSkipped().isEmpty());

      SqlMigrationResult second = PostgresSqlMigrationRunner.migrate(connection, module);
      assertEquals(second.getApplied().size(), 0);
      assertEquals(second.getSkipped().size(), 3);

      PostgresSqlSetupSession.ensureSchemaAndSearchPath(connection, ns.getSchema());
      try (Statement st = connection.createStatement();
          ResultSet rs =
              st.executeQuery(
                  "SELECT COUNT(*) FROM "
                      + PostgresSqlUtils.quotePgIdentifier(
                          ns.getTablePrefix() + "_schema_migration"))) {
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 3);
      }
    }
  }

  @Test
  public void migrateFailsWhenVersionedChecksumChanges() throws Exception {
    PostgresTestUtils.IntegrationNamespace isolated =
        PostgresTestUtils.newIntegrationNamespace("sql_mig_chk");
    SqlMigrationModule module =
        SqlMigrationModule.builder()
            .migrationNamespace("testfixture_chk")
            .targetSchema(isolated.getSchema())
            .classpathLocation("sqlsetup/testfixture/migrations")
            .ledgerTableName(isolated.getTablePrefix() + "_schema_migration")
            .tokenReplacement("__PREFIX__", isolated.getTablePrefix())
            .build();

    try (Connection connection = postgres.createConnection("")) {
      connection.setAutoCommit(true);
      PostgresSqlMigrationRunner.migrate(connection, module);

      SqlMigrationModule tampered =
          SqlMigrationModule.builder()
              .migrationNamespace("testfixture_chk")
              .targetSchema(isolated.getSchema())
              .classpathLocation("sqlsetup/testfixture/migrations")
              .ledgerTableName(isolated.getTablePrefix() + "_schema_migration")
              .tokenReplacement("__PREFIX__", isolated.getTablePrefix() + "_x")
              .build();

      try {
        PostgresSqlMigrationRunner.migrate(connection, tampered);
        throw new AssertionError("expected checksum conflict");
      } catch (SqlMigrationException expected) {
        assertTrue(expected.getMessage().contains("different checksum"));
      }
    }
  }
}
