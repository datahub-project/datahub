package com.linkedin.metadata.sqlsetup.postgres.migration;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.PostgresTestUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PostgresSqlMigrationRunnerMultiModuleTest {

  private PostgreSQLContainer<?> postgres;
  private PostgresTestUtils.IntegrationNamespace nsA;
  private PostgresTestUtils.IntegrationNamespace nsB;

  @BeforeClass
  public void setUp() {
    postgres = PostgresTestUtils.startPostgres();
    nsA = PostgresTestUtils.newIntegrationNamespace("mig_a");
    nsB = PostgresTestUtils.newIntegrationNamespace("mig_b");
  }

  @Test
  public void twoModulesUseSeparateLedgers() throws Exception {
    SqlMigrationModule moduleA =
        SqlMigrationModule.builder()
            .migrationNamespace("fixture_a")
            .targetSchema(nsA.getSchema())
            .classpathLocation("sqlsetup/testfixture/migrations")
            .ledgerTableName(nsA.getTablePrefix() + "_schema_migration")
            .tokenReplacement("__PREFIX__", nsA.getTablePrefix())
            .build();

    SqlMigrationModule moduleB =
        SqlMigrationModule.builder()
            .migrationNamespace("fixture_b")
            .targetSchema(nsB.getSchema())
            .classpathLocation("sqlsetup/testfixture/migrations")
            .ledgerTableName(nsB.getTablePrefix() + "_schema_migration")
            .tokenReplacement("__PREFIX__", nsB.getTablePrefix())
            .build();

    try (Connection connection = postgres.createConnection("")) {
      connection.setAutoCommit(true);
      PostgresSqlMigrationRunner.migrate(connection, moduleA);
      PostgresSqlMigrationRunner.migrate(connection, moduleB);

      assertLedgerRowCount(connection, nsA, 3);
      assertLedgerRowCount(connection, nsB, 3);
    }
  }

  private static void assertLedgerRowCount(
      Connection connection, PostgresTestUtils.IntegrationNamespace ns, int expected)
      throws Exception {
    try (Statement st = connection.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT COUNT(*) FROM "
                    + PostgresSqlUtils.quotePgIdentifier(ns.getSchema())
                    + "."
                    + PostgresSqlUtils.quotePgIdentifier(
                        ns.getTablePrefix() + "_schema_migration"))) {
      if (!rs.next()) {
        throw new AssertionError("expected COUNT row");
      }
      assertEquals(rs.getInt(1), expected);
    }
  }
}
