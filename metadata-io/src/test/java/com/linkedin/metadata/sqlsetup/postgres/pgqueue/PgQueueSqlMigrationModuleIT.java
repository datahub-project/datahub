package com.linkedin.metadata.sqlsetup.postgres.pgqueue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupSession;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationResult;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PgQueueSqlMigrationModuleIT {

  private PostgreSQLContainer<?> postgres;
  private PostgresTestUtils.IntegrationNamespace ns;

  @BeforeClass
  public void setUp() {
    postgres = PostgresTestUtils.startPostgres();
    ns = PostgresTestUtils.newIntegrationNamespace("pgqueue_mig");
  }

  @Test
  public void pgQueueModuleMigratesOnEmptyDatabase() throws Exception {
    PostgresSqlSetupProperties props = pgQueueProps();
    props.getPgQueue().setSchema(ns.getSchema());
    props.getPgQueue().setTablePrefix(ns.getTablePrefix());
    PgQueueSetupOptions options = props.buildPgQueueOptions();
    assert options != null;

    try (Connection connection = postgres.createConnection("")) {
      connection.setAutoCommit(true);
      ensurePgPartman(connection);

      String partmanSchema = PgQueueSqlSetupSupport.resolvePgPartmanExtensionSchema(connection);
      assert partmanSchema != null;

      PgQueueSqlMigrationTokens tokens =
          PgQueueSqlMigrationTokens.builder()
              .quotedSchema(PostgresSqlUtils.quotePgIdentifier(ns.getSchema()))
              .tablePrefix(ns.getTablePrefix())
              .batchDeleteLimit("5000")
              .partmanParentQualified(ns.getSchema() + "." + ns.getTablePrefix() + "_message")
              .partmanInterval("1 day")
              .partmanPremake("4")
              .retentionPartmanTail(
                  PgQueueSqlSetupSupport.buildRetentionPartmanTail(
                      partmanSchema, ns.getSchema(), ns.getTablePrefix()))
              .build();

      var module = PgQueueSqlMigrationModules.from(options, tokens);

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
                  "SELECT to_regclass('"
                      + ns.getSchema()
                      + "."
                      + ns.getTablePrefix()
                      + "_message') IS NOT NULL")) {
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
      }
    }
  }

  private static void ensurePgPartman(Connection connection) throws Exception {
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE EXTENSION IF NOT EXISTS pg_partman");
    }
  }

  private static PostgresSqlSetupProperties pgQueueProps() {
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.getPgQueue().setEnabled(true);
    props.getPgQueue().getRetention().setPartmanPartitionInterval("1 day");
    props.getPgQueue().getRetention().setPartmanPremake(4);
    return props;
  }
}
