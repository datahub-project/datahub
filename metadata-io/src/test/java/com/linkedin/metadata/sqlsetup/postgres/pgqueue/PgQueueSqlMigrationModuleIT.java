package com.linkedin.metadata.sqlsetup.postgres.pgqueue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.queue.QueueTableNames;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.postgres.EbeanPostgresMetadataQueueStore;
import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupSession;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationResult;
import io.ebean.Database;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
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

  @Test(dependsOnMethods = "pgQueueModuleMigratesOnEmptyDatabase")
  public void unackedMessageSurvivesWorkerLossOnMigratedSchema() throws Exception {
    PostgresSqlSetupProperties props = pgQueueProps();
    props.getPgQueue().setSchema(ns.getSchema());
    props.getPgQueue().setTablePrefix(ns.getTablePrefix());
    props.getPgQueue().getTopicDefaults().setPriorityBands("[{\"range\":[0,9],\"weight\":100}]");
    props.getPgQueue().getTopicDefaults().setPartitionCount(1);
    QueueTableNames names = QueueTableNames.fromPostgresProperties(props);
    String consumerGroup = "worker-loss-it";
    String topicName = "worker-loss-topic";
    byte[] payload = "metadata-change-log".getBytes(StandardCharsets.UTF_8);
    QueueTopicDefaults defaults = new QueueTopicDefaults(1, 0, 0L, 0L, false, null);
    QueueMessageHandle handle;

    Database firstWorker =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pgqueue_first_worker"));
    try {
      MetadataQueueStore store =
          new EbeanPostgresMetadataQueueStore(firstWorker, props, null, new ObjectMapper());
      handle =
          store.enqueue(
              topicName,
              "urn:li:test:worker-loss",
              defaults,
              5,
              payload,
              Optional.of("application/avro"),
              List.of());
      List<QueueReceivedMessage> received =
          store.receiveBatchForGroup(
              consumerGroup,
              handle.topicId(),
              List.of(handle.partitionId()),
              "first-worker",
              Duration.ofMinutes(5),
              1);
      assertEquals(received.size(), 1);
      assertEquals(received.get(0).handle(), handle);
      assertEquals(
          store.getCommittedOffset(consumerGroup, handle.topicId(), handle.partitionId()), 0L);
    } finally {
      // Drop the worker's connection pool without acknowledging or releasing its lease.
      firstWorker.shutdown();
    }

    Database recoveryWorker =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pgqueue_recovery_worker"));
    try {
      MetadataQueueStore store =
          new EbeanPostgresMetadataQueueStore(recoveryWorker, props, null, new ObjectMapper());
      assertTrue(
          store
              .receiveBatchForGroup(
                  consumerGroup,
                  handle.topicId(),
                  List.of(handle.partitionId()),
                  "recovery-worker",
                  Duration.ofMinutes(5),
                  1)
              .isEmpty(),
          "A replacement worker must not steal an unexpired lease");

      // Expire only this message's persisted lease; avoid a wall-clock timeout in the test.
      try (Connection connection = postgres.createConnection("");
          PreparedStatement statement =
              connection.prepareStatement(
                  "UPDATE "
                      + names.qualifiedMessageGroupLease()
                      + " SET locked_until = NOW() - INTERVAL '1 second'"
                      + " WHERE consumer_group = ? AND message_id = ? AND message_enqueued_at = ?"
                      + " AND lock_owner = ?")) {
        statement.setString(1, consumerGroup);
        statement.setLong(2, handle.id());
        statement.setTimestamp(3, java.sql.Timestamp.from(handle.enqueuedAt()));
        statement.setString(4, "first-worker");
        assertEquals(statement.executeUpdate(), 1);
      }

      List<QueueReceivedMessage> redelivered =
          store.receiveBatchForGroup(
              consumerGroup,
              handle.topicId(),
              List.of(handle.partitionId()),
              "recovery-worker",
              Duration.ofMinutes(5),
              1);
      assertEquals(redelivered.size(), 1);
      assertEquals(redelivered.get(0).handle(), handle);
      assertEquals(redelivered.get(0).payload(), payload);
      assertEquals(redelivered.get(0).contentType(), Optional.of("application/avro"));
      assertEquals(redelivered.get(0).routingKey(), "urn:li:test:worker-loss");
      assertEquals(redelivered.get(0).lockOwner(), "recovery-worker");
      assertEquals(
          store.getCommittedOffset(consumerGroup, handle.topicId(), handle.partitionId()), 0L);

      assertEquals(
          store.commitForGroup(consumerGroup, List.of(redelivered.get(0).handle()), true), 1);
      assertEquals(
          store.getCommittedOffset(consumerGroup, handle.topicId(), handle.partitionId()),
          handle.enqueueSeq());
      assertTrue(
          store
              .receiveBatchForGroup(
                  consumerGroup,
                  handle.topicId(),
                  List.of(handle.partitionId()),
                  "recovery-worker",
                  Duration.ofMinutes(5),
                  1)
              .isEmpty(),
          "An acknowledged message must not be delivered again");
    } finally {
      EbeanTestUtils.shutdownDatabase(recoveryWorker);
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
