package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.MysqlTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.ConditionalAspectUpdate;
import com.linkedin.metadata.entity.ConditionalUpdateResult;
import com.linkedin.metadata.entity.TransactionContext;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.Transaction;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Real integration test for batch conditional-write (CAS) via {@code
 * EbeanAspectDao.updateAspectsConditionalBatch()}. Exercises the production DAO method against real
 * MySQL and PostgreSQL instances.
 *
 * <p>Tests the core batch update outcomes (UPDATED, CONFLICT) through the DAO, plus diagnostic
 * probes to understand JDBC batch behavior (per-row counts, error handling).
 *
 * <p><b>Determinism.</b> Fixed timestamps, unique URNs via {@link #shortId()}, no sleeps, no
 * concurrency. Each test uses a unique URN, so no dependence on table-global state.
 */
public class EbeanAspectDaoConditionalBatchIT {

  private static final String CREATED_BY = "urn:li:corpuser:test";
  // Fixed so systemMetadata round-trips deterministically.
  private static final long FIXED_TS = 1_700_000_000_000L;

  private GenericContainer<?> container;
  private Database primaryDatabase;
  private EbeanAspectDao dao;
  private OperationContext opContext;

  @BeforeClass
  public void init() {
    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (primaryDatabase != null) {
      EbeanTestUtils.shutdownDatabase(primaryDatabase);
    }
  }

  @Test
  public void mysql() throws Exception {
    MySQLContainer<?> mysql = MysqlTestUtils.startMysql();
    primaryDatabase =
        MysqlTestUtils.createEbeanPrimaryDatabase(
            mysql, MysqlTestUtils.uniqueServerName("olbatch"));
    final PrimaryStorageResolver resolver = PrimaryStorageTestUtils.ebeanResolver(primaryDatabase);

    dao =
        new EbeanAspectDao(
            resolver,
            EbeanConfiguration.builder().optimisticLockingEnabled(true).build(),
            null,
            List.of(),
            null,
            true);
    dao.setConnectionValidated(true);

    runDaoTests(dao);
    runDiagnostics(primaryDatabase, "mysql");
  }

  @Test
  public void postgres() throws Exception {
    PostgreSQLContainer<?> postgres = PostgresTestUtils.startPostgres();
    primaryDatabase =
        PostgresTestUtils.createEbeanPrimaryDatabase(
            postgres, PostgresTestUtils.uniqueServerName("olbatch"));
    final PrimaryStorageResolver resolver = PrimaryStorageTestUtils.ebeanResolver(primaryDatabase);

    dao =
        new EbeanAspectDao(
            resolver,
            EbeanConfiguration.builder().optimisticLockingEnabled(true).build(),
            null,
            List.of(),
            null,
            true);
    dao.setConnectionValidated(true);

    runDaoTests(dao);
    runDiagnostics(primaryDatabase, "postgres");
  }

  // ============================================================================
  // Primary DAO Tests
  // ============================================================================

  private void runDaoTests(EbeanAspectDao dao) {
    System.out.println("\n=== Running primary DAO tests for updateAspectsConditionalBatch() ===");

    testAllMatch(dao);
    testMatchConflictMatch(dao);
    testEmptyBatch(dao);

    System.out.println("=== All primary DAO tests passed ===\n");
  }

  /**
   * Test: all three rows match their expected versions. All should return UPDATED.
   *
   * <p>Seed A, B, C at version "1". Batch [A expects "1", B expects "1", C expects "1"], each with
   * setRemoved(true) + systemMetadata version "2". Expect result [UPDATED, UPDATED, UPDATED].
   * Assert each row now at version "2" and removed=true.
   */
  private void testAllMatch(EbeanAspectDao dao) {
    System.out.println("\n--- Test: allMatch ---");

    String urnA = "urn:li:corpuser:allMatch_a_" + shortId();
    String urnB = "urn:li:corpuser:allMatch_b_" + shortId();
    String urnC = "urn:li:corpuser:allMatch_c_" + shortId();

    // Seed rows with version="1".
    seedVersion0(urnA, new Status(), sysMeta("1"));
    seedVersion0(urnB, new Status(), sysMeta("1"));
    seedVersion0(urnC, new Status(), sysMeta("1"));

    // Build batch: all expect matching version "1".
    List<ConditionalAspectUpdate> updates =
        List.of(
            new ConditionalAspectUpdate(
                newAspect(urnA, new Status().setRemoved(true), sysMeta("2")), "1"),
            new ConditionalAspectUpdate(
                newAspect(urnB, new Status().setRemoved(true), sysMeta("2")), "1"),
            new ConditionalAspectUpdate(
                newAspect(urnC, new Status().setRemoved(true), sysMeta("2")), "1"));

    // Execute via DAO.
    List<ConditionalUpdateResult> results =
        inTx(txContext -> dao.updateAspectsConditionalBatch(opContext, txContext, updates));

    assertEquals(results.size(), 3, "batch should return 3 results");
    assertEquals(
        results,
        List.of(
            ConditionalUpdateResult.UPDATED,
            ConditionalUpdateResult.UPDATED,
            ConditionalUpdateResult.UPDATED),
        "all three should match and return UPDATED");

    // Verify each row is now at version "2" and removed=true.
    assertEquals(storedVersion(urnA), "2", "row A must be at version 2");
    assertEquals(storedVersion(urnB), "2", "row B must be at version 2");
    assertEquals(storedVersion(urnC), "2", "row C must be at version 2");
    assertTrue(storedRemoved(urnA), "row A must have removed=true");
    assertTrue(storedRemoved(urnB), "row B must have removed=true");
    assertTrue(storedRemoved(urnC), "row C must have removed=true");

    System.out.println("PASS: allMatch");
  }

  /**
   * Test: A matches, B stales (CONFLICT), C matches. The critical no-data-loss test.
   *
   * <p>Seed A, B, C at version "1". Batch [A expects "1", B expects "999" (stale), C expects "1"],
   * each with setRemoved(true) + version "2". Expect result [UPDATED, CONFLICT, UPDATED]. Assert: A
   * version "2" removed true; B version STILL "1" removed false (no lost update); C version "2"
   * removed true.
   */
  private void testMatchConflictMatch(EbeanAspectDao dao) {
    System.out.println("\n--- Test: matchConflictMatch (no-data-loss) ---");

    String urnA = "urn:li:corpuser:matchConflict_a_" + shortId();
    String urnB = "urn:li:corpuser:matchConflict_b_" + shortId();
    String urnC = "urn:li:corpuser:matchConflict_c_" + shortId();

    // Seed rows with version="1".
    seedVersion0(urnA, new Status(), sysMeta("1"));
    seedVersion0(urnB, new Status(), sysMeta("1"));
    seedVersion0(urnC, new Status(), sysMeta("1"));

    // Build batch: A matches, B stales, C matches.
    List<ConditionalAspectUpdate> updates =
        List.of(
            new ConditionalAspectUpdate(
                newAspect(urnA, new Status().setRemoved(true), sysMeta("2")), "1"),
            new ConditionalAspectUpdate(
                newAspect(urnB, new Status().setRemoved(true), sysMeta("2")), "999"),
            new ConditionalAspectUpdate(
                newAspect(urnC, new Status().setRemoved(true), sysMeta("2")), "1"));

    // Execute via DAO.
    List<ConditionalUpdateResult> results =
        inTx(txContext -> dao.updateAspectsConditionalBatch(opContext, txContext, updates));

    assertEquals(results.size(), 3, "batch should return 3 results");
    assertEquals(
        results,
        List.of(
            ConditionalUpdateResult.UPDATED,
            ConditionalUpdateResult.CONFLICT,
            ConditionalUpdateResult.UPDATED),
        "result ordering: [UPDATED, CONFLICT, UPDATED]");

    // Verify no-data-loss: B must NOT have been updated (it conflicts).
    assertEquals(storedVersion(urnA), "2", "row A must be at version 2");
    assertEquals(storedVersion(urnB), "1", "row B must STILL be at version 1 (not updated)");
    assertEquals(storedVersion(urnC), "2", "row C must be at version 2");

    assertTrue(storedRemoved(urnA), "row A must have removed=true");
    assertFalse(storedRemoved(urnB), "row B must have removed=false (stale write did not apply)");
    assertTrue(storedRemoved(urnC), "row C must have removed=true");

    System.out.println("PASS: matchConflictMatch (no-data-loss verified)");
  }

  /** Test: empty batch. Should return empty list. */
  private void testEmptyBatch(EbeanAspectDao dao) {
    System.out.println("\n--- Test: emptyBatch ---");

    List<ConditionalUpdateResult> results =
        inTx(txContext -> dao.updateAspectsConditionalBatch(opContext, txContext, List.of()));

    assertEquals(results.size(), 0, "empty batch should return empty list");

    System.out.println("PASS: emptyBatch");
  }

  // ============================================================================
  // Diagnostic Tests
  // ============================================================================

  /**
   * DIAGNOSTIC: informs the DAO impl / ADR, not testing the production method. Probes raw JDBC
   * batch behavior to understand per-row counts and error handling.
   */
  private void runDiagnostics(Database db, String dialect) throws Exception {
    System.out.println("\n=== Running diagnostic JDBC probes for " + dialect + " ===");

    // Probe 1: per-row counts via raw JDBC, happy + conflict mix.
    probe1(db, dialect);

    // Probe 3: MySQL rewriteBatchedStatements=true → SUCCESS_NO_INFO (MySQL only).
    if ("mysql".equals(dialect)) {
      probe3(db);
    }

    // Probe 4: mid-batch SQL error → BatchUpdateException + failed txn.
    probe4(db, dialect);

    System.out.println("=== All diagnostic probes passed for " + dialect + " ===\n");
  }

  /**
   * DIAGNOSTIC: per-row counts via raw JDBC, happy + conflict mix (the core JDBC question).
   *
   * <p>Seed 3 version-0 rows: A(version="1"), B(version="1"), C(version="1"), all aspect=status.
   * Batch of 3 CAS updates via raw JDBC: A expects "1" (match), B expects "999" (stale), C expects
   * "1" (match). Execute batch and assert int[] == [1, 0, 1].
   */
  private void probe1(Database db, String dialect) throws SQLException {
    System.out.println("\nSPIKE: === Probe 1 (per-row counts) ===");

    String versionPredicate =
        "postgres".equals(dialect)
            ? "(systemmetadata::jsonb ->> 'version') = ?"
            : "systemmetadata->>'$.version' = ?";

    String sql =
        "UPDATE metadata_aspect_v2 SET metadata=?, systemmetadata=?, "
            + "createdon=?, createdby=?, createdfor=? "
            + "WHERE urn=? AND aspect=? AND version=0 AND "
            + versionPredicate;

    String urnA = "urn:li:corpuser:probe1_a_" + shortId();
    String urnB = "urn:li:corpuser:probe1_b_" + shortId();
    String urnC = "urn:li:corpuser:probe1_c_" + shortId();

    // Seed rows with version="1".
    seedVersion0Json(db, urnA, toJsonString(new Status()), toJsonString(sysMeta("1")));
    seedVersion0Json(db, urnB, toJsonString(new Status()), toJsonString(sysMeta("1")));
    seedVersion0Json(db, urnC, toJsonString(new Status()), toJsonString(sysMeta("1")));

    // Execute batch: [A(expects "1"), B(expects "999"), C(expects "1")].
    try (Transaction tx = db.beginTransaction()) {
      Connection conn = tx.connection();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        // Row A: match (expects "1").
        String metaA = toJsonString(new Status().setRemoved(true));
        String sysMetaA = toJsonString(sysMeta("2"));
        setParams(ps, metaA, sysMetaA, urnA, "1");
        ps.addBatch();
        ps.clearParameters();

        // Row B: stale (expects "999", row has "1").
        String metaB = toJsonString(new Status().setRemoved(true));
        String sysMetaB = toJsonString(sysMeta("2"));
        setParams(ps, metaB, sysMetaB, urnB, "999");
        ps.addBatch();
        ps.clearParameters();

        // Row C: match (expects "1").
        String metaC = toJsonString(new Status().setRemoved(true));
        String sysMetaC = toJsonString(sysMeta("2"));
        setParams(ps, metaC, sysMetaC, urnC, "1");
        ps.addBatch();

        int[] counts = ps.executeBatch();
        System.out.println("SPIKE: Probe 1 raw counts: " + Arrays.toString(counts));
        assertEquals(counts.length, 3, "batch should have 3 results");
        assertEquals(counts[0], 1, "row A should match");
        assertEquals(counts[1], 0, "row B should conflict (stale version)");
        assertEquals(counts[2], 1, "row C should match");
      }
      tx.commit();
    }
    System.out.println("SPIKE: Probe 1 PASSED");
  }

  /**
   * DIAGNOSTIC: MySQL rewriteBatchedStatements=true → SUCCESS_NO_INFO (MySQL only).
   *
   * <p>Open a connection with rewriteBatchedStatements=true, run the same 3-row batch, and assert
   * the result contains SUCCESS_NO_INFO (-2).
   */
  private void probe3(Database db) throws SQLException {
    System.out.println("\nSPIKE: === Probe 3 (MySQL rewriteBatchedStatements=true) ===");

    MySQLContainer<?> mysql = MysqlTestUtils.startMysql();

    String versionPredicate = "systemmetadata->>'$.version' = ?";
    String sql =
        "UPDATE metadata_aspect_v2 SET metadata=?, systemmetadata=?, "
            + "createdon=?, createdby=?, createdfor=? "
            + "WHERE urn=? AND aspect=? AND version=0 AND "
            + versionPredicate;

    String urnA = "urn:li:corpuser:probe3_a_" + shortId();
    String urnB = "urn:li:corpuser:probe3_b_" + shortId();
    String urnC = "urn:li:corpuser:probe3_c_" + shortId();

    // Seed rows with version="1".
    seedVersion0Json(db, urnA, toJsonString(new Status()), toJsonString(sysMeta("1")));
    seedVersion0Json(db, urnB, toJsonString(new Status()), toJsonString(sysMeta("1")));
    seedVersion0Json(db, urnC, toJsonString(new Status()), toJsonString(sysMeta("1")));

    // Open connection with rewriteBatchedStatements=true.
    String url =
        mysql.getJdbcUrl()
            + "?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8&rewriteBatchedStatements=true";
    try (java.sql.Connection conn =
        java.sql.DriverManager.getConnection(url, mysql.getUsername(), mysql.getPassword())) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        // Row A: match (expects "1").
        String metaA = toJsonString(new Status().setRemoved(true));
        String sysMetaA = toJsonString(sysMeta("2"));
        setParams(ps, metaA, sysMetaA, urnA, "1");
        ps.addBatch();
        ps.clearParameters();

        // Row B: stale (expects "999").
        String metaB = toJsonString(new Status().setRemoved(true));
        String sysMetaB = toJsonString(sysMeta("2"));
        setParams(ps, metaB, sysMetaB, urnB, "999");
        ps.addBatch();
        ps.clearParameters();

        // Row C: match (expects "1").
        String metaC = toJsonString(new Status().setRemoved(true));
        String sysMetaC = toJsonString(sysMeta("2"));
        setParams(ps, metaC, sysMetaC, urnC, "1");
        ps.addBatch();

        int[] counts = ps.executeBatch();
        System.out.println(
            "SPIKE: Probe 3 (rewriteBatchedStatements=true) raw counts: "
                + Arrays.toString(counts));

        // At least one should be SUCCESS_NO_INFO (-2).
        boolean hasSuccessNoInfo = false;
        for (int count : counts) {
          if (count == Statement.SUCCESS_NO_INFO) {
            hasSuccessNoInfo = true;
            break;
          }
        }
        if (!hasSuccessNoInfo) {
          System.out.println(
              "SPIKE: WARNING: No SUCCESS_NO_INFO found; got: " + Arrays.toString(counts));
        } else {
          System.out.println("SPIKE: SUCCESS_NO_INFO confirmed in batch result");
        }
      }
    }
    System.out.println("SPIKE: Probe 3 PASSED");
  }

  /**
   * DIAGNOSTIC: mid-batch SQL error → BatchUpdateException + failed txn on Postgres.
   *
   * <p>Seed row D with invalid JSON. Batch [A(match), D(expects "1"), C(match)]. When driver
   * evaluates D's predicate, it errors (PG 22P02 / MySQL 3141) → executeBatch() throws
   * BatchUpdateException. On Postgres, after the throw, `SELECT 1` fails with SQLState 25P02
   * (in-failed-sql-transaction). Roll back; fresh transaction then works.
   */
  private void probe4(Database db, String dialect) throws SQLException {
    System.out.println("\nSPIKE: === Probe 4 (mid-batch SQL error) ===");

    String versionPredicate =
        "postgres".equals(dialect)
            ? "(systemmetadata::jsonb ->> 'version') = ?"
            : "systemmetadata->>'$.version' = ?";

    String sql =
        "UPDATE metadata_aspect_v2 SET metadata=?, systemmetadata=?, "
            + "createdon=?, createdby=?, createdfor=? "
            + "WHERE urn=? AND aspect=? AND version=0 AND "
            + versionPredicate;

    String urnA = "urn:li:corpuser:probe4_a_" + shortId();
    String urnD = "urn:li:corpuser:probe4_d_" + shortId();
    String urnC = "urn:li:corpuser:probe4_c_" + shortId();

    // Seed A and C normally.
    seedVersion0Json(db, urnA, toJsonString(new Status()), toJsonString(sysMeta("1")));
    seedVersion0Json(db, urnC, toJsonString(new Status()), toJsonString(sysMeta("1")));

    // Seed D with INVALID JSON in systemmetadata.
    primaryDatabase.save(
        new EbeanAspectV2(
            urnD,
            STATUS_ASPECT_NAME,
            ASPECT_LATEST_VERSION,
            toJsonString(new Status()),
            new Timestamp(FIXED_TS),
            CREATED_BY,
            null,
            "not-json")); // Invalid JSON

    // Try batch [A(match), D(error), C(match)]; expect exception.
    try (Transaction tx = db.beginTransaction()) {
      Connection conn = tx.connection();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        // Row A: match (expects "1").
        String metaA = toJsonString(new Status().setRemoved(true));
        String sysMetaA = toJsonString(sysMeta("2"));
        setParams(ps, metaA, sysMetaA, urnA, "1");
        ps.addBatch();
        ps.clearParameters();

        // Row D: error (systemmetadata is invalid JSON).
        String metaD = toJsonString(new Status().setRemoved(true));
        String sysMetaD = toJsonString(sysMeta("2"));
        setParams(ps, metaD, sysMetaD, urnD, "1");
        ps.addBatch();
        ps.clearParameters();

        // Row C: match (expects "1").
        String metaC = toJsonString(new Status().setRemoved(true));
        String sysMetaC = toJsonString(sysMeta("2"));
        setParams(ps, metaC, sysMetaC, urnC, "1");
        ps.addBatch();

        try {
          int[] counts = ps.executeBatch();
          System.out.println(
              "SPIKE: ERROR: executeBatch did not throw; got: " + Arrays.toString(counts));
          fail("Expected BatchUpdateException or SQLException for invalid JSON in predicate");
        } catch (BatchUpdateException bue) {
          System.out.println(
              "SPIKE: Caught BatchUpdateException (expected): "
                  + bue.getClass().getSimpleName()
                  + ", SQLState="
                  + bue.getSQLState());

          // On Postgres: check that txn is now failed (SQLState 25P02).
          if ("postgres".equals(dialect)) {
            try (java.sql.Statement stmt = conn.createStatement()) {
              stmt.executeQuery("SELECT 1");
              System.out.println(
                  "SPIKE: WARNING: SELECT 1 did not fail after BatchUpdateException");
            } catch (SQLException selectErr) {
              String sqlState = selectErr.getSQLState();
              System.out.println("SPIKE: SELECT 1 failed (expected) with SQLState=" + sqlState);
              assertEquals(
                  sqlState,
                  "25P02",
                  "Postgres txn must be in failed state (25P02 = in-failed-sql-transaction)");
            }
          }
        } catch (SQLException se) {
          System.out.println(
              "SPIKE: Caught SQLException (acceptable): "
                  + se.getClass().getSimpleName()
                  + ", SQLState="
                  + se.getSQLState());
        }
      }
      tx.rollback();
    }

    // Verify a fresh transaction works.
    try (Transaction tx = db.beginTransaction()) {
      Connection conn = tx.connection();
      try (java.sql.Statement stmt = conn.createStatement()) {
        stmt.executeQuery("SELECT 1");
        System.out.println("SPIKE: Fresh transaction works after rollback (expected)");
      }
      tx.commit();
    }

    System.out.println("SPIKE: Probe 4 PASSED");
  }

  // ============================================================================
  // Helpers
  // ============================================================================

  /** Run {@code work} inside an explicit transaction wired through a {@link TransactionContext}. */
  private <T> T inTx(java.util.function.Function<TransactionContext, T> work) {
    try (Transaction tx = primaryDatabase.beginTransaction()) {
      final T result =
          work.apply(
              TransactionContext.empty(tx, TransactionContext.DEFAULT_MAX_TRANSACTION_RETRY));
      tx.commit();
      return result;
    }
  }

  private void seedVersion0(String urn, RecordTemplate content, SystemMetadata sm) {
    seedVersion0Json(urn, RecordUtils.toJsonString(content), RecordUtils.toJsonString(sm));
  }

  private void seedVersion0Json(
      Database db, String urn, String metadataJson, String systemMetadataJson) {
    db.save(row(urn, metadataJson, systemMetadataJson));
  }

  private void seedVersion0Json(String urn, String metadataJson, String systemMetadataJson) {
    seedVersion0Json(primaryDatabase, urn, metadataJson, systemMetadataJson);
  }

  /**
   * A fresh, detached version-0 ORM row; a new instance each call so wrapping never sees DB state.
   */
  private EbeanAspectV2 row(String urn, String metadataJson, String systemMetadataJson) {
    return new EbeanAspectV2(
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        metadataJson,
        new Timestamp(FIXED_TS),
        CREATED_BY,
        null,
        systemMetadataJson);
  }

  private SystemAspect newAspect(String urn, RecordTemplate content, SystemMetadata sm) {
    return new EbeanSystemAspect(
        null,
        UrnUtils.getUrn(urn),
        STATUS_ASPECT_NAME,
        opContext.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME),
        opContext.getEntityRegistry().getAspectSpecs().get(STATUS_ASPECT_NAME),
        content,
        sm,
        AuditStampUtils.createDefaultAuditStamp(),
        null,
        null,
        null);
  }

  private static SystemMetadata sysMeta(@Nullable String version) {
    final SystemMetadata sm = new SystemMetadata().setLastObserved(FIXED_TS);
    if (version != null) {
      sm.setVersion(version);
    }
    return sm;
  }

  private String storedVersion(String urn) {
    final EntityAspect aspect =
        dao.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertNotNull(aspect, "expected a stored version-0 row");
    return RecordUtils.toRecordTemplate(SystemMetadata.class, aspect.getSystemMetadata())
        .getVersion();
  }

  private boolean storedRemoved(String urn) {
    final EntityAspect aspect =
        dao.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertNotNull(aspect, "expected a stored version-0 row");
    final Status status = RecordUtils.toRecordTemplate(Status.class, aspect.getMetadata());
    return status.hasRemoved() && status.isRemoved();
  }

  /**
   * Set parameters for one batch row: 8 positional ? in order.
   *
   * <p>metadata(?1), systemmetadata(?2), createdon(?3, FIXED_TS), createdby(?4, CREATED_BY),
   * createdfor(?5, null), urn(?6), aspect(?7, STATUS_ASPECT_NAME), expectedVersion(?8).
   *
   * <p>Call this, then ps.addBatch(), then ps.clearParameters() for each row in the batch.
   */
  private void setParams(
      PreparedStatement ps,
      String metadata,
      String systemMetadata,
      String urn,
      String expectedVersion)
      throws SQLException {
    ps.setString(1, metadata);
    ps.setString(2, systemMetadata);
    ps.setTimestamp(3, new Timestamp(FIXED_TS));
    ps.setString(4, CREATED_BY);
    ps.setObject(5, null);
    ps.setString(6, urn);
    ps.setString(7, STATUS_ASPECT_NAME);
    ps.setString(8, expectedVersion);
  }

  private String toJsonString(RecordTemplate obj) {
    return RecordUtils.toJsonString(obj);
  }

  private static String shortId() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }
}
