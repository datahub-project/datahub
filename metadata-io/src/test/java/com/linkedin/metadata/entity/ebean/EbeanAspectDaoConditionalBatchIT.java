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
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
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
 * <p>Covers the batch outcomes through the DAO (all-match, mixed match/conflict with no lost
 * update, empty batch, thrown mid-batch SQL error) plus the rewriteBatchedStatements runtime latch.
 *
 * <p><b>Determinism.</b> Fixed timestamps, unique URNs via {@link #shortId()}, no sleeps, no
 * concurrency. Each test uses a unique URN, so no dependence on table-global state.
 */
public class EbeanAspectDaoConditionalBatchIT {

  private static final String CREATED_BY = "urn:li:corpuser:test";
  // Fixed so systemMetadata round-trips deterministically.
  private static final long FIXED_TS = 1_700_000_000_000L;

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
  }

  // ============================================================================
  // Primary DAO Tests
  // ============================================================================

  private void runDaoTests(EbeanAspectDao dao) {
    testAllMatch(dao);
    testMatchConflictMatch(dao);
    testEmptyBatch(dao);
    testMidBatchSqlErrorThrows(dao);
  }

  /**
   * Test: a stored row whose systemmetadata is invalid JSON makes the CAS version predicate error
   * at execution, so executeBatch throws. The DAO must wrap it as a PersistenceException (never a
   * partial / silent commit) so the outer runInTransactionWithRetry rolls back. Covers the thrown-
   * batch-error path on both dialects (MySQL ER_INVALID_JSON_TEXT / Postgres invalid jsonb).
   */
  private void testMidBatchSqlErrorThrows(EbeanAspectDao dao) {
    String urnD = "urn:li:corpuser:sqlError_d_" + shortId();
    // Seed a version-0 row whose systemmetadata is NOT valid JSON.
    seedVersion0Json(primaryDatabase, urnD, RecordUtils.toJsonString(new Status()), "not-json");

    List<ConditionalAspectUpdate> updates =
        List.of(
            new ConditionalAspectUpdate(
                newAspect(urnD, new Status().setRemoved(true), sysMeta("2")), "1"));

    try {
      inTx(txContext -> dao.updateAspectsConditionalBatch(opContext, txContext, updates));
      fail("expected PersistenceException from the mid-batch SQL error (invalid JSON predicate)");
    } catch (jakarta.persistence.PersistenceException expected) {
      // DAO wrapped the thrown BatchUpdateException; the outer transaction rolls back.
    }
  }

  /**
   * Test: all three rows match their expected versions. All should return UPDATED.
   *
   * <p>Seed A, B, C at version "1". Batch [A expects "1", B expects "1", C expects "1"], each with
   * setRemoved(true) + systemMetadata version "2". Expect result [UPDATED, UPDATED, UPDATED].
   * Assert each row now at version "2" and removed=true.
   */
  private void testAllMatch(EbeanAspectDao dao) {
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
  }

  /** Test: empty batch. Should return empty list. */
  private void testEmptyBatch(EbeanAspectDao dao) {
    List<ConditionalUpdateResult> results =
        inTx(txContext -> dao.updateAspectsConditionalBatch(opContext, txContext, List.of()));

    assertEquals(results.size(), 0, "empty batch should return empty list");
  }

  /**
   * Test: rewriteBatchedStatements=true latch. After a SUCCESS_NO_INFO (-2) result,
   * dao.isOptimisticWriteBatchEnabled() must flip to false, latching the DAO into sequential mode.
   *
   * <p>Mechanism: Open MySQL with rewriteBatchedStatements=true, which causes the JDBC driver to
   * rewrite batch updates into a single multi-row statement. When a conflict occurs, the driver
   * returns Statement.SUCCESS_NO_INFO (-2), signaling that per-row counts are unavailable. This
   * forces the DAO to latch and fall back to sequential writes.
   */
  @Test
  public void rewriteBatchedStatementsLatchesBatchingOff() throws Exception {
    MySQLContainer<?> mysql = MysqlTestUtils.startMysql();

    // Build an Ebean Database whose JDBC URL enables rewriteBatchedStatements — mirrors
    // MysqlTestUtils.createEbeanPrimaryDatabase but with the rewrite flag, so tx.connection()
    // returns SUCCESS_NO_INFO (-2) for a batched UPDATE and the DAO latches batching off.
    io.ebean.datasource.DataSourceConfig dsc = new io.ebean.datasource.DataSourceConfig();
    dsc.setUrl(
        mysql.getJdbcUrl()
            + "?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8"
            + "&rewriteBatchedStatements=true");
    dsc.setUsername(mysql.getUsername());
    dsc.setPassword(mysql.getPassword());
    dsc.setDriver("com.mysql.cj.jdbc.Driver");
    io.ebean.config.DatabaseConfig cfg = new io.ebean.config.DatabaseConfig();
    cfg.setName(MysqlTestUtils.uniqueServerName("olbatch_latch"));
    cfg.setDataSourceConfig(dsc);
    cfg.setDefaultServer(false);
    cfg.setDdlGenerate(true);
    cfg.setDdlRun(true);
    cfg.addPackage("com.linkedin.metadata.entity.ebean");
    Database rewriteDb = io.ebean.DatabaseFactory.create(cfg);

    final PrimaryStorageResolver resolver = PrimaryStorageTestUtils.ebeanResolver(rewriteDb);

    // Build DAO with OL + scoped retry + batch all enabled.
    EbeanAspectDao latchDao =
        new EbeanAspectDao(
            resolver,
            EbeanConfiguration.builder()
                .optimisticLockingEnabled(true)
                .scopedRetryEnabled(true)
                .optimisticWriteBatchEnabled(true)
                .optimisticWriteBatchMinSize(1)
                .build(),
            null,
            List.of(),
            null,
            true);
    latchDao.setConnectionValidated(true);

    // Verify batching is enabled before the latch event.
    assertTrue(latchDao.isOptimisticWriteBatchEnabled(), "batching must be enabled initially");

    // Seed 2 version-0 rows INTO rewriteDb — the DAO runs against this rewrite-enabled connection.
    String urnA = "urn:li:corpuser:latch_a_" + shortId();
    String urnB = "urn:li:corpuser:latch_b_" + shortId();
    seedVersion0Json(
        rewriteDb,
        urnA,
        RecordUtils.toJsonString(new Status()),
        RecordUtils.toJsonString(sysMeta("1")));
    seedVersion0Json(
        rewriteDb,
        urnB,
        RecordUtils.toJsonString(new Status()),
        RecordUtils.toJsonString(sysMeta("1")));

    List<ConditionalAspectUpdate> updates =
        List.of(
            new ConditionalAspectUpdate(
                newAspect(urnA, new Status().setRemoved(true), sysMeta("2")), "1"),
            new ConditionalAspectUpdate(
                newAspect(urnB, new Status().setRemoved(true), sysMeta("2")), "999"));

    // Run the batch on a rewriteDb transaction so tx.connection() carries rewriteBatchedStatements
    // -> executeBatch returns SUCCESS_NO_INFO (-2) -> the DAO latches batching off. The throw is
    // the
    // expected ambiguous-result path (rolls back for sequential retry); swallow it, then assert the
    // latch flipped.
    try (Transaction tx = rewriteDb.beginTransaction()) {
      latchDao.updateAspectsConditionalBatch(
          opContext,
          TransactionContext.empty(tx, TransactionContext.DEFAULT_MAX_TRANSACTION_RETRY),
          updates);
      tx.commit();
    } catch (jakarta.persistence.PersistenceException expected) {
      // -2 ambiguous path.
    } finally {
      EbeanTestUtils.shutdownDatabase(rewriteDb);
    }

    assertFalse(
        latchDao.isOptimisticWriteBatchEnabled(),
        "batching must be latched OFF after a SUCCESS_NO_INFO (-2) result");
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

  private String toJsonString(RecordTemplate obj) {
    return RecordUtils.toJsonString(obj);
  }

  private static String shortId() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }
}
