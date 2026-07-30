package com.linkedin.metadata.entity.ebean;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.Transaction;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Characterizes the PostgreSQL row-lock behavior of {@link EbeanAspectDao#batchGet} with {@code
 * forUpdate=true}, reproducing the production deadlock seen on the logical-model ingest path
 * ({@code EntityServiceImpl.ingestAspectsToLocalDB}).
 *
 * <p>Findings encoded here:
 *
 * <ul>
 *   <li>Two concurrent <em>single-statement</em> {@code FOR UPDATE} batchGets over overlapping key
 *       sets do NOT deadlock: PostgreSQL plans the composite-key IN query as a (bitmap) scan that
 *       locks rows in physical heap order, which is identical for both statements regardless of
 *       IN-list order. Sorting the IN-list is therefore not a fix on PostgreSQL.
 *   <li>Deadlocks arise when transactions acquire locks in <em>multiple waves</em> (multiple
 *       locking statements per transaction), as the ingest path did before {@code
 *       AspectDao.lockLatestRows}: {@code exists(..., forUpdate=true)} (wave 1) followed by {@code
 *       getLatestAspects(..., forUpdate=true)} (wave 2). Two transactions whose waves overlap
 *       crosswise deadlock deterministically — this was the production failure mode behind
 *       "RetryLimitReached: Failed to add after 3 retries".
 * </ul>
 *
 * <p>The fix: {@code EntityServiceImpl.ingestAspectsToLocalDB} now acquires the batch's full lock
 * set in one up-front {@code lockLatestRows} statement (see {@link
 * EntityServiceConcurrentIngestPostgresIT} for the end-to-end regression test). The cross-wave test
 * below intentionally still demonstrates the deadlock at the DAO level — it documents WHY
 * single-wave acquisition is required and must keep passing (i.e. keep deadlocking) as long as
 * PostgreSQL locking semantics are what they are.
 */
public class EbeanAspectDaoConcurrentLockPostgresIT {

  private static final String KEY_ASPECT = "schemaFieldKey";
  private static final int ROWS = 200;
  private static final int SINGLE_STATEMENT_ITERATIONS = 50;

  private PostgreSQLContainer<?> postgres;
  private Database database;
  private EbeanAspectDao aspectDao;
  private OperationContext opContext;

  /** All seeded key identifiers, in insertion (physical) order. */
  private List<EntityAspectIdentifier> allKeys;

  @BeforeClass
  public void init() {
    postgres = PostgresTestUtils.startPostgres();
    database =
        PostgresTestUtils.createEbeanPrimaryDatabase(
            postgres,
            PostgresTestUtils.uniqueServerName("lock_wave_it"),
            PostgresTestUtils.newIntegrationNamespace("lock_wave").getSchema());
    aspectDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(database),
            EbeanConfiguration.testDefault,
            null,
            List.of(),
            null);
    aspectDao.setConnectionValidated(true);
    opContext = TestOperationContexts.systemContextNoValidate();

    allKeys = new ArrayList<>(ROWS);
    for (int i = 0; i < ROWS; i++) {
      String urn =
          String.format(
              "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mainframe,lock_it,PROD),field_%d)",
              i);
      EbeanAspectV2 row = new EbeanAspectV2();
      row.setUrn(urn);
      row.setAspect(KEY_ASPECT);
      row.setVersion(0L);
      row.setMetadata("{}");
      row.setCreatedBy("urn:li:corpuser:test");
      row.setCreatedOn(new Timestamp(System.currentTimeMillis()));
      database.save(row);
      allKeys.add(new EntityAspectIdentifier(urn, KEY_ASPECT, 0L));
    }
    database.sqlUpdate("ANALYZE metadata_aspect_v2").execute();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(database);
  }

  /**
   * Two concurrent transactions each acquiring all their locks in ONE batchGet statement never
   * deadlock, even over overlapping key sets: PostgreSQL locks rows in physical scan order, which
   * is consistent across both statements. This is the invariant a single-wave locking fix relies
   * on.
   */
  @Test(timeOut = 300_000)
  public void singleStatementForUpdate_overlappingSets_noDeadlock() throws Exception {
    Set<EntityAspectIdentifier> setA = new HashSet<>(allKeys.subList(0, 150));
    Set<EntityAspectIdentifier> setB = new HashSet<>(allKeys.subList(50, 200));

    CyclicBarrier barrier = new CyclicBarrier(2);
    AtomicInteger deadlocks = new AtomicInteger();
    AtomicInteger unexpectedFailures = new AtomicInteger();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> workerA =
          executor.submit(() -> singleStatementLoop(setA, barrier, deadlocks, unexpectedFailures));
      Future<?> workerB =
          executor.submit(() -> singleStatementLoop(setB, barrier, deadlocks, unexpectedFailures));
      workerA.get(120, TimeUnit.SECONDS);
      workerB.get(120, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertEquals(unexpectedFailures.get(), 0, "Non-deadlock failures during concurrent batchGet");
    assertEquals(
        deadlocks.get(), 0, "Single-statement FOR UPDATE batchGets must not deadlock each other");
  }

  /**
   * Reproduces the production deadlock: each transaction locks rows in TWO waves (two batchGet FOR
   * UPDATE statements), and the waves overlap crosswise. Transaction A locks rows [0,100) then
   * wants [100,200); transaction B locks [100,200) then wants [0,100). This mirrors {@code
   * ingestAspectsToLocalDB}: {@code exists()} locks key-aspect rows, then {@code
   * getLatestAspects()} locks batch-aspect rows, so two ingests touching overlapping entities
   * interlock across statements.
   *
   * <p>This test asserts the deadlock DOES occur, characterizing the defect. A fix that collapses
   * lock acquisition into a single wave (or otherwise prevents the interlock) should flip this
   * assertion to zero deadlocks.
   */
  @Test(timeOut = 300_000)
  public void crossWaveForUpdate_overlappingSets_deadlocks() throws Exception {
    Set<EntityAspectIdentifier> firstHalf = new HashSet<>(allKeys.subList(0, 100));
    Set<EntityAspectIdentifier> secondHalf = new HashSet<>(allKeys.subList(100, 200));

    // Both threads hold their wave-1 locks before either starts wave 2.
    CyclicBarrier midTransactionBarrier = new CyclicBarrier(2);
    AtomicInteger deadlocks = new AtomicInteger();
    AtomicInteger commits = new AtomicInteger();
    AtomicInteger unexpectedFailures = new AtomicInteger();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> workerA =
          executor.submit(
              () ->
                  twoWaveTransaction(
                      firstHalf,
                      secondHalf,
                      midTransactionBarrier,
                      deadlocks,
                      commits,
                      unexpectedFailures));
      Future<?> workerB =
          executor.submit(
              () ->
                  twoWaveTransaction(
                      secondHalf,
                      firstHalf,
                      midTransactionBarrier,
                      deadlocks,
                      commits,
                      unexpectedFailures));
      workerA.get(120, TimeUnit.SECONDS);
      workerB.get(120, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertEquals(unexpectedFailures.get(), 0, "Non-deadlock failures during cross-wave locking");
    // PostgreSQL picks exactly one victim; the survivor's second wave proceeds and commits.
    assertEquals(
        deadlocks.get(),
        1,
        "Cross-wave FOR UPDATE lock acquisition should deadlock (production failure mode); "
            + "if this now reports 0, the locking strategy changed — update this test to assert "
            + "the fixed behavior");
    assertEquals(commits.get(), 1, "The non-victim transaction should commit successfully");
  }

  private void singleStatementLoop(
      Set<EntityAspectIdentifier> keys,
      CyclicBarrier barrier,
      AtomicInteger deadlocks,
      AtomicInteger unexpectedFailures) {
    for (int i = 0; i < SINGLE_STATEMENT_ITERATIONS; i++) {
      try {
        barrier.await(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        return;
      }
      try (Transaction transaction = database.beginTransaction()) {
        forceIndexPlan();
        aspectDao.batchGet(opContext, keys, true);
        transaction.commit();
      } catch (Exception e) {
        if (isDeadlock(e)) {
          deadlocks.incrementAndGet();
        } else {
          unexpectedFailures.incrementAndGet();
        }
      }
    }
  }

  private void twoWaveTransaction(
      Set<EntityAspectIdentifier> waveOne,
      Set<EntityAspectIdentifier> waveTwo,
      CyclicBarrier midTransactionBarrier,
      AtomicInteger deadlocks,
      AtomicInteger commits,
      AtomicInteger unexpectedFailures) {
    try (Transaction transaction = database.beginTransaction()) {
      forceIndexPlan();
      aspectDao.batchGet(opContext, waveOne, true);
      try {
        midTransactionBarrier.await(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        unexpectedFailures.incrementAndGet();
        return;
      }
      aspectDao.batchGet(opContext, waveTwo, true);
      transaction.commit();
      commits.incrementAndGet();
    } catch (Exception e) {
      if (isDeadlock(e)) {
        deadlocks.incrementAndGet();
      } else {
        unexpectedFailures.incrementAndGet();
      }
    }
  }

  /**
   * The test table is tiny, so the planner would seq-scan. Production metadata_aspect_v2 is large
   * and plans these queries as index/bitmap scans; force the comparable plan shape. (Lock order is
   * physical heap order either way, but keep the plan honest.)
   */
  private void forceIndexPlan() {
    database.sqlUpdate("SET LOCAL enable_seqscan = off").execute();
  }

  private static boolean isDeadlock(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      String message = cause.getMessage();
      if (message != null && message.contains("deadlock detected")) {
        return true;
      }
    }
    return false;
  }
}
