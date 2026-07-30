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
 * Guards the PostgreSQL locking invariant that single-wave lock acquisition ({@code
 * AspectDao.lockLatestRows}) relies on: two concurrent <em>single-statement</em> {@code FOR UPDATE}
 * batchGets over overlapping key sets cannot deadlock, because PostgreSQL acquires multi-row locks
 * in physical scan order, which is consistent across both statements regardless of IN-list order.
 *
 * <p>Deadlocks arise only when a transaction acquires locks in multiple statements ("waves"), as
 * the ingest path did before {@code lockLatestRows}: {@code exists(..., forUpdate=true)} followed
 * by {@code getLatestAspects(..., forUpdate=true)}. Two transactions whose waves overlap crosswise
 * interlock — the production failure mode behind "RetryLimitReached: Failed to add after 3
 * retries". See {@link EntityServiceConcurrentIngestPostgresIT} for the end-to-end regression test
 * of the fixed ingest path.
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
