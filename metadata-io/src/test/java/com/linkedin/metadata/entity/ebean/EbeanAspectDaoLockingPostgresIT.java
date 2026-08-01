package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.Transaction;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testcontainers integration test for the PostgreSQL write/delete lock-ordering changes that guard
 * against lock-order deadlocks between a multi-row {@code FOR UPDATE} write (e.g. logical-model
 * linking) and a concurrent hard-delete on overlapping rows.
 *
 * <p>Two things are verified against a real PostgreSQL instance:
 *
 * <ul>
 *   <li>the opt-in {@code pg_advisory_xact_lock} serialization (holds within a transaction, blocks
 *       a concurrent writer on the same urn, releases on commit; no-op when disabled), and
 *   <li>the ordered {@code FOR UPDATE} SQL added to {@code deleteUrn} and {@code batchGetIn}
 *       actually parses and runs on PostgreSQL (the {@code orderBy(...)} property path and the
 *       {@code ORDER BY ... FOR UPDATE} raw SQL).
 * </ul>
 *
 * <p>We deliberately do not attempt to reproduce a nondeterministic deadlock; that is flaky and
 * characterizes the engine rather than this code. The advisory-serialization test is the
 * deterministic proof that the serialization mechanism works.
 */
public class EbeanAspectDaoLockingPostgresIT {

  private static final String CREATED_BY = "urn:li:corpuser:test";

  private PostgreSQLContainer<?> postgres;
  private Database primaryDatabase;
  private EbeanAspectDao advisoryDao;
  private EbeanAspectDao defaultDao;
  private OperationContext opContext;

  @BeforeClass
  public void init() {
    postgres = PostgresTestUtils.startPostgres();
    primaryDatabase =
        PostgresTestUtils.createEbeanPrimaryDatabase(
            postgres, PostgresTestUtils.uniqueServerName("locking_it"));

    advisoryDao =
        new EbeanAspectDao(
            primaryDatabase,
            EbeanConfiguration.builder().entityWriteAdvisoryLockEnabled(true).build(),
            null,
            List.of(),
            null);
    advisoryDao.setConnectionValidated(true);

    defaultDao =
        new EbeanAspectDao(primaryDatabase, EbeanConfiguration.testDefault, null, List.of(), null);
    defaultDao.setConnectionValidated(true);

    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(primaryDatabase);
  }

  @Test
  public void advisoryLock_serializesConcurrentWritersOnSameUrn() throws Exception {
    final String urn = "urn:li:corpuser:adv_serialize_" + shortId();
    final CountDownLatch aHoldsLock = new CountDownLatch(1);
    final CountDownLatch releaseA = new CountDownLatch(1);
    final AtomicLong bAcquiredAtNanos = new AtomicLong(0);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread a =
        new Thread(
            () -> {
              try (Transaction tx = primaryDatabase.beginTransaction()) {
                advisoryDao.lockUrnsForWrite(opContext, List.of(urn));
                aHoldsLock.countDown();
                releaseA.await(10, TimeUnit.SECONDS);
                tx.commit();
              } catch (Throwable t) {
                failure.set(t);
                aHoldsLock.countDown();
              }
            });
    final Thread b =
        new Thread(
            () -> {
              try {
                aHoldsLock.await(10, TimeUnit.SECONDS);
                try (Transaction tx = primaryDatabase.beginTransaction()) {
                  advisoryDao.lockUrnsForWrite(opContext, List.of(urn));
                  bAcquiredAtNanos.set(System.nanoTime());
                  tx.commit();
                }
              } catch (Throwable t) {
                failure.set(t);
              }
            });

    a.start();
    b.start();
    aHoldsLock.await(10, TimeUnit.SECONDS);

    // While A holds the advisory lock, B must be blocked and must not have acquired it.
    Thread.sleep(400);
    assertEquals(bAcquiredAtNanos.get(), 0L, "B acquired the advisory lock while A still held it");

    final long releasedAtNanos = System.nanoTime();
    releaseA.countDown();
    a.join(10_000);
    b.join(10_000);

    assertNull(failure.get(), "advisory lock threads should not error");
    assertTrue(
        bAcquiredAtNanos.get() >= releasedAtNanos,
        "B must acquire the advisory lock only after A released it on commit");
  }

  @Test
  public void advisoryLock_isNoOpWhenDisabled() throws Exception {
    final String urn = "urn:li:corpuser:adv_disabled_" + shortId();
    final CountDownLatch aHoldsTxn = new CountDownLatch(1);
    final CountDownLatch releaseA = new CountDownLatch(1);
    final AtomicLong bProceededAtNanos = new AtomicLong(0);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread a =
        new Thread(
            () -> {
              try (Transaction tx = primaryDatabase.beginTransaction()) {
                defaultDao.lockUrnsForWrite(opContext, List.of(urn));
                aHoldsTxn.countDown();
                releaseA.await(10, TimeUnit.SECONDS);
                tx.commit();
              } catch (Throwable t) {
                failure.set(t);
                aHoldsTxn.countDown();
              }
            });
    final Thread b =
        new Thread(
            () -> {
              try {
                aHoldsTxn.await(10, TimeUnit.SECONDS);
                try (Transaction tx = primaryDatabase.beginTransaction()) {
                  defaultDao.lockUrnsForWrite(opContext, List.of(urn));
                  bProceededAtNanos.set(System.nanoTime());
                  tx.commit();
                }
              } catch (Throwable t) {
                failure.set(t);
              }
            });

    a.start();
    b.start();
    aHoldsTxn.await(10, TimeUnit.SECONDS);

    // Disabled → no serialization: B proceeds even while A's transaction is open.
    Thread.sleep(400);
    assertNotEquals(
        bProceededAtNanos.get(), 0L, "disabled advisory lock must not serialize writers");

    releaseA.countDown();
    a.join(10_000);
    b.join(10_000);
    assertNull(failure.get());
  }

  @Test
  public void deleteUrn_orderedForUpdate_runsOnPostgresAndRemovesAllRows() {
    final String urn = "urn:li:corpuser:delete_" + shortId();
    saveAspect(urn, opContext.getKeyAspectName(com.linkedin.common.urn.UrnUtils.getUrn(urn)));
    saveAspect(urn, STATUS_ASPECT_NAME);

    final int deleted;
    try (Transaction tx = primaryDatabase.beginTransaction()) {
      deleted = advisoryDao.deleteUrn(opContext, null, urn);
      tx.commit();
    }

    assertEquals(deleted, 2, "both the key aspect and the status aspect should be deleted");
    assertNull(advisoryDao.getAspect(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION));
  }

  @Test
  public void getLatestAspects_orderedForUpdate_runsOnPostgres() {
    final String urn = "urn:li:corpuser:get_" + shortId();
    saveAspect(urn, STATUS_ASPECT_NAME);

    try (Transaction tx = primaryDatabase.beginTransaction()) {
      final Map<String, Map<String, com.linkedin.metadata.aspect.SystemAspect>> latest =
          advisoryDao.getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true);
      assertTrue(
          latest.getOrDefault(urn, Map.of()).containsKey(STATUS_ASPECT_NAME),
          "ordered FOR UPDATE read should return the row on PostgreSQL");
      tx.commit();
    }
  }

  @Test
  public void batchGet_unionMethodFallsBackToInOnPostgres() {
    // PostgreSQL rejects "... UNION ALL ... FOR UPDATE", so the constructor must remap
    // EBEAN_BATCH_GET_METHOD=UNION to the IN form on Postgres. Without the remap this
    // locking batchGet would fail outright.
    final String urn = "urn:li:corpuser:union_" + shortId();
    saveAspect(urn, STATUS_ASPECT_NAME);

    final EbeanAspectDao unionDao =
        new EbeanAspectDao(
            primaryDatabase,
            EbeanConfiguration.builder().batchGetMethod("UNION").build(),
            null,
            List.of(),
            null);
    unionDao.setConnectionValidated(true);

    try (Transaction tx = primaryDatabase.beginTransaction()) {
      final Map<EntityAspectIdentifier, EntityAspect> result =
          unionDao.batchGet(
              Set.of(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION)),
              true);
      assertEquals(result.size(), 1, "locking batchGet should work with UNION remapped to IN");
      tx.commit();
    }
  }

  @Test
  public void lockUrnsForWrite_withoutActiveTransaction_skipsGracefully() {
    // No enclosing transaction → the advisory lock can't be held (would auto-commit), so it is
    // skipped with a warning rather than aborting the caller. Must not throw.
    advisoryDao.lockUrnsForWrite(opContext, List.of("urn:li:corpuser:no_txn_" + shortId()));
  }

  private void saveAspect(String urn, String aspectName) {
    final EbeanAspectV2 row = new EbeanAspectV2();
    row.setUrn(urn);
    row.setAspect(aspectName);
    row.setVersion(ASPECT_LATEST_VERSION);
    row.setMetadata("{}");
    row.setCreatedBy(CREATED_BY);
    row.setCreatedOn(new Timestamp(System.currentTimeMillis()));
    primaryDatabase.save(row);
  }

  private static String shortId() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }
}
