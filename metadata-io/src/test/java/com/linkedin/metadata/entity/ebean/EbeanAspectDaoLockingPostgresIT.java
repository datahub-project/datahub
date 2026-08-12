package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
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
 * Testcontainers integration test for the PostgreSQL per-{@code (urn, aspect)} advisory write lock
 * and the ordered {@code FOR UPDATE} lock-ordering changes.
 *
 * <p>Verified against a real PostgreSQL instance:
 *
 * <ul>
 *   <li>the opt-in {@code pg_advisory_xact_lock} serializes concurrent writers on the same {@code
 *       (urn, aspect)} pair (holds within a transaction, blocks a concurrent writer on the same
 *       key, releases on commit; no-op when disabled),
 *   <li>writers on the same URN but different aspects do NOT serialize (the fix for the
 *       entity-level over-serialization regression — CAS and {@code FOR UPDATE} contend on {@code
 *       (urn, aspect)}, not the entity),
 *   <li>{@code deleteUrn} locks the entity's full aspect key-set, so a concurrent upsert on any
 *       aspect of that URN blocks (delete↔upsert safety is key-set overlap),
 *   <li>the ordered {@code FOR UPDATE} SQL added to {@code deleteUrn} and {@code batchGetIn}
 *       actually parses and runs on PostgreSQL.
 * </ul>
 *
 * <p>We deliberately do not attempt to reproduce a nondeterministic deadlock; that is flaky and
 * characterizes the engine rather than this code. The advisory-serialization tests are the
 * deterministic proof that the serialization mechanism works at the right granularity.
 */
public class EbeanAspectDaoLockingPostgresIT {

  private static final String CREATED_BY = "urn:li:corpuser:test";
  private static final String KEY_ASPECT_NAME = "corpUserInfo";

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
    final PrimaryStorageResolver resolver = PrimaryStorageTestUtils.ebeanResolver(primaryDatabase);

    advisoryDao =
        new EbeanAspectDao(
            resolver,
            EbeanConfiguration.builder().entityWriteAdvisoryLockEnabled(true).build(),
            null,
            List.of(),
            null,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(primaryDatabase));
    advisoryDao.setConnectionValidated(true);

    defaultDao =
        new EbeanAspectDao(
            resolver,
            EbeanConfiguration.testDefault,
            null,
            List.of(),
            null,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(primaryDatabase));
    defaultDao.setConnectionValidated(true);

    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(primaryDatabase);
  }

  /** Same (urn, aspect): B must block until A commits, then acquire. */
  @Test
  public void advisoryLock_serializesConcurrentWritersOnSameUrnAspect() throws Exception {
    final String urn = "urn:li:corpuser:adv_same_" + shortId();
    final String aspect = STATUS_ASPECT_NAME;
    final CountDownLatch aHoldsLock = new CountDownLatch(1);
    final CountDownLatch releaseA = new CountDownLatch(1);
    final AtomicLong bAcquiredAtNanos = new AtomicLong(0);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread a =
        new Thread(
            () -> {
              try (Transaction tx = primaryDatabase.beginTransaction()) {
                advisoryDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(aspect)));
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
                  advisoryDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(aspect)));
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

  /**
   * Same URN, different aspects: B must proceed while A holds its aspect lock. This is the fix for
   * the entity-level over-serialization regression — cross-aspect writers on the same URN share no
   * row and must not share a mutex.
   */
  @Test
  public void advisoryLock_doesNotSerializeDifferentAspectsOnSameUrn() throws Exception {
    final String urn = "urn:li:corpuser:adv_diff_" + shortId();
    final String aspectA = STATUS_ASPECT_NAME;
    final String aspectB = KEY_ASPECT_NAME;
    final CountDownLatch aHoldsLock = new CountDownLatch(1);
    final CountDownLatch releaseA = new CountDownLatch(1);
    final AtomicLong bAcquiredAtNanos = new AtomicLong(0);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread a =
        new Thread(
            () -> {
              try (Transaction tx = primaryDatabase.beginTransaction()) {
                advisoryDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(aspectA)));
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
                  advisoryDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(aspectB)));
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

    // Different aspect -> no shared key -> B must NOT block on A.
    Thread.sleep(400);
    assertNotEquals(
        bAcquiredAtNanos.get(),
        0L,
        "B on a different aspect must not be serialized behind A on the same URN");

    releaseA.countDown();
    a.join(10_000);
    b.join(10_000);
    assertNull(failure.get(), "advisory lock threads should not error");
  }

  /** Disjoint URNs: B must proceed while A holds its lock. No shared key, no contention. */
  @Test
  public void advisoryLock_disjointUrnsDoNotContend() throws Exception {
    final String urnA = "urn:li:corpuser:adv_disjoint_a_" + shortId();
    final String urnB = "urn:li:corpuser:adv_disjoint_b_" + shortId();
    final String aspect = STATUS_ASPECT_NAME;
    final CountDownLatch aHoldsLock = new CountDownLatch(1);
    final CountDownLatch releaseA = new CountDownLatch(1);
    final AtomicLong bAcquiredAtNanos = new AtomicLong(0);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread a =
        new Thread(
            () -> {
              try (Transaction tx = primaryDatabase.beginTransaction()) {
                advisoryDao.lockAspectsForWrite(opContext, Map.of(urnA, Set.of(aspect)));
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
                  advisoryDao.lockAspectsForWrite(opContext, Map.of(urnB, Set.of(aspect)));
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

    Thread.sleep(400);
    assertNotEquals(
        bAcquiredAtNanos.get(), 0L, "B on a disjoint URN must not be serialized behind A");

    releaseA.countDown();
    a.join(10_000);
    b.join(10_000);
    assertNull(failure.get(), "advisory lock threads should not error");
  }

  /**
   * deleteUrn locks the entity's full aspect key-set (wide), so a concurrent upsert on any aspect
   * of that URN must block until the delete commits. This is the delete<->upsert safety guarantee:
   * key-set overlap, not a permanent URN-wide lock on every ingest.
   */
  @Test
  public void deleteUrn_locksFullAspectSet_blocksUpsertOnAnyAspect() throws Exception {
    final String urn = "urn:li:corpuser:adv_delete_" + shortId();
    // Seed two aspects so deleteUrn has rows to act on.
    saveAspect(urn, KEY_ASPECT_NAME);
    saveAspect(urn, STATUS_ASPECT_NAME);

    final CountDownLatch aHoldsLock = new CountDownLatch(1);
    final CountDownLatch releaseA = new CountDownLatch(1);
    final AtomicLong bAcquiredAtNanos = new AtomicLong(0);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    // Thread A: open tx, run deleteUrn (locks the full aspect set), hold without committing.
    final Thread a =
        new Thread(
            () -> {
              try (Transaction tx = primaryDatabase.beginTransaction()) {
                advisoryDao.deleteUrn(opContext, null, urn);
                aHoldsLock.countDown();
                releaseA.await(10, TimeUnit.SECONDS);
                tx.commit();
              } catch (Throwable t) {
                failure.set(t);
                aHoldsLock.countDown();
              }
            });
    // Thread B: try to acquire the advisory lock for a single aspect of the same URN.
    final Thread b =
        new Thread(
            () -> {
              try {
                aHoldsLock.await(10, TimeUnit.SECONDS);
                try (Transaction tx = primaryDatabase.beginTransaction()) {
                  advisoryDao.lockAspectsForWrite(
                      opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)));
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

    // deleteUrn holds the full aspect key-set -> B's single-aspect lock must block.
    Thread.sleep(400);
    assertEquals(
        bAcquiredAtNanos.get(),
        0L,
        "B on any aspect must block behind deleteUrn's full aspect key-set lock");

    final long releasedAtNanos = System.nanoTime();
    releaseA.countDown();
    a.join(10_000);
    b.join(10_000);

    assertNull(failure.get(), "delete vs upsert threads should not error");
    assertTrue(
        bAcquiredAtNanos.get() >= releasedAtNanos,
        "B must acquire its aspect lock only after deleteUrn committed and released the full set");
  }

  @Test
  public void advisoryLock_isNoOpWhenDisabled() throws Exception {
    final String urn = "urn:li:corpuser:adv_disabled_" + shortId();
    final String aspect = STATUS_ASPECT_NAME;
    final CountDownLatch aHoldsTxn = new CountDownLatch(1);
    final CountDownLatch releaseA = new CountDownLatch(1);
    final AtomicLong bProceededAtNanos = new AtomicLong(0);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread a =
        new Thread(
            () -> {
              try (Transaction tx = primaryDatabase.beginTransaction()) {
                defaultDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(aspect)));
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
                  defaultDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(aspect)));
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
    assertNull(advisoryDao.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION));
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
  public void lockAspectsForWrite_withoutActiveTransaction_skipsGracefully() {
    // No enclosing transaction -> the advisory lock can't be held (would auto-commit), so it is
    // skipped with a warning rather than aborting the caller. Must not throw.
    advisoryDao.lockAspectsForWrite(
        opContext, Map.of("urn:li:corpuser:no_txn_" + shortId(), Set.of(STATUS_ASPECT_NAME)));
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
