package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
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
  private static final long LOCK_WAIT_SECONDS = 10;
  private static final long NON_BLOCK_ACQUIRE_SECONDS = 2;
  private static final long THREAD_JOIN_MILLIS = 10_000;

  private PostgreSQLContainer<?> postgres;
  private Database primaryDatabase;
  private EbeanAspectDao advisoryDao;
  private EbeanAspectDao defaultDao;
  private OperationContext opContext;

  @BeforeClass
  public void init() {
    postgres = PostgresTestUtils.startPostgres();
    PostgresTestUtils.IntegrationNamespace ns =
        PostgresTestUtils.newIntegrationNamespace("locking_it");
    primaryDatabase =
        PostgresTestUtils.createEbeanPrimaryDatabase(
            postgres, PostgresTestUtils.uniqueServerName("locking_it"), ns);
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

  /**
   * Runs a two-thread advisory-lock contention scenario and asserts whether B blocks, with no fixed
   * sleeps (deterministic latch sequencing).
   *
   * <p>Thread A opens a transaction, runs {@code aBody} (acquires a lock and holds it), signals it
   * holds the lock, waits for {@code releaseA}, then commits. Thread B waits for A's signal,
   * signals it is about to attempt, opens a transaction, runs {@code bBody} (attempts its lock),
   * records the acquire time, and commits.
   *
   * @param aBody runs inside A's transaction; acquires A's lock and holds it
   * @param bBody runs inside B's transaction; attempts to acquire B's lock
   * @param expectBBlocks true if B must block until A commits; false if B must proceed promptly
   */
  private void runLockPair(Runnable aBody, Runnable bBody, boolean expectBBlocks) throws Exception {
    final CountDownLatch aHoldsLock = new CountDownLatch(1);
    final CountDownLatch releaseA = new CountDownLatch(1);
    final CountDownLatch bAttempting = new CountDownLatch(1);
    final CountDownLatch bAcquired = new CountDownLatch(1);
    final AtomicLong bAcquiredAtNanos = new AtomicLong(0);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread a =
        new Thread(
            () -> {
              try (Transaction tx = primaryDatabase.beginTransaction()) {
                aBody.run();
                aHoldsLock.countDown();
                releaseA.await(LOCK_WAIT_SECONDS, TimeUnit.SECONDS);
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
                aHoldsLock.await(LOCK_WAIT_SECONDS, TimeUnit.SECONDS);
                try (Transaction tx = primaryDatabase.beginTransaction()) {
                  // Signal inside the open transaction, right before the lock call, so
                  // bAttempting means "B is in the transaction and about to lock" -- a
                  // stronger not-yet-acquired anchor than signaling before beginTransaction.
                  bAttempting.countDown();
                  bBody.run();
                  bAcquiredAtNanos.set(System.nanoTime());
                  bAcquired.countDown();
                  tx.commit();
                }
              } catch (Throwable t) {
                failure.set(t);
              }
            });

    a.start();
    b.start();
    aHoldsLock.await(LOCK_WAIT_SECONDS, TimeUnit.SECONDS);

    if (expectBBlocks) {
      // Deterministic blocking proof (no sleep): wait for B to signal it is attempting the
      // lock, then assert it has NOT acquired (A still holds it). B signaled bAttempting
      // right before calling lockAspectsForWrite, so at this point B is at or past the
      // blocking call; if the lock works, B is blocked and bAcquiredAtNanos is still 0.
      bAttempting.await(LOCK_WAIT_SECONDS, TimeUnit.SECONDS);
      assertEquals(bAcquiredAtNanos.get(), 0L, "B acquired while A still held the lock");
      final long releasedAtNanos = System.nanoTime();
      releaseA.countDown();
      a.join(THREAD_JOIN_MILLIS);
      b.join(THREAD_JOIN_MILLIS);
      assertNull(failure.get(), "lock threads should not error");
      assertTrue(
          bAcquiredAtNanos.get() >= releasedAtNanos,
          "B must acquire only after A released on commit");
    } else {
      // Non-blocking proof (no sleep): A KEEPS holding its lock while we judge B.
      // Anchor the judgment window on bAttempting (B is in its transaction and
      // about to lock), not on aHoldsLock, so the NON_BLOCK_ACQUIRE_SECONDS
      // window starts only once B is actually ready to lock. This avoids a
      // spurious timeout if B is slow to schedule between aHoldsLock and its
      // lock call. If B acquires while A holds, the lock is not over-serializing
      // (correct). If B blocks (bug), the latch times out and the test fails.
      // Do NOT release A before judging B -- releasing A early would let an
      // over-serializing lock unblock B and pass spuriously, hiding the
      // regression this test is meant to catch. Release A only after the
      // judgment so threads can finish.
      bAttempting.await(LOCK_WAIT_SECONDS, TimeUnit.SECONDS);
      boolean acquired = bAcquired.await(NON_BLOCK_ACQUIRE_SECONDS, TimeUnit.SECONDS);
      releaseA.countDown();
      a.join(THREAD_JOIN_MILLIS);
      b.join(THREAD_JOIN_MILLIS);
      assertNull(failure.get(), "lock threads should not error");
      assertTrue(
          acquired,
          "B did not acquire within "
              + NON_BLOCK_ACQUIRE_SECONDS
              + "s after signaling it was about to lock (while A held its lock);"
              + " it appears blocked behind A, which means the lock"
              + " over-serialized when it should not have");
    }
  }

  /** Same (urn, aspect): B must block until A commits, then acquire. */
  @Test
  public void advisoryLock_serializesConcurrentWritersOnSameUrnAspect() throws Exception {
    final String urn = "urn:li:corpuser:adv_same_" + shortId();
    final String aspect = STATUS_ASPECT_NAME;
    runLockPair(
        () -> advisoryDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(aspect))),
        () -> advisoryDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(aspect))),
        true);
  }

  /**
   * Same URN, different aspects: B must proceed while A holds its aspect lock. This is the fix for
   * the entity-level over-serialization regression — cross-aspect writers on the same URN share no
   * row and must not share a mutex.
   */
  @Test
  public void advisoryLock_doesNotSerializeDifferentAspectsOnSameUrn() throws Exception {
    final String urn = "urn:li:corpuser:adv_diff_" + shortId();
    runLockPair(
        () -> advisoryDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME))),
        () -> advisoryDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(KEY_ASPECT_NAME))),
        false);
  }

  /** Disjoint URNs: B must proceed while A holds its lock. No shared key, no contention. */
  @Test
  public void advisoryLock_disjointUrnsDoNotContend() throws Exception {
    final String urnA = "urn:li:corpuser:adv_disjoint_a_" + shortId();
    final String urnB = "urn:li:corpuser:adv_disjoint_b_" + shortId();
    runLockPair(
        () -> advisoryDao.lockAspectsForWrite(opContext, Map.of(urnA, Set.of(STATUS_ASPECT_NAME))),
        () -> advisoryDao.lockAspectsForWrite(opContext, Map.of(urnB, Set.of(STATUS_ASPECT_NAME))),
        false);
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
    runLockPair(
        () -> advisoryDao.deleteUrn(opContext, null, urn),
        () -> advisoryDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME))),
        true);
  }

  @Test
  public void advisoryLock_isNoOpWhenDisabled() throws Exception {
    final String urn = "urn:li:corpuser:adv_disabled_" + shortId();
    final String aspect = STATUS_ASPECT_NAME;
    runLockPair(
        () -> defaultDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(aspect))),
        () -> defaultDao.lockAspectsForWrite(opContext, Map.of(urn, Set.of(aspect))),
        false);
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
