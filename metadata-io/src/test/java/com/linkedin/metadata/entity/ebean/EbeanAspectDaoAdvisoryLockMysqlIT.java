package com.linkedin.metadata.entity.ebean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.MysqlTestUtils;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.Transaction;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.testcontainers.containers.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testcontainers integration test for the MySQL per-entity advisory lock ({@code GET_LOCK} / {@code
 * RELEASE_LOCK}) used by {@code EbeanAspectDao.lockUrnsForWrite} / {@code releaseUrnsForWrite}.
 * Unlike Postgres' transaction-scoped {@code pg_advisory_xact_lock}, MySQL named locks are
 * session-scoped, so this verifies against a real MySQL that:
 *
 * <ul>
 *   <li>concurrent writers on the same urn serialize (one blocks until the other releases), and
 *   <li>{@code releaseUrnsForWrite} — keyed by the urns, with no JVM-side registry — actually frees
 *       the lock on the acquiring connection before commit, so a subsequent writer can proceed.
 * </ul>
 *
 * <p><b>Depends on the MySQL Testcontainers harness ({@code MysqlTestUtils}, the {@code
 * testContainersMysql} dependency, and {@code testng-mysql.xml}) that ships with the base
 * optimistic-locking change.</b> Register this class in {@code testng-mysql.xml} alongside that
 * change; it is not built or run without that harness.
 */
public class EbeanAspectDaoAdvisoryLockMysqlIT {

  private MySQLContainer<?> mysql;
  private Database primaryDatabase;
  private EbeanAspectDao advisoryDao;
  private OperationContext opContext;

  @BeforeClass
  public void init() {
    mysql = MysqlTestUtils.startMysql();
    primaryDatabase =
        MysqlTestUtils.createEbeanPrimaryDatabase(
            mysql, MysqlTestUtils.uniqueServerName("advisory_lock_it"));
    final PrimaryStorageResolver resolver = PrimaryStorageTestUtils.ebeanResolver(primaryDatabase);
    advisoryDao =
        new EbeanAspectDao(
            resolver,
            EbeanConfiguration.builder().entityWriteAdvisoryLockEnabled(true).build(),
            null,
            List.of(),
            null);
    advisoryDao.setConnectionValidated(true);
    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(primaryDatabase);
  }

  @Test
  public void getLock_serializesConcurrentWritersOnSameUrn() throws Exception {
    final String urn = "urn:li:corpuser:mysql_serialize_" + MysqlTestUtils.shortId();
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
                // Release the session-scoped GET_LOCK on THIS connection before commit.
                advisoryDao.releaseUrnsForWrite(opContext, List.of(urn));
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
                  advisoryDao.releaseUrnsForWrite(opContext, List.of(urn));
                  tx.commit();
                }
              } catch (Throwable t) {
                failure.set(t);
              }
            });

    a.start();
    b.start();
    aHoldsLock.await(10, TimeUnit.SECONDS);

    // While A holds the named lock, B must be blocked on GET_LOCK and must not have acquired it.
    Thread.sleep(400);
    assertEquals(bAcquiredAtNanos.get(), 0L, "B acquired the MySQL lock while A still held it");

    final long releasedAtNanos = System.nanoTime();
    releaseA.countDown();
    a.join(15_000);
    b.join(15_000);

    assertNull(failure.get(), "advisory lock threads should not error");
    assertTrue(
        bAcquiredAtNanos.get() >= releasedAtNanos,
        "B must acquire the MySQL lock only after A released it");
  }

  @Test
  public void releaseUrnsForWrite_freesLockOnSameConnectionBeforeCommit() throws Exception {
    // Acquire then release within one transaction (no commit yet); a second connection must then be
    // able to acquire immediately — proving the keyed, stateless release hit the right connection.
    final String urn = "urn:li:corpuser:mysql_release_" + MysqlTestUtils.shortId();
    try (Transaction tx = primaryDatabase.beginTransaction()) {
      advisoryDao.lockUrnsForWrite(opContext, List.of(urn));
      advisoryDao.releaseUrnsForWrite(opContext, List.of(urn));
      // still inside tx (not committed) — the lock must already be free
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicLong acquiredAt = new AtomicLong(0);
      Thread other =
          new Thread(
              () -> {
                try (Transaction otherTx = primaryDatabase.beginTransaction()) {
                  advisoryDao.lockUrnsForWrite(opContext, List.of(urn));
                  acquiredAt.set(System.nanoTime());
                  advisoryDao.releaseUrnsForWrite(opContext, List.of(urn));
                  otherTx.commit();
                } catch (Throwable t) {
                  failure.set(t);
                }
              });
      other.start();
      other.join(5_000);
      assertNull(failure.get());
      assertTrue(acquiredAt.get() > 0L, "second writer must acquire once the first released");
      tx.commit();
    }
  }
}
