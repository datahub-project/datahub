package com.linkedin.metadata.entity.ebean.optimistic;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.ConditionalSaveResult;
import com.linkedin.metadata.entity.ConditionalWriteOutcome;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.OptimisticLockConflictException;
import com.linkedin.metadata.entity.TransactionResult;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanSystemAspect;
import com.linkedin.metadata.entity.lock.EntityWriteLock;
import com.linkedin.metadata.entity.lock.HazelcastEntityWriteLock;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

/**
 * Shared optimistic-locking assertions against a real SQL dialect (not H2 INSTR). Concrete ITs
 * supply a {@link Database} whose platform matches MySQL or PostgreSQL.
 */
abstract class EbeanOptimisticLockingDialectIT {

  protected abstract Database database();

  protected abstract EbeanAspectDao.Dialect expectedDialect();

  protected final OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  protected EbeanAspectDao newOptimisticDao() {
    return newDao(/* optimisticLocking */ true);
  }

  protected EbeanAspectDao newLegacyDao() {
    return newDao(/* optimisticLocking */ false);
  }

  private EbeanAspectDao newDao(boolean optimisticLocking) {
    PrimaryStorageResolver resolver = PrimaryStorageTestUtils.ebeanResolver(database());
    EbeanAspectDao dao =
        new EbeanAspectDao(
            resolver,
            EbeanConfiguration.testDefault,
            mock(MetricUtils.class),
            List.of(),
            null,
            optimisticLocking);
    dao.setWritable(true);
    // DDL from Testcontainers may not include every production index; skip connection validation.
    dao.setConnectionValidated(true);
    return dao;
  }

  protected SystemAspect statusAspect(String urn, Status status, SystemMetadata systemMetadata) {
    return new EbeanSystemAspect(
        null,
        UrnUtils.getUrn(urn),
        STATUS_ASPECT_NAME,
        opContext.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME),
        opContext.getEntityRegistry().getAspectSpecs().get(STATUS_ASPECT_NAME),
        status,
        systemMetadata,
        AuditStampUtils.createDefaultAuditStamp(),
        null,
        null,
        null);
  }

  @Test
  public void detectsExpectedDialect() {
    EbeanAspectDao dao = newOptimisticDao();
    assertEquals(dao.getDialect(), expectedDialect());
    String sql = dao.buildConditionalUpdateSql(dao.getDialect());
    if (expectedDialect() == EbeanAspectDao.Dialect.POSTGRES) {
      assertTrue(sql.contains("systemmetadata::jsonb ->> 'version'"));
    } else if (expectedDialect() == EbeanAspectDao.Dialect.MYSQL) {
      assertTrue(sql.contains("systemmetadata->>'$.version'"));
    } else {
      throw new AssertionError("unexpected dialect " + expectedDialect());
    }
  }

  @Test
  public void conditionalUpdateMatchesAndConflictsOnSystemMetadataVersion() {
    EbeanAspectDao dao = newOptimisticDao();
    String urn = "urn:li:corpuser:olDialectCas_" + expectedDialect().name().toLowerCase();

    SystemMetadata v1 = new SystemMetadata();
    v1.setVersion("1");
    dao.insertAspect(
        opContext,
        null,
        statusAspect(urn, new Status().setRemoved(false), v1),
        ASPECT_LATEST_VERSION);

    SystemMetadata v2 = new SystemMetadata();
    v2.setVersion("2");
    Optional<EntityAspect> conflict =
        dao.updateAspectConditional(
            opContext, null, statusAspect(urn, new Status().setRemoved(true), v2), "99");
    assertFalse(conflict.isPresent(), "stale expected version must not update");

    Optional<EntityAspect> matched =
        dao.updateAspectConditional(
            opContext, null, statusAspect(urn, new Status().setRemoved(true), v2), "1");
    assertTrue(matched.isPresent(), "CAS must match real dialect JSON version predicate");

    EntityAspect after =
        dao.batchGet(
                opContext,
                Set.of(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION)),
                false)
            .get(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION));
    assertNotNull(after);
    assertEquals(
        SystemMetadataUtils.parseSystemMetadata(after.getSystemMetadata()).getVersion(), "2");
  }

  @Test
  public void saveLatestAspectConditionalConflictThenRetrySucceeds() {
    EbeanAspectDao dao = newOptimisticDao();
    String urn = "urn:li:corpuser:olDialectRetry_" + expectedDialect().name().toLowerCase();

    SystemMetadata seed = new SystemMetadata();
    seed.setVersion("1");
    dao.insertAspect(
        opContext,
        null,
        statusAspect(urn, new Status().setRemoved(false), seed),
        ASPECT_LATEST_VERSION);

    AtomicInteger attempts = new AtomicInteger();
    dao.runInTransactionWithRetryUnlocked(
        opContext,
        (txContext) -> {
          if (attempts.getAndIncrement() == 0) {
            throw new OptimisticLockConflictException("force retry on " + expectedDialect());
          }
          SystemAspect latest =
              dao.getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true)
                  .get(urn)
                  .get(STATUS_ASPECT_NAME);
          String expected = latest.getDatabaseAspect().get().getSystemMetadata().getVersion();
          SystemMetadata next = new SystemMetadata();
          next.setVersion(String.valueOf(Long.parseLong(expected) + 1));
          ConditionalSaveResult result =
              dao.saveLatestAspectConditional(
                  opContext,
                  txContext,
                  latest,
                  statusAspect(urn, new Status().setRemoved(true), next),
                  1);
          assertEquals(result.getOutcome(), ConditionalWriteOutcome.UPDATED);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        5);

    assertEquals(attempts.get(), 2);
    EntityAspect after = dao.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertEquals(
        SystemMetadataUtils.parseSystemMetadata(after.getSystemMetadata()).getVersion(), "2");
  }

  @Test
  public void concurrentWritersConvergeWithVersionBump() throws Exception {
    EbeanAspectDao dao = newOptimisticDao();
    String urn = "urn:li:corpuser:olDialectConcurrent_" + expectedDialect().name().toLowerCase();

    SystemMetadata seed = new SystemMetadata();
    seed.setVersion("1");
    dao.insertAspect(
        opContext,
        null,
        statusAspect(urn, new Status().setRemoved(false), seed),
        ASPECT_LATEST_VERSION);

    AtomicInteger successes = new AtomicInteger();
    AtomicInteger conflicts = new AtomicInteger();
    AtomicReference<Throwable> firstError = new AtomicReference<>();
    CountDownLatch bothRead = new CountDownLatch(2);
    CountDownLatch writeGate = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(2);
    ExecutorService pool = Executors.newFixedThreadPool(2);

    for (int i = 0; i < 2; i++) {
      final boolean removed = i == 0;
      pool.submit(
          () -> {
            try {
              dao.runInTransactionWithRetryUnlocked(
                  opContext,
                  (txContext) -> {
                    // forUpdate=false: SELECT FOR UPDATE before the write-gate deadlocks both
                    // writers (one holds the row lock while waiting on the latch).
                    SystemAspect latest =
                        dao.getLatestAspects(
                                opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), false)
                            .get(urn)
                            .get(STATUS_ASPECT_NAME);
                    String expected =
                        latest.getDatabaseAspect().get().getSystemMetadata().getVersion();
                    bothRead.countDown();
                    try {
                      assertTrue(writeGate.await(45, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException(e);
                    }
                    SystemMetadata next = new SystemMetadata();
                    next.setVersion(String.valueOf(Long.parseLong(expected) + 1));
                    ConditionalSaveResult r =
                        dao.saveLatestAspectConditional(
                            opContext,
                            txContext,
                            latest,
                            statusAspect(urn, new Status().setRemoved(removed), next),
                            1);
                    if (r.getOutcome() == ConditionalWriteOutcome.CONFLICT) {
                      conflicts.incrementAndGet();
                      throw new OptimisticLockConflictException("conflict " + removed);
                    }
                    assertEquals(r.getOutcome(), ConditionalWriteOutcome.UPDATED);
                    successes.incrementAndGet();
                    return TransactionResult.commit("");
                  },
                  mock(AspectsBatch.class),
                  10);
            } catch (Throwable t) {
              firstError.compareAndSet(null, t);
            } finally {
              done.countDown();
            }
          });
    }

    assertTrue(
        bothRead.await(45, TimeUnit.SECONDS), "bothRead timed out; firstError=" + firstError.get());
    writeGate.countDown();
    assertTrue(done.await(90, TimeUnit.SECONDS));
    pool.shutdownNow();

    assertNull(firstError.get(), "unexpected failure: " + firstError.get());
    assertEquals(successes.get(), 2);
    assertTrue(conflicts.get() >= 1, "expected at least one CAS conflict");

    EntityAspect after = dao.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertEquals(
        SystemMetadataUtils.parseSystemMetadata(after.getSystemMetadata()).getVersion(), "3");
  }

  /**
   * The write gate's payoff: with the Hazelcast per-URN gate, N concurrent writers on the SAME urn
   * serialize — each reads the fresh committed version under the lock, so its CAS succeeds first
   * try. Contrast {@link #concurrentWritersConvergeWithVersionBump} (ungated), which forces a CAS
   * conflict. Here: zero conflicts, no lost writes, final version = seed(1) + one bump per writer.
   */
  @Test
  public void writeGateSerializesConcurrentWritersWithoutCasThrash() throws Exception {
    EbeanAspectDao dao = newOptimisticDao();
    String urn = "urn:li:corpuser:olGate_" + expectedDialect().name().toLowerCase();

    SystemMetadata seed = new SystemMetadata();
    seed.setVersion("1");
    dao.insertAspect(
        opContext,
        null,
        statusAspect(urn, new Status().setRemoved(false), seed),
        ASPECT_LATEST_VERSION);

    final int writers = 8;
    final HazelcastInstance hz = isolatedHazelcast();
    final EntityWriteLock gate = new HazelcastEntityWriteLock(hz, "it-entity-write-gate", 30, 300);

    final AtomicInteger successes = new AtomicInteger();
    final AtomicInteger conflicts = new AtomicInteger();
    final AtomicReference<Throwable> firstError = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(writers);
    final ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      for (int i = 0; i < writers; i++) {
        final boolean removed = (i % 2 == 0);
        pool.submit(
            () -> {
              // Gate acquired BEFORE the transaction (as EntityServiceImpl does), so writers queue
              // and
              // each reads a fresh committed version -> its CAS never conflicts.
              try (EntityWriteLock.LockHandle handle = gate.acquire(opContext, List.of(urn))) {
                dao.runInTransactionWithRetryUnlocked(
                    opContext,
                    (txContext) -> {
                      SystemAspect latest =
                          dao.getLatestAspects(
                                  opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), false)
                              .get(urn)
                              .get(STATUS_ASPECT_NAME);
                      String expected =
                          latest.getDatabaseAspect().get().getSystemMetadata().getVersion();
                      SystemMetadata next = new SystemMetadata();
                      next.setVersion(String.valueOf(Long.parseLong(expected) + 1));
                      ConditionalSaveResult r =
                          dao.saveLatestAspectConditional(
                              opContext,
                              txContext,
                              latest,
                              statusAspect(urn, new Status().setRemoved(removed), next),
                              1);
                      if (r.getOutcome() == ConditionalWriteOutcome.CONFLICT) {
                        conflicts.incrementAndGet();
                        throw new OptimisticLockConflictException("conflict");
                      }
                      successes.incrementAndGet();
                      return TransactionResult.commit("");
                    },
                    mock(AspectsBatch.class),
                    10);
              } catch (Throwable t) {
                firstError.compareAndSet(null, t);
              } finally {
                done.countDown();
              }
            });
      }
      assertTrue(done.await(120, TimeUnit.SECONDS), "gated writers timed out");
    } finally {
      pool.shutdownNow();
      hz.shutdown();
    }

    assertNull(firstError.get(), "unexpected failure: " + firstError.get());
    assertEquals(successes.get(), writers, "every gated writer must apply exactly once (no loss)");
    assertEquals(
        conflicts.get(),
        0,
        "gate serializes writers so CAS never conflicts (no thundering-herd thrash)");
    EntityAspect after = dao.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertEquals(
        SystemMetadataUtils.parseSystemMetadata(after.getSystemMetadata()).getVersion(),
        String.valueOf(writers + 1),
        "final version = seed(1) + one bump per writer");
  }

  private static HazelcastInstance isolatedHazelcast() {
    Config config = new Config();
    config.setInstanceName("ol-it-hz-" + UUID.randomUUID());
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    return Hazelcast.newHazelcastInstance(config);
  }

  @Test
  public void skippedNoOpDoesNotConflict() {
    EbeanAspectDao dao = newOptimisticDao();
    String urn = "urn:li:corpuser:olDialectNoOp_" + expectedDialect().name().toLowerCase();

    SystemMetadata seed = new SystemMetadata();
    seed.setVersion("1");
    Status status = new Status().setRemoved(false);
    dao.insertAspect(opContext, null, statusAspect(urn, status, seed), ASPECT_LATEST_VERSION);

    SystemAspect latest =
        dao.getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true)
            .get(urn)
            .get(STATUS_ASPECT_NAME);

    Status sameStatus =
        latest.getRecordTemplate() instanceof Status ? (Status) latest.getRecordTemplate() : status;
    ConditionalSaveResult result =
        dao.saveLatestAspectConditional(
            opContext, null, latest, statusAspect(urn, sameStatus, latest.getSystemMetadata()), 1);
    assertEquals(result.getOutcome(), ConditionalWriteOutcome.SKIPPED_NOOP);
  }

  /**
   * Mixed fleet: legacy {@code SELECT FOR UPDATE} writer and OL CAS writer share one DB. Sequential
   * hand-off must keep {@code SystemMetadata.version} coherent.
   */
  @Test
  public void mixedModeLegacyThenOptimisticThenLegacyConverges() {
    EbeanAspectDao legacy = newLegacyDao();
    EbeanAspectDao optimistic = newOptimisticDao();
    String urn = "urn:li:corpuser:olDialectMixedSeq_" + expectedDialect().name().toLowerCase();

    SystemMetadata seed = new SystemMetadata();
    seed.setVersion("1");
    legacy.insertAspect(
        opContext,
        null,
        statusAspect(urn, new Status().setRemoved(false), seed),
        ASPECT_LATEST_VERSION);

    legacy.runInTransactionWithRetryUnlocked(
        opContext,
        (tx) -> {
          SystemAspect latest =
              legacy
                  .getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true)
                  .get(urn)
                  .get(STATUS_ASPECT_NAME);
          SystemMetadata next = new SystemMetadata();
          next.setVersion("2");
          legacy.saveLatestAspect(
              opContext, tx, latest, statusAspect(urn, new Status().setRemoved(true), next), 1);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        3);

    optimistic.runInTransactionWithRetryUnlocked(
        opContext,
        (tx) -> {
          SystemAspect latest =
              optimistic
                  .getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true)
                  .get(urn)
                  .get(STATUS_ASPECT_NAME);
          SystemMetadata next = new SystemMetadata();
          next.setVersion("3");
          ConditionalSaveResult r =
              optimistic.saveLatestAspectConditional(
                  opContext,
                  tx,
                  latest,
                  statusAspect(urn, new Status().setRemoved(false), next),
                  1);
          assertEquals(r.getOutcome(), ConditionalWriteOutcome.UPDATED);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        3);

    legacy.runInTransactionWithRetryUnlocked(
        opContext,
        (tx) -> {
          SystemAspect latest =
              legacy
                  .getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true)
                  .get(urn)
                  .get(STATUS_ASPECT_NAME);
          SystemMetadata next = new SystemMetadata();
          next.setVersion("4");
          legacy.saveLatestAspect(
              opContext, tx, latest, statusAspect(urn, new Status().setRemoved(true), next), 1);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        3);

    EntityAspect after =
        optimistic.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertEquals(
        SystemMetadataUtils.parseSystemMetadata(after.getSystemMetadata()).getVersion(), "4");
    assertTrue(
        after.getMetadata().contains("\"removed\":true")
            || after.getMetadata().contains("\"removed\": true"));
  }

  /**
   * Mixed fleet under contention: legacy holds {@code FOR UPDATE} while OL CAS races. Both writers
   * must converge (OL retries on CAS miss after the lock is released).
   */
  @Test
  public void mixedModeConcurrentLegacyAndOptimisticConverges() throws Exception {
    EbeanAspectDao legacy = newLegacyDao();
    EbeanAspectDao optimistic = newOptimisticDao();
    String urn = "urn:li:corpuser:olDialectMixedConc_" + expectedDialect().name().toLowerCase();

    SystemMetadata seed = new SystemMetadata();
    seed.setVersion("1");
    legacy.insertAspect(
        opContext,
        null,
        statusAspect(urn, new Status().setRemoved(false), seed),
        ASPECT_LATEST_VERSION);

    AtomicInteger successes = new AtomicInteger();
    AtomicReference<Throwable> firstError = new AtomicReference<>();
    CountDownLatch bothRead = new CountDownLatch(2);
    CountDownLatch writeGate = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(2);
    ExecutorService pool = Executors.newFixedThreadPool(2);

    pool.submit(
        () -> {
          try {
            legacy.runInTransactionWithRetryUnlocked(
                opContext,
                (tx) -> {
                  // Hold the row lock across the gate — simulates live GMS/MCE with OL off.
                  SystemAspect latest =
                      legacy
                          .getLatestAspects(
                              opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true)
                          .get(urn)
                          .get(STATUS_ASPECT_NAME);
                  bothRead.countDown();
                  try {
                    assertTrue(writeGate.await(45, TimeUnit.SECONDS));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                  SystemMetadata next = new SystemMetadata();
                  next.setVersion("2");
                  legacy.saveLatestAspect(
                      opContext,
                      tx,
                      latest,
                      statusAspect(urn, new Status().setRemoved(true), next),
                      1);
                  successes.incrementAndGet();
                  return TransactionResult.commit("");
                },
                mock(AspectsBatch.class),
                10);
          } catch (Throwable t) {
            firstError.compareAndSet(null, t);
          } finally {
            done.countDown();
          }
        });

    pool.submit(
        () -> {
          try {
            optimistic.runInTransactionWithRetryUnlocked(
                opContext,
                (tx) -> {
                  // Unlocked read under OL; may observe v1 while legacy holds FOR UPDATE.
                  SystemAspect latest =
                      optimistic
                          .getLatestAspects(
                              opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true)
                          .get(urn)
                          .get(STATUS_ASPECT_NAME);
                  String expected =
                      latest.getDatabaseAspect().get().getSystemMetadata().getVersion();
                  bothRead.countDown();
                  try {
                    assertTrue(writeGate.await(45, TimeUnit.SECONDS));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                  SystemMetadata next = new SystemMetadata();
                  next.setVersion(String.valueOf(Long.parseLong(expected) + 1));
                  ConditionalSaveResult r =
                      optimistic.saveLatestAspectConditional(
                          opContext,
                          tx,
                          latest,
                          statusAspect(urn, new Status().setRemoved(false), next),
                          1);
                  if (r.getOutcome() == ConditionalWriteOutcome.CONFLICT) {
                    throw new OptimisticLockConflictException("mixed-mode CAS conflict");
                  }
                  assertEquals(r.getOutcome(), ConditionalWriteOutcome.UPDATED);
                  successes.incrementAndGet();
                  return TransactionResult.commit("");
                },
                mock(AspectsBatch.class),
                10);
          } catch (Throwable t) {
            firstError.compareAndSet(null, t);
          } finally {
            done.countDown();
          }
        });

    assertTrue(
        bothRead.await(45, TimeUnit.SECONDS), "bothRead timed out; firstError=" + firstError.get());
    writeGate.countDown();
    assertTrue(done.await(90, TimeUnit.SECONDS));
    pool.shutdownNow();

    assertNull(firstError.get(), "mixed-mode writers failed: " + firstError.get());
    assertEquals(successes.get(), 2);

    EntityAspect after =
        optimistic.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertEquals(
        SystemMetadataUtils.parseSystemMetadata(after.getSystemMetadata()).getVersion(), "3");
  }
}
