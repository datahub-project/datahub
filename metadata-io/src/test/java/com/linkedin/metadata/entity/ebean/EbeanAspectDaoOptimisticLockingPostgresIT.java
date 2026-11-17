package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.ConditionalSaveResult;
import com.linkedin.metadata.entity.ConditionalWriteOutcome;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testcontainers integration test for the optimistic-locking (OL) compare-and-set write path
 * against a real PostgreSQL instance. It exercises the Stage-1 conditional-write DAO API with OL
 * enabled:
 *
 * <ul>
 *   <li>{@code updateAspectConditional} — CAS on the version-0 row guarded by the stored {@code
 *       systemmetadata->>'version'}: succeeds on a matching expected version, and returns empty
 *       (conflict) on a stale one.
 *   <li>{@code saveLatestAspectConditional} — distinguishes {@code UPDATED}, {@code SKIPPED_NOOP},
 *       and {@code CONFLICT}, plus the legacy fallback (last-writer-wins) for rows whose stored
 *       systemMetadata has no {@code version}.
 * </ul>
 *
 * <p><b>Determinism.</b> No test attempts to reproduce a nondeterministic race or deadlock. The
 * single concurrency test uses latches so both writers read the same version before either writes,
 * and relies on PostgreSQL serializing the two row UPDATEs (READ COMMITTED re-checks the CAS
 * predicate against the winner's committed row): exactly one UPDATE matches, the other matches zero
 * rows. Every test isolates itself with a unique urn, so it never depends on table-global state.
 *
 * <p><b>MySQL coverage lives elsewhere.</b> This class is Postgres-specific; the MySQL CAS
 * predicate ({@code systemmetadata->>'$.version'}) is verified by {@code
 * EbeanOptimisticLockingMysqlIT} (and the shared {@code EbeanOptimisticLockingDialectIT}
 * assertions) via {@code MysqlTestUtils}, run by the {@code :metadata-io:testMysql} suite ({@code
 * testng-mysql.xml}).
 */
public class EbeanAspectDaoOptimisticLockingPostgresIT {

  private static final String CREATED_BY = "urn:li:corpuser:test";
  // Fixed so serialized systemMetadata round-trips to an equal object (no wall-clock jitter),
  // which the SKIPPED_NOOP equality check depends on.
  private static final long FIXED_TS = 1_700_000_000_000L;

  private PostgreSQLContainer<?> postgres;
  private Database primaryDatabase;
  private EbeanAspectDao dao;
  private OperationContext opContext;

  @BeforeClass
  public void init() {
    postgres = PostgresTestUtils.startPostgres();
    primaryDatabase =
        PostgresTestUtils.createEbeanPrimaryDatabase(
            postgres, PostgresTestUtils.uniqueServerName("ol_it"));
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

    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(primaryDatabase);
  }

  @Test
  public void isOptimisticLockingEnabled_reflectsConfig() {
    assertTrue(dao.isOptimisticLockingEnabled());
  }

  @Test
  public void updateAspectConditional_matchingVersion_updatesRow() {
    final String urn = "urn:li:corpuser:ol_match_" + shortId();
    seedVersion0(urn, new Status(), sysMeta("1"));

    final Optional<EntityAspect> result =
        inTx(
            txContext ->
                dao.updateAspectConditional(
                    opContext,
                    txContext,
                    newAspect(urn, new Status().setRemoved(true), sysMeta("2")),
                    "1"));

    assertTrue(result.isPresent(), "matching CAS should update the version-0 row");
    assertEquals(storedVersion(urn), "2", "row must reflect the new systemMetadata version");
    assertTrue(storedRemoved(urn), "row must reflect the new aspect value");
  }

  @Test
  public void updateAspectConditional_staleVersion_returnsEmptyConflict() {
    final String urn = "urn:li:corpuser:ol_stale_" + shortId();
    seedVersion0(urn, new Status(), sysMeta("1"));

    // Advance the row from version 1 -> 2 with a first, matching conditional update.
    final Optional<EntityAspect> advanced =
        inTx(
            txContext ->
                dao.updateAspectConditional(
                    opContext, txContext, newAspect(urn, new Status(), sysMeta("2")), "1"));
    assertTrue(advanced.isPresent(), "precondition: first conditional update should succeed");

    // Re-issue with the now-STALE expected version "1": the row is at "2", so zero rows match.
    final Optional<EntityAspect> stale =
        inTx(
            txContext ->
                dao.updateAspectConditional(
                    opContext,
                    txContext,
                    newAspect(urn, new Status().setRemoved(true), sysMeta("3")),
                    "1"));

    assertFalse(stale.isPresent(), "stale expected version must be treated as a conflict (empty)");
    assertEquals(storedVersion(urn), "2", "the stale write must not have modified the row");
    assertFalse(storedRemoved(urn), "the stale write must not have changed the aspect value");
  }

  @Test
  public void saveLatestAspectConditional_noOp_returnsSkippedNoop() {
    final String urn = "urn:li:corpuser:ol_noop_" + shortId();
    final SystemMetadata sm = sysMeta("1");
    final String smJson = RecordUtils.toJsonString(sm);
    final String contentJson = RecordUtils.toJsonString(new Status());
    seedVersion0Json(urn, contentJson, smJson);

    // latest wraps the stored row; newAspect carries byte-identical content + systemMetadata, so
    // this is a true no-op (not a conflict) and must not write a new version.
    final SystemAspect latest =
        EbeanSystemAspect.builder()
            .forUpdate(row(urn, contentJson, smJson), opContext.getEntityRegistry());
    final SystemAspect newAspect =
        newAspect(
            urn,
            RecordUtils.toRecordTemplate(Status.class, contentJson),
            RecordUtils.toRecordTemplate(SystemMetadata.class, smJson));

    final ConditionalSaveResult cond =
        inTx(
            txContext ->
                dao.saveLatestAspectConditional(opContext, txContext, latest, newAspect, 1));

    assertEquals(cond.getOutcome(), ConditionalWriteOutcome.SKIPPED_NOOP);
    assertFalse(cond.getUpdated().isPresent(), "a no-op must not report an updated row");
    assertFalse(cond.getInserted().isPresent(), "a no-op must not write a history row");
    assertEquals(storedVersion(urn), "1", "a no-op must leave the stored version unchanged");
    assertNull(
        dao.getAspect(opContext, urn, STATUS_ASPECT_NAME, 1L),
        "a no-op must not create a version-1 history row");
  }

  @Test
  public void saveLatestAspectConditional_conflict_whenVersionAdvancedUnderneath() {
    final String urn = "urn:li:corpuser:ol_conflict_" + shortId();
    final String smJsonV1 = RecordUtils.toJsonString(sysMeta("1"));
    final String contentJsonA = RecordUtils.toJsonString(new Status());
    seedVersion0Json(urn, contentJsonA, smJsonV1);

    // Snapshot of the version-0 row as first read (version "1"); this becomes the stale "latest".
    final SystemAspect staleLatest =
        EbeanSystemAspect.builder()
            .forUpdate(row(urn, contentJsonA, smJsonV1), opContext.getEntityRegistry());

    // A separate conditional writer advances the underlying row to version "2".
    final Optional<EntityAspect> advanced =
        inTx(
            txContext ->
                dao.updateAspectConditional(
                    opContext,
                    txContext,
                    newAspect(urn, new Status().setRemoved(true), sysMeta("2")),
                    "1"));
    assertTrue(advanced.isPresent(), "precondition: the advancing write should succeed");

    // Saving against the stale latest expects version "1" but the row is now at "2" -> CONFLICT.
    final ConditionalSaveResult cond =
        inTx(
            txContext ->
                dao.saveLatestAspectConditional(
                    opContext,
                    txContext,
                    staleLatest,
                    newAspect(urn, new Status(), sysMeta("2")),
                    1));

    assertEquals(cond.getOutcome(), ConditionalWriteOutcome.CONFLICT);
    assertFalse(cond.getUpdated().isPresent());
    assertEquals(
        storedVersion(urn), "2", "the conflicted save must not overwrite the winning write");
    assertTrue(storedRemoved(urn), "the winning write's value must survive the conflict");
  }

  @Test
  public void saveLatestAspectConditional_legacyNullVersion_fallsBackAndStampsVersion() {
    final String urn = "urn:li:corpuser:ol_legacy_" + shortId();
    // Legacy row: stored systemMetadata has NO version, so the CAS predicate could never match.
    final String legacySmJson = RecordUtils.toJsonString(sysMeta(null));
    final String contentJsonA = RecordUtils.toJsonString(new Status());
    seedVersion0Json(urn, contentJsonA, legacySmJson);

    final SystemAspect latest =
        EbeanSystemAspect.builder()
            .forUpdate(row(urn, contentJsonA, legacySmJson), opContext.getEntityRegistry());

    // A real change: falls back to last-writer-wins (UPDATED) and stamps version "1" onto the row.
    final ConditionalSaveResult cond =
        inTx(
            txContext ->
                dao.saveLatestAspectConditional(
                    opContext,
                    txContext,
                    latest,
                    newAspect(urn, new Status().setRemoved(true), sysMeta("1")),
                    1));

    assertEquals(cond.getOutcome(), ConditionalWriteOutcome.UPDATED);
    assertEquals(
        storedVersion(urn), "1", "the fallback write must stamp a version onto the legacy row");

    // Because a version is now stamped, a subsequent CAS on the matching version must succeed.
    final Optional<EntityAspect> cas =
        inTx(
            txContext ->
                dao.updateAspectConditional(
                    opContext, txContext, newAspect(urn, new Status(), sysMeta("2")), "1"));
    assertTrue(cas.isPresent(), "after the version is stamped, CAS on the matching version works");
    assertEquals(storedVersion(urn), "2");
  }

  @Test
  public void concurrentWriters_oneCasWinsOtherConflicts() throws Exception {
    final String urn = "urn:li:corpuser:ol_race_" + shortId();
    seedVersion0(urn, new Status(), sysMeta("1"));

    final CountDownLatch ready = new CountDownLatch(2);
    final CountDownLatch go = new CountDownLatch(1);
    final AtomicReference<Optional<EntityAspect>> resultA = new AtomicReference<>(Optional.empty());
    final AtomicReference<Optional<EntityAspect>> resultB = new AtomicReference<>(Optional.empty());
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread a = casWriter(urn, ready, go, resultA, failure);
    final Thread b = casWriter(urn, ready, go, resultB, failure);

    a.start();
    b.start();
    // Release both writers only once both have armed. They use a fixed expected version ("1"), so
    // there's nothing to capture — the gate just guarantees both threads are started before the
    // race, which is what makes the CAS collision deterministic. Capture readiness, then ALWAYS
    // release + join before asserting it, so a readiness timeout can't leave armed writers blocked
    // to race teardown / obscure the failure.
    final boolean bothArmed = ready.await(10, TimeUnit.SECONDS);
    go.countDown();
    a.join(15_000);
    b.join(15_000);
    assertTrue(bothArmed, "both writers must arm before release");

    assertNull(failure.get(), "concurrent CAS writers should not error");
    final int winners = (resultA.get().isPresent() ? 1 : 0) + (resultB.get().isPresent() ? 1 : 0);
    assertEquals(winners, 1, "exactly one concurrent CAS must win; the other must conflict");
    assertEquals(storedVersion(urn), "2", "the winning write must have advanced the row");
  }

  private Thread casWriter(
      String urn,
      CountDownLatch ready,
      CountDownLatch go,
      AtomicReference<Optional<EntityAspect>> result,
      AtomicReference<Throwable> failure) {
    return new Thread(
        () -> {
          try {
            ready.countDown();
            go.await(10, TimeUnit.SECONDS);
            try (Transaction tx = primaryDatabase.beginTransaction()) {
              final Optional<EntityAspect> r =
                  dao.updateAspectConditional(
                      opContext,
                      TransactionContext.empty(
                          tx, TransactionContext.DEFAULT_MAX_TRANSACTION_RETRY),
                      newAspect(urn, new Status().setRemoved(true), sysMeta("2")),
                      "1");
              tx.commit();
              result.set(r);
            }
          } catch (Throwable t) {
            failure.set(t);
          }
        });
  }

  // --- helpers -------------------------------------------------------------------------------

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

  private void seedVersion0Json(String urn, String metadataJson, String systemMetadataJson) {
    primaryDatabase.save(row(urn, metadataJson, systemMetadataJson));
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

  private static String shortId() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }
}
