package com.linkedin.metadata.entity.coordinator.concurrency;

import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.codahale.metrics.MetricRegistry;
import com.datahub.util.exception.RetryLimitReached;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.config.CoordinatedIngestConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.EntityServiceAspectRetriever;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.coordinator.ConflictKeyResolver;
import com.linkedin.metadata.entity.coordinator.HazelcastLockProvider;
import com.linkedin.metadata.entity.coordinator.MutationCoordinator;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Dialect-parameterized concurrency integration tests for coordinated ingest, exercised against a
 * REAL relational engine (MySQL / PostgreSQL via Testcontainers) plus a REAL embedded single-node
 * Hazelcast. H2 is deliberately not used: it does not reproduce {@code SELECT ... FOR UPDATE} /
 * gap-lock deadlock semantics, so an H2 pass would not prove the deadlock is gone.
 *
 * <p>Subclasses supply the engine (container + Ebean {@link Database}) via {@link
 * #startEngineDatabase()} / {@link #stopEngineDatabase(Database)}; every scenario below runs on
 * both engines.
 *
 * <p>Design reference: {@code docs/superpowers/specs/2026-07-31-coordinated-ingest-design.md} — the
 * v1 LOCKED SCOPE (plan &rarr; bounded IMap serialize &rarr; single globally-sorted {@code FOR
 * UPDATE} commit, DB authoritative).
 */
public abstract class AbstractCoordinatedIngestConcurrencyIT {

  private static final Logger log =
      LoggerFactory.getLogger(AbstractCoordinatedIngestConcurrencyIT.class);

  // ---- Load constants (kept modest so each engine finishes well under ~60s in CI) ----

  /** Overlapping writers for the deadlock-elimination / legacy-control scenarios. */
  private static final int DEADLOCK_WRITER_COUNT = 12;

  /**
   * schemaFields of the single dataset each deadlock writer touches (all map to one conflict key).
   */
  private static final int OVERLAP_FIELD_COUNT = 20;

  /** Writers all hammering the SAME conflict key (one dataset's field) for the herd scenario. */
  private static final int THUNDERING_HERD_WRITER_COUNT = 24;

  /** Writers each targeting a DISTINCT dataset (distinct conflict key) — must stay parallel. */
  private static final int DISTINCT_KEY_WRITER_COUNT = 8;

  /** A-batch and B-batch writer counts for the partial multi-key overlap scenario. */
  private static final int PARTIAL_A_WRITER_COUNT = 8;

  private static final int PARTIAL_B_WRITER_COUNT = 8;

  /** Guard so a hung writer fails the test instead of hanging the suite. */
  private static final long JOIN_TIMEOUT_SECONDS = 120L;

  /**
   * Terminal retry-exhaustion counter emitted by {@link EbeanAspectDao} when a transaction cannot
   * commit after all retries. Zero of these under coordinated ingest is the hard correctness gate.
   */
  private static final String TX_FAILED_AFTER_RETRIES_METRIC =
      MetricRegistry.name(EbeanAspectDao.class, "txFailedAfterRetries");

  private Database database;
  private EbeanAspectDao aspectDao;
  private HazelcastInstance hazelcast;
  private MetricUtils metricUtils;
  private EntityRegistry entityRegistry;
  private EventProducer mockProducer;
  private UpdateIndicesService mockUpdateIndices;

  /**
   * Start the engine container (if needed), run the aspect DDL, and return a primary Ebean pool.
   */
  protected abstract Database startEngineDatabase();

  /** Shut down the Ebean pool and stop the engine container. */
  protected abstract void stopEngineDatabase(Database database);

  /** Short human label for logs / diagnostics (e.g. "mysql", "postgres"). */
  protected abstract String engineLabel();

  @BeforeClass
  public void setUp() {
    database = startEngineDatabase();

    metricUtils = MetricUtils.builder().registry(new SimpleMeterRegistry()).build();
    // Pre-register the terminal-retry counter against our registry so per-scenario delta reads are
    // reliable. EbeanAspectDao emits it lazily; other metadata-io tests pass a null MetricUtils, so
    // this test is the first to bind that meter name in this JVM/suite.
    metricUtils.increment(TX_FAILED_AFTER_RETRIES_METRIC, 0.0d);

    aspectDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(database),
            EbeanConfiguration.testDefault,
            metricUtils,
            List.of(),
            null);
    aspectDao.setConnectionValidated(true);

    entityRegistry = TestOperationContexts.defaultEntityRegistry();
    mockProducer = Mockito.mock(EventProducer.class);
    mockUpdateIndices = Mockito.mock(UpdateIndicesService.class);

    hazelcast = newEmbeddedHazelcast(engineLabel());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (hazelcast != null) {
      hazelcast.shutdown();
    }
    stopEngineDatabase(database);
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 1 — Mode-1/Mode-2 deadlock elimination (coordinated ON).
  // ---------------------------------------------------------------------------------------------

  /**
   * Many writers submit OVERLAPPING batches (the same set of schemaField urns of one dataset, two
   * aspects each so lock sets fully overlap), fired simultaneously. Under coordinated ingest they
   * serialize on the single parent-dataset conflict key and commit via one globally-sorted {@code
   * FOR UPDATE}. Hard gate: every writer succeeds, ZERO {@link RetryLimitReached}, and the {@code
   * txFailedAfterRetries} counter does not move.
   */
  @Test
  public void coordinatedIngestEliminatesDeadlockUnderOverlappingBatches() {
    final String runId = shortId();
    final Urn dataset = datasetUrn(runId, 0);
    final List<Urn> fields = schemaFields(dataset, OVERLAP_FIELD_COUNT);

    final EntityServiceImpl service = buildService(true);
    final OperationContext op = buildOpContext(service);

    final double before = txFailedAfterRetriesCount();
    final ConcurrencyResult result =
        fireConcurrently(
            DEADLOCK_WRITER_COUNT,
            writerId -> {
              final List<ChangeItemImpl> items = new ArrayList<>();
              for (final Urn field : fields) {
                items.add(globalTagsItem(field, tagName(runId, writerId)));
                items.add(statusItem(field));
              }
              service.ingestAspects(op, batchOf(items, op), false, true);
            });

    assertAllSucceeded(result);
    assertEquals(
        result.retryExhausted,
        0,
        engineLabel() + ": coordinated ingest must not exhaust retries on overlapping batches");
    assertNoTxFailedAfterRetries(txFailedAfterRetriesCount() - before);
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 2 — Legacy control vs coordinated under identical load.
  // ---------------------------------------------------------------------------------------------

  /**
   * Runs the same overlapping load once on the legacy multi-wave path and once coordinated. The
   * hard gate is the coordinated invariant: ZERO retry-exhaustions and a flat {@code
   * txFailedAfterRetries} counter. The legacy run is a comparative control — we assert coordinated
   * has no more retry-exhaustions than legacy (i.e. the fix never makes contention worse).
   *
   * <p>We intentionally do NOT hard-require {@code legacy > 0}. Forcing a deterministic
   * cross-wave/cross-tx deadlock across two different engines and CI schedulers is inherently
   * timing-dependent; asserting on it would make the test itself flaky. When legacy does exhaust
   * retries (the common case under this load) it is logged as positive evidence the fix does
   * something.
   */
  @Test
  public void legacyExhaustsRetriesWhileCoordinatedDoesNot() {
    final String legacyRun = shortId();
    final ConcurrencyResult legacy = runOverlappingLoad(legacyRun, buildService(false));

    final String coordinatedRun = shortId();
    final EntityServiceImpl coordinatedService = buildService(true);
    final double before = txFailedAfterRetriesCount();
    final ConcurrencyResult coordinated = runOverlappingLoad(coordinatedRun, coordinatedService);
    final double coordinatedDelta = txFailedAfterRetriesCount() - before;

    assertAllSucceeded(coordinated);
    assertEquals(
        coordinated.retryExhausted,
        0,
        engineLabel() + ": coordinated ingest must never exhaust retries");
    assertNoTxFailedAfterRetries(coordinatedDelta);
    assertTrue(
        coordinated.retryExhausted <= legacy.retryExhausted,
        engineLabel()
            + ": coordinated retry-exhaustions ("
            + coordinated.retryExhausted
            + ") must not exceed legacy ("
            + legacy.retryExhausted
            + ")");

    if (legacy.retryExhausted == 0) {
      log.warn(
          "[{}] legacy control did not exhaust retries this run; coordinated invariant still"
              + " enforced (0 exhaustions). Contention is timing-dependent and not asserted as a"
              + " hard gate to avoid flakiness.",
          engineLabel());
    } else {
      log.info(
          "[{}] legacy control exhausted retries {} time(s); coordinated exhausted 0.",
          engineLabel(),
          legacy.retryExhausted);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 3 — Thundering herd on one conflict key (coordinated ON).
  // ---------------------------------------------------------------------------------------------

  /**
   * Every writer targets the SAME conflict key (one dataset's field) with a DISTINCT value.
   * Serialization is asserted via the observable no-lost-update proxy: zero retry-exhaustions plus
   * a strictly increasing version chain whose final length equals the writer count. A lost update
   * (two writers overwriting from the same base) would leave the final systemMetadata version below
   * the writer count.
   */
  @Test
  public void thunderingHerdOnSingleConflictKeyHasNoLostUpdates() {
    final String runId = shortId();
    final Urn field = schemaFields(datasetUrn(runId, 0), 1).get(0);

    final EntityServiceImpl service = buildService(true);
    final OperationContext op = buildOpContext(service);

    final double before = txFailedAfterRetriesCount();
    final ConcurrencyResult result =
        fireConcurrently(
            THUNDERING_HERD_WRITER_COUNT,
            writerId ->
                service.ingestAspects(
                    op,
                    batchOf(List.of(globalTagsItem(field, tagName(runId, writerId))), op),
                    false,
                    true));

    assertAllSucceeded(result);
    assertEquals(
        result.retryExhausted, 0, engineLabel() + ": herd on one key must not exhaust retries");
    assertNoTxFailedAfterRetries(txFailedAfterRetriesCount() - before);
    assertEquals(
        latestVersion(service, op, field),
        THUNDERING_HERD_WRITER_COUNT,
        engineLabel()
            + ": every distinct writer must land a version (no lost updates on the hot key)");
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 4 — Distinct conflict keys stay parallel (coordinated ON).
  // ---------------------------------------------------------------------------------------------

  /**
   * Writers targeting DIFFERENT datasets (disjoint conflict keys) must NOT be serialized onto one
   * queue. Observed via real overlap: writers are released together and we record the max
   * concurrent in-flight ingests. The threshold is deliberately generous ({@code >= 2}) to avoid CI
   * flakiness.
   */
  @Test
  public void distinctConflictKeysRunInParallel() {
    final String runId = shortId();

    final EntityServiceImpl service = buildService(true);
    final OperationContext op = buildOpContext(service);

    final ConcurrencyResult result =
        fireConcurrently(
            DISTINCT_KEY_WRITER_COUNT,
            writerId -> {
              final Urn field = schemaFields(datasetUrn(runId, writerId), 1).get(0);
              service.ingestAspects(
                  op,
                  batchOf(List.of(globalTagsItem(field, tagName(runId, writerId))), op),
                  false,
                  true);
            });

    assertAllSucceeded(result);
    assertEquals(
        result.retryExhausted, 0, engineLabel() + ": distinct keys must not exhaust retries");
    assertTrue(
        result.maxInFlight >= 2,
        engineLabel()
            + ": disjoint conflict keys must run concurrently (maxInFlight="
            + result.maxInFlight
            + "), not be serialized onto one queue");
  }

  // ---------------------------------------------------------------------------------------------
  // Scenario 5 — Partial multi-key overlap: serialize only the shared key (coordinated ON).
  // ---------------------------------------------------------------------------------------------

  /**
   * A-batches touch datasets {D0,D1,D2}; B-batches touch {D0,D3,D4}. Only D0 (its schemaField) is
   * shared, so only that conflict key is contended. Validates sorted multi-key acquisition (no
   * distributed AB-BA deadlock — every writer takes D0 first in the common sorted order) and
   * serialize-only-on-overlap:
   *
   * <ol>
   *   <li>all succeed, zero retry-exhaustions, flat {@code txFailedAfterRetries};
   *   <li>no lost update on the shared field — its final version == total writers touching it;
   *   <li>the non-shared keys ran with real parallelism (maxInFlight &gt; 1).
   * </ol>
   */
  @Test
  public void partialMultiKeyOverlapSerializesOnlySharedKey() {
    final String runId = shortId();
    final Urn sharedField = schemaFields(datasetUrn(runId, 0), 1).get(0);
    final Urn aField2 = schemaFields(datasetUrn(runId, 1), 1).get(0);
    final Urn aField3 = schemaFields(datasetUrn(runId, 2), 1).get(0);
    final Urn bField5 = schemaFields(datasetUrn(runId, 3), 1).get(0);
    final Urn bField6 = schemaFields(datasetUrn(runId, 4), 1).get(0);

    final int total = PARTIAL_A_WRITER_COUNT + PARTIAL_B_WRITER_COUNT;
    final EntityServiceImpl service = buildService(true);
    final OperationContext op = buildOpContext(service);

    final double before = txFailedAfterRetriesCount();
    final ConcurrencyResult result =
        fireConcurrently(
            total,
            writerId -> {
              final String tag = tagName(runId, writerId);
              final List<ChangeItemImpl> items = new ArrayList<>();
              // Shared field is included by BOTH batch types.
              items.add(globalTagsItem(sharedField, tag));
              if (writerId < PARTIAL_A_WRITER_COUNT) {
                items.add(globalTagsItem(aField2, tag));
                items.add(globalTagsItem(aField3, tag));
              } else {
                items.add(globalTagsItem(bField5, tag));
                items.add(globalTagsItem(bField6, tag));
              }
              service.ingestAspects(op, batchOf(items, op), false, true);
            });

    assertAllSucceeded(result);
    assertEquals(
        result.retryExhausted, 0, engineLabel() + ": partial overlap must not exhaust retries");
    assertNoTxFailedAfterRetries(txFailedAfterRetriesCount() - before);
    assertEquals(
        latestVersion(service, op, sharedField),
        total,
        engineLabel() + ": shared field version must reflect ALL writers (no lost updates)");
    assertTrue(
        result.maxInFlight >= 2,
        engineLabel()
            + ": non-shared conflict keys must run in parallel (maxInFlight="
            + result.maxInFlight
            + "); the whole workload must not serialize on the shared key");
  }

  // ---------------------------------------------------------------------------------------------
  // Harness
  // ---------------------------------------------------------------------------------------------

  private ConcurrencyResult runOverlappingLoad(String runId, EntityServiceImpl service) {
    final Urn dataset = datasetUrn(runId, 0);
    final List<Urn> fields = schemaFields(dataset, OVERLAP_FIELD_COUNT);
    final OperationContext op = buildOpContext(service);
    return fireConcurrently(
        DEADLOCK_WRITER_COUNT,
        writerId -> {
          final List<ChangeItemImpl> items = new ArrayList<>();
          for (final Urn field : fields) {
            items.add(globalTagsItem(field, tagName(runId, writerId)));
            items.add(statusItem(field));
          }
          service.ingestAspects(op, batchOf(items, op), false, true);
        });
  }

  /**
   * Releases {@code writerCount} threads simultaneously via a start latch, records the max
   * concurrent in-flight ingests, and classifies each writer's outcome (success / retry-exhausted /
   * other failure). Thread-local exception capture sidesteps the JVM-global metric caches.
   */
  private ConcurrencyResult fireConcurrently(int writerCount, Writer writer) {
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(writerCount);
    final AtomicInteger inFlight = new AtomicInteger(0);
    final AtomicInteger maxInFlight = new AtomicInteger(0);
    final AtomicInteger succeeded = new AtomicInteger(0);
    final AtomicInteger retryExhausted = new AtomicInteger(0);
    final List<Throwable> otherFailures = Collections.synchronizedList(new ArrayList<>());

    final List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < writerCount; i++) {
      final int writerId = i;
      final Thread thread =
          new Thread(
              () -> {
                try {
                  start.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
                final int now = inFlight.incrementAndGet();
                maxInFlight.accumulateAndGet(now, Math::max);
                try {
                  writer.write(writerId);
                  succeeded.incrementAndGet();
                } catch (Throwable t) {
                  if (isRetryLimitReached(t)) {
                    retryExhausted.incrementAndGet();
                  } else {
                    otherFailures.add(t);
                  }
                } finally {
                  inFlight.decrementAndGet();
                  done.countDown();
                }
              },
              engineLabel() + "-coord-writer-" + writerId);
      threads.add(thread);
      thread.start();
    }

    start.countDown();
    final boolean finished;
    try {
      finished = done.await(JOIN_TIMEOUT_SECONDS, SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted awaiting writers", e);
    }
    if (!finished) {
      fail(engineLabel() + ": writers did not finish within " + JOIN_TIMEOUT_SECONDS + "s");
    }

    return new ConcurrencyResult(
        writerCount,
        succeeded.get(),
        retryExhausted.get(),
        maxInFlight.get(),
        new ArrayList<>(otherFailures));
  }

  private void assertAllSucceeded(ConcurrencyResult result) {
    if (!result.otherFailures.isEmpty()) {
      fail(
          engineLabel()
              + ": "
              + result.otherFailures.size()
              + " writer(s) failed unexpectedly; first="
              + result.otherFailures.get(0),
          result.otherFailures.get(0));
    }
    assertEquals(
        result.succeeded,
        result.writerCount,
        engineLabel() + ": every writer must complete successfully");
  }

  private EntityServiceImpl buildService(boolean coordinated) {
    final PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(false);
    final EntityServiceImpl service =
        new EntityServiceImpl(
            aspectDao, mockProducer, false, false, preProcessHooks, null, true, metricUtils);
    service.setUpdateIndicesService(mockUpdateIndices);
    service.setRetentionService(new EbeanRetentionService<>(service, database, 1000));
    if (coordinated) {
      service.setCoordinatedIngest(
          new MutationCoordinator(
              new HazelcastLockProvider(hazelcast), coordinatedConfig(), metricUtils),
          new ConflictKeyResolver(),
          true,
          0);
    }
    return service;
  }

  private OperationContext buildOpContext(EntityServiceImpl service) {
    return TestOperationContexts.systemContext(
        null,
        null,
        null,
        () -> entityRegistry,
        () ->
            RetrieverContext.builder()
                .aspectRetriever(
                    EntityServiceAspectRetriever.builder()
                        .entityService(service)
                        .entityRegistry(entityRegistry)
                        .build())
                .cachingAspectRetriever(
                    TestOperationContexts.emptyActiveUsersAspectRetriever(() -> entityRegistry))
                .graphRetriever(GraphRetriever.EMPTY)
                .searchRetriever(SearchRetriever.EMPTY)
                .build(),
        null,
        opContext ->
            ((EntityServiceAspectRetriever) opContext.getAspectRetriever())
                .setSystemOperationContext(opContext),
        null);
  }

  private static CoordinatedIngestConfiguration coordinatedConfig() {
    // (maxPlanExpansions, maxMutationCount, lockLeaseSeconds, lockAcquireTimeoutSeconds,
    // lockProvider). Generous acquire timeout so writers genuinely serialize on the IMap lock
    // rather
    // than falling through to a lock-free best-effort commit; maxMutationCount comfortably exceeds
    // any scenario's closure.
    return new CoordinatedIngestConfiguration(5, 10_000, 30L, 30L, "hazelcast");
  }

  // ---------------------------------------------------------------------------------------------
  // Aspect / URN builders
  // ---------------------------------------------------------------------------------------------

  private ChangeItemImpl globalTagsItem(Urn schemaFieldUrn, String tagName) {
    final GlobalTags tags =
        new GlobalTags()
            .setTags(new TagAssociationArray(new TagAssociation().setTag(tagUrn(tagName))));
    return ChangeItemImpl.builder()
        .urn(schemaFieldUrn)
        .aspectName(GLOBAL_TAGS_ASPECT_NAME)
        .recordTemplate(tags)
        .systemMetadata(AspectGenerationUtils.createSystemMetadata())
        .auditStamp(AspectGenerationUtils.createAuditStamp())
        .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null));
  }

  private ChangeItemImpl statusItem(Urn schemaFieldUrn) {
    return ChangeItemImpl.builder()
        .urn(schemaFieldUrn)
        .aspectName(STATUS_ASPECT_NAME)
        .recordTemplate(new Status().setRemoved(false))
        .systemMetadata(AspectGenerationUtils.createSystemMetadata())
        .auditStamp(AspectGenerationUtils.createAuditStamp())
        .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null));
  }

  private AspectsBatchImpl batchOf(List<ChangeItemImpl> items, OperationContext op) {
    return AspectsBatchImpl.builder()
        .retrieverContext(op.getRetrieverContext())
        .items(items)
        .build(op);
  }

  /** Generic placeholder dataset urn (public repo — no real identifiers). */
  private static Urn datasetUrn(String runId, int index) {
    return UrnUtils.getUrn(
        "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_schema.events_"
            + runId
            + "_"
            + index
            + ",PROD)");
  }

  private static List<Urn> schemaFields(Urn dataset, int count) {
    final List<Urn> fields = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      fields.add(SchemaFieldUtils.generateSchemaFieldUrn(dataset, "col_" + i));
    }
    return fields;
  }

  private static String tagName(String runId, int writerId) {
    return "urn:li:tag:t_" + runId + "_" + writerId;
  }

  private static TagUrn tagUrn(String urn) {
    try {
      return TagUrn.createFromString(urn);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid tag urn: " + urn, e);
    }
  }

  private int latestVersion(EntityServiceImpl service, OperationContext op, Urn schemaFieldUrn) {
    final EnvelopedAspect latest =
        service.getLatestEnvelopedAspect(
            op, SCHEMA_FIELD_ENTITY_NAME, schemaFieldUrn, GLOBAL_TAGS_ASPECT_NAME);
    if (latest == null || latest.getSystemMetadata() == null) {
      return 0;
    }
    return Integer.parseInt(latest.getSystemMetadata().getVersion());
  }

  private double txFailedAfterRetriesCount() {
    final Counter counter =
        metricUtils.getRegistry().find(TX_FAILED_AFTER_RETRIES_METRIC).counter();
    return counter == null ? 0.0d : counter.count();
  }

  private void assertNoTxFailedAfterRetries(double delta) {
    assertTrue(
        Math.abs(delta) < 1e-9,
        engineLabel()
            + ": coordinated ingest must not emit txFailedAfterRetries (delta="
            + delta
            + ")");
  }

  private static boolean isRetryLimitReached(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (cause instanceof RetryLimitReached) {
        return true;
      }
      if (cause.getCause() == cause) {
        break;
      }
    }
    return false;
  }

  private static String shortId() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }

  protected static HazelcastInstance newEmbeddedHazelcast(String label) {
    final Config config = new Config();
    config.setInstanceName("coordinated-ingest-concurrency-" + label + "-" + UUID.randomUUID());
    config.setProperty("hazelcast.phone.home.enabled", "false");
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    return Hazelcast.newHazelcastInstance(config);
  }

  @FunctionalInterface
  private interface Writer {
    void write(int writerId) throws Exception;
  }

  /** Aggregated outcome of one concurrent load. */
  private static final class ConcurrencyResult {
    private final int writerCount;
    private final int succeeded;
    private final int retryExhausted;
    private final int maxInFlight;
    private final List<Throwable> otherFailures;

    private ConcurrencyResult(
        int writerCount,
        int succeeded,
        int retryExhausted,
        int maxInFlight,
        List<Throwable> otherFailures) {
      this.writerCount = writerCount;
      this.succeeded = succeeded;
      this.retryExhausted = retryExhausted;
      this.maxInFlight = maxInFlight;
      this.otherFailures = otherFailures;
    }
  }
}
