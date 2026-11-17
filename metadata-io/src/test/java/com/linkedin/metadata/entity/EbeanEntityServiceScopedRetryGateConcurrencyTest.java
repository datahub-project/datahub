package com.linkedin.metadata.entity;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.codahale.metrics.MetricRegistry;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.entity.ebean.PassThroughScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.lock.HazelcastEntityWriteLock;
import com.linkedin.metadata.entity.retention.RetentionTestUtils;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Full-stack concurrency IT for the write gate: drives the REAL end-to-end ingest path ( {@link
 * EntityServiceImpl#ingestAspects} → {@code ingestAspectsToLocalDB} → {@code
 * runInTransactionWithRetry} → scoped retry → CAS) with a REAL Hazelcast {@link
 * HazelcastEntityWriteLock} injected via {@link EntityServiceImpl#setEntityWriteLock}, and with
 * {@code optimisticLockingEnabled=true} + {@code scopedRetryEnabled=true} + {@code
 * entityWriteLockBackend=hazelcast}. Contrast the DAO-level sibling {@code
 * EbeanOptimisticLockingDialectIT#writeGateSerializesConcurrentWritersWithoutCasThrash}, which
 * exercises the same gate one layer down.
 *
 * <p><b>Coverage.</b> N threads call {@code ingestAspects} on a single hot URN. Because the gate
 * serializes writers on that URN, each reads the fresh committed version under the lock and its CAS
 * succeeds first try: every write lands (no loss), and the final {@code SystemMetadata.version} ==
 * seed + N. Deterministic — convergence + accounting assertions only, no timing assertions.
 *
 * <p><b>Not covered here (documented):</b> this runs against the embedded H2 store (like the other
 * EntityService-level OL tests) rather than a Postgres/MySQL testcontainer, so it does NOT exercise
 * the real-dialect JSON CAS predicate. That is covered separately at the DAO layer by the
 * dialect-specific {@code EbeanOptimisticLockingPostgresIT} / {@code EbeanOptimisticLockingMysqlIT}
 * (which extend {@code EbeanOptimisticLockingDialectIT}). Keeping this test on H2 makes it
 * container-free and part of the default suite, so it always runs.
 */
public class EbeanEntityServiceScopedRetryGateConcurrencyTest {

  private static final AuditStamp TEST_AUDIT_STAMP = AspectGenerationUtils.createAuditStamp();

  private EbeanAspectDao aspectDao;
  private EntityServiceImpl entityService;
  private HazelcastInstance hazelcast;
  private OperationContext opContext;
  private SimpleMeterRegistry meterRegistry;

  public EbeanEntityServiceScopedRetryGateConcurrencyTest() throws EntityRegistryException {}

  @BeforeMethod
  public void setup() {
    EventProducer mockProducer = mock(EventProducer.class);
    UpdateIndicesService mockUpdateIndicesService = mock(UpdateIndicesService.class);

    Database server =
        EbeanTestUtils.createTestServer(
            EbeanEntityServiceScopedRetryGateConcurrencyTest.class.getSimpleName());

    // Scoped-retry + hazelcast-backend config, built via the Lombok builder so the test also
    // exercises the @Builder.Default lock-field defaults (acquireTimeout=10s, lease=300s).
    EbeanConfiguration config =
        EbeanConfiguration.builder()
            .optimisticLockingEnabled(true)
            .scopedRetryEnabled(true)
            .entityWriteLockBackend("hazelcast")
            .build();

    // Real metric registry so the test can PROVE the gate engaged (zero CAS conflicts), not just
    // that no writes were lost (which holds even with a NoOp gate, since scoped retry recovers).
    meterRegistry = new SimpleMeterRegistry();
    MetricUtils metricUtils = MetricUtils.builder().registry(meterRegistry).build();

    aspectDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            config,
            metricUtils,
            List.of(),
            null,
            /* optimisticLocking */ true);
    aspectDao.setWritable(true);

    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    entityService =
        new EntityServiceImpl(
            aspectDao,
            mockProducer,
            preProcessHooks,
            new EntityServiceConfiguration()
                .setAlwaysEmitChangeLog(false)
                .setCdcModeChangeLog(false)
                .setEnableBrowseV2(true),
            null);
    entityService.setUpdateIndicesService(mockUpdateIndicesService);

    // Real embedded Hazelcast gate, isolated (no cluster join), injected as EntityServiceImpl does
    // in production via EntityWriteLockFactory.
    hazelcast = isolatedHazelcast();
    entityService.setEntityWriteLock(
        new HazelcastEntityWriteLock(hazelcast, "it-scoped-gate", 30, 300));

    EbeanRetentionService<ChangeItemImpl> retentionService =
        new EbeanRetentionService<>(
            entityService,
            server,
            1000,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server),
            RetentionTestUtils.systemEntityClient(
                entityService, mockProducer, mock(MetricUtils.class)));
    entityService.setRetentionService(retentionService);

    opContext =
        TestOperationContexts.systemContext(
            null,
            null,
            null,
            () -> TestOperationContexts.defaultEntityRegistry(),
            () ->
                RetrieverContext.builder()
                    .aspectRetriever(
                        EntityServiceAspectRetriever.builder()
                            .entityService(entityService)
                            .entityRegistry(TestOperationContexts.defaultEntityRegistry())
                            .build())
                    .cachingAspectRetriever(
                        TestOperationContexts.emptyActiveUsersAspectRetriever(
                            () -> TestOperationContexts.defaultEntityRegistry()))
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .build(),
            null,
            ctx ->
                ((EntityServiceAspectRetriever) ctx.getAspectRetriever())
                    .setSystemOperationContext(ctx),
            null);

    assertTrue(aspectDao.isOptimisticLockingEnabled());
    assertTrue(aspectDao.isScopedRetryEnabled());
  }

  @AfterMethod
  public void cleanup() {
    if (hazelcast != null) {
      hazelcast.shutdown();
    }
    EbeanTestUtils.shutdownDatabaseFromAspectDao(aspectDao);
  }

  @Test
  public void gatedConcurrentIngestAllWritesLandNoLostUpdates() throws Exception {
    final int writers = 6;
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olGateFullStack");
    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());

    // Seed version-0 row at SystemMetadata.version=1.
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(
            Pair.of(
                aspectName,
                (RecordTemplate) AspectGenerationUtils.createCorpUserInfo("seed@test.com"))),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    final AtomicInteger successes = new AtomicInteger();
    final AtomicReference<Throwable> firstError = new AtomicReference<>();
    // Release all writers at once to maximize contention on the hot URN.
    final CountDownLatch ready = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(writers);
    final ExecutorService pool = Executors.newFixedThreadPool(writers);

    try {
      for (int i = 0; i < writers; i++) {
        // Distinct payload per writer so every write is a real change (a duplicate would
        // SKIPPED_NOOP and not bump the version, breaking the seed+N accounting).
        final String email = "writer" + i + "@test.com";
        pool.submit(
            () -> {
              try {
                ready.await();
                entityService.ingestAspects(
                    opContext,
                    urn,
                    List.of(
                        Pair.of(
                            aspectName,
                            (RecordTemplate) AspectGenerationUtils.createCorpUserInfo(email))),
                    TEST_AUDIT_STAMP,
                    AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));
                successes.incrementAndGet();
              } catch (Throwable t) {
                firstError.compareAndSet(null, t);
              } finally {
                done.countDown();
              }
            });
      }

      ready.countDown();
      assertTrue(done.await(120, TimeUnit.SECONDS), "gated concurrent ingest timed out");
    } finally {
      pool.shutdownNow();
    }

    assertNull(firstError.get(), "gated concurrent ingest failed: " + firstError.get());
    assertEquals(successes.get(), writers, "every gated writer must apply exactly once (no loss)");

    // seed(v1) + one version bump per landed write == 1 + writers proves zero lost updates: the
    // gate serialized the writers so each CAS applied against the fresh committed version.
    EntityAspect stored = aspectDao.getAspect(opContext, urn.toString(), aspectName, 0L);
    assertNotNull(stored);
    SystemMetadata storedMeta = SystemMetadataUtils.parseSystemMetadata(stored.getSystemMetadata());
    assertEquals(
        storedMeta.getVersion(),
        String.valueOf(writers + 1),
        "final SystemMetadata.version must equal seed(1) + one bump per writer (no lost updates)");

    // Gate-engagement proof (distinct from no-loss): because the gate serializes writers on the
    // same
    // (urn, aspect), each reads the fresh committed version under the lock and its CAS succeeds
    // first
    // try → ZERO CAS conflicts. A NoOp or broken gate wiring would let writers collide and the
    // DAO's
    // CAS-conflict counter would be > 0 (scoped retry would still recover, so the no-loss
    // assertions
    // above alone cannot catch that). This makes a broken gate a hard test failure.
    Counter conflicts =
        meterRegistry
            .find(MetricRegistry.name(EbeanAspectDao.class, "optimistic_lock_update_conflict"))
            .counter();
    assertTrue(
        conflicts == null || conflicts.count() == 0.0,
        "engaged write gate must serialize same-(urn,aspect) writers → zero CAS conflicts; got "
            + (conflicts == null ? 0.0 : conflicts.count())
            + " (broken / NoOp gate wiring?)");
  }

  private static HazelcastInstance isolatedHazelcast() {
    Config config = new Config();
    config.setInstanceName("scoped-gate-it-hz-" + UUID.randomUUID());
    config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
    config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
    return Hazelcast.newHazelcastInstance(config);
  }
}
