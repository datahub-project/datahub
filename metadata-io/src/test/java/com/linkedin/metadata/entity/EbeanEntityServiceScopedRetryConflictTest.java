package com.linkedin.metadata.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.entity.ebean.PassThroughScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.retention.RetentionTestUtils;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Deterministic proof that the Stage-2 <b>scoped-retry</b> branch of {@link
 * EntityServiceImpl#ingestAspects} actually executes under a real CAS conflict, and does the
 * scoped-retry-specific bookkeeping the gate concurrency test cannot observe.
 *
 * <p><b>Why a separate test.</b> {@link EbeanEntityServiceScopedRetryGateConcurrencyTest} runs the
 * real path but injects a Hazelcast write gate that serializes writers to <em>zero</em> CAS
 * conflicts, so first-attempt success and scoped-retry are indistinguishable there — it can never
 * prove the retry loop fired. {@link EntityServiceImplScopedRetryTest} only unit-tests the pure
 * decision helpers ({@code branchScopedRecompute} / {@code committedKeysOf} / {@code
 * isAlreadyCommitted} / {@code filterItemsForRecompute}) in isolation. This test wires the whole
 * {@link EntityServiceImpl} ingest path with optimistic locking + scoped retry enabled and <b>no
 * write gate</b> ({@code entityWriteLockBackend=none}, {@link EntityServiceImpl#setEntityWriteLock}
 * never called → {@code NoOpEntityWriteLock}), then <b>forces exactly one CAS conflict</b> on one
 * aspect's first persist attempt and lets everything else run for real against embedded H2.
 *
 * <p><b>How the conflict is forced deterministically.</b> The single CAS seam is {@code
 * EbeanAspectDao.updateAspectConditional}, which returns {@code Optional.empty()} when the
 * version-predicated {@code UPDATE} matches zero rows (a lost CAS race). We spy the real DAO and
 * make that one method return {@code Optional.empty()} on the <em>first</em> call for {@code
 * corpUserInfo} — without touching the DB — so the code takes the CONFLICT-as-data path exactly as
 * a genuine lost race would; every other call (including {@code corpUserInfo}'s retry) delegates to
 * the real method. No threads, latches, sleeps, or timing assertions.
 *
 * <p>The batch writes two aspects on one URN: {@code corpUserInfo} (forced conflict on pass 1) and
 * a sibling {@code status} (commits on pass 1). This is the exact shape that exercises the
 * cross-pass double-commit guard: the scoped sub-batch is scoped by URN, so the retry re-includes
 * the already-committed {@code status} sibling, and only the {@code committedKeys} guard stops it
 * from being persisted (and MCL-emitted) twice.
 */
public class EbeanEntityServiceScopedRetryConflictTest {

  private static final AuditStamp TEST_AUDIT_STAMP = AspectGenerationUtils.createAuditStamp();
  private static final String CORP_USER_INFO_ASPECT =
      AspectGenerationUtils.getAspectName(new CorpUserInfo());
  private static final String STATUS_ASPECT = AspectGenerationUtils.getAspectName(new Status());

  private EbeanAspectDao aspectDao;
  private EntityServiceImpl entityService;
  private EventProducer mockProducer;
  private MetricUtils spyMetrics;
  private OperationContext opContext;

  public EbeanEntityServiceScopedRetryConflictTest() throws EntityRegistryException {}

  @BeforeMethod
  public void setup() {
    mockProducer = mock(EventProducer.class);
    UpdateIndicesService mockUpdateIndicesService = mock(UpdateIndicesService.class);

    Database server =
        EbeanTestUtils.createTestServer(
            EbeanEntityServiceScopedRetryConflictTest.class.getSimpleName());

    // OL + scoped retry, no write gate: default entityWriteLockBackend="none" and we never call
    // setEntityWriteLock, so the NoOpEntityWriteLock is used and a CAS conflict is reachable.
    EbeanConfiguration config =
        EbeanConfiguration.builder()
            .optimisticLockingEnabled(true)
            .scopedRetryEnabled(true)
            .build();

    // Spy the real DAO so the entire path runs against H2 and only the one CAS seam is intercepted.
    aspectDao =
        spy(
            new EbeanAspectDao(
                PrimaryStorageTestUtils.ebeanResolver(server),
                config,
                null,
                List.of(),
                null,
                /* optimisticLocking */ true));
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
    // Keep only current version so history-insert races are out of scope (matches the sibling
    // EntityService-level OL tests).
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

    // Real MetricUtils (SimpleMeterRegistry) wrapped in a spy so scoped-retry metric increments can
    // be verified while all real metric behavior is preserved. Injected via a per-test
    // SystemTelemetryContext so counts are isolated from the shared SystemTelemetryContext.TEST.
    spyMetrics = spy(MetricUtils.builder().registry(new SimpleMeterRegistry()).build());
    SystemTelemetryContext telemetry =
        SystemTelemetryContext.TEST.toBuilder().metricUtils(spyMetrics).build();

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
            null,
            null,
            () -> telemetry);

    assertTrue(aspectDao.isOptimisticLockingEnabled());
    assertTrue(aspectDao.isScopedRetryEnabled());
  }

  @AfterMethod
  public void cleanup() {
    EbeanTestUtils.shutdownDatabaseFromAspectDao(aspectDao);
  }

  @Test
  public void scopedRetryRecomputesOnlyConflictedBranchAndSkipsCommittedSibling() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olScopedRetryConflict");

    // Seed both aspects at SystemMetadata.version=1. Seeding inserts (no existing row), so it never
    // calls updateAspectConditional — the forced-conflict stub is installed afterwards, and the
    // updateAspectConditional call counts below reflect only the conflicting batch.
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(
            com.linkedin.util.Pair.of(
                CORP_USER_INFO_ASPECT,
                (RecordTemplate) AspectGenerationUtils.createCorpUserInfo("seed@test.com")),
            com.linkedin.util.Pair.of(
                STATUS_ASPECT, (RecordTemplate) new Status().setRemoved(false))),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Drop seed MCLs so the post-batch MCL assertions count only the conflicting batch.
    reset(mockProducer);

    // Force exactly one CAS conflict: the FIRST updateAspectConditional for corpUserInfo returns
    // empty (zero rows updated == lost CAS race) WITHOUT touching the DB, so the row stays at the
    // seeded version and the real retry CAS then matches and commits. Every other call — the
    // sibling status on pass 1 and corpUserInfo's retry — delegates to the real method.
    final AtomicInteger corpUserInfoUpdateCalls = new AtomicInteger();
    doAnswer(
            invocation -> {
              SystemAspect newAspect = invocation.getArgument(2);
              if (newAspect != null
                  && CORP_USER_INFO_ASPECT.equals(newAspect.getAspectName())
                  && corpUserInfoUpdateCalls.getAndIncrement() == 0) {
                return Optional.empty();
              }
              return invocation.callRealMethod();
            })
        .when(aspectDao)
        .updateAspectConditional(any(), any(), any(), any());

    // Conflicting batch: new values for BOTH aspects on the same URN, in one transaction.
    CorpUserInfo newInfo = AspectGenerationUtils.createCorpUserInfo("writer@test.com");
    Status newStatus = new Status().setRemoved(true);
    AspectsBatchImpl batch =
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(
                List.of(
                    ChangeItemImpl.builder()
                        .urn(urn)
                        .aspectName(CORP_USER_INFO_ASPECT)
                        .recordTemplate(newInfo)
                        .systemMetadata(
                            AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP))
                        .auditStamp(TEST_AUDIT_STAMP)
                        .build(opContext.getAspectRetriever()),
                    ChangeItemImpl.builder()
                        .urn(urn)
                        .aspectName(STATUS_ASPECT)
                        .recordTemplate(newStatus)
                        .systemMetadata(
                            AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP))
                        .auditStamp(TEST_AUDIT_STAMP)
                        .build(opContext.getAspectRetriever())))
            .build(opContext);

    entityService.ingestAspects(opContext, batch, true, true);

    // (1) Scoped retry FIRED exactly once — the branch the gate test can never reach.
    verify(spyMetrics, times(1))
        .increment(EntityServiceImpl.class, "optimistic_lock_scoped_retry", 1.0);
    // (2) Only ONE URN's branch was recomputed on the retry (not the whole batch).
    verify(spyMetrics, times(1))
        .increment(EntityServiceImpl.class, "optimistic_lock_scoped_retry_urns", 1.0);

    // (3) The conflicted aspect was persisted twice (pass-1 forced conflict + successful retry)...
    verify(aspectDao, times(2))
        .updateAspectConditional(
            any(),
            any(),
            argThat(sa -> sa != null && CORP_USER_INFO_ASPECT.equals(sa.getAspectName())),
            any());
    // ...while the already-committed sibling was persisted exactly ONCE: the committedKeys guard
    // skipped it on the scoped retry even though the URN-scoped sub-batch re-included it. This is
    // the double-commit guard the gate test cannot exercise (no conflict there → no retry pass).
    verify(aspectDao, times(1))
        .updateAspectConditional(
            any(),
            any(),
            argThat(sa -> sa != null && STATUS_ASPECT.equals(sa.getAspectName())),
            any());

    // (4) No duplicate MCL for either aspect (one emission each — the sibling was not re-emitted).
    verify(mockProducer, times(1))
        .produceMetadataChangeLog(
            any(OperationContext.class),
            any(Urn.class),
            argThat(
                (AspectSpec spec) -> spec != null && CORP_USER_INFO_ASPECT.equals(spec.getName())),
            any());
    verify(mockProducer, times(1))
        .produceMetadataChangeLog(
            any(OperationContext.class),
            any(Urn.class),
            argThat((AspectSpec spec) -> spec != null && STATUS_ASPECT.equals(spec.getName())),
            any());

    // (5) The batch ultimately succeeded with correct final state: both aspects hold their new
    // values and each landed exactly once (seed v1 + one write == v2).
    CorpUserInfo storedInfo =
        (CorpUserInfo)
            entityService
                .getLatestAspectsForUrn(opContext, urn, Set.of(CORP_USER_INFO_ASPECT), false)
                .get(CORP_USER_INFO_ASPECT);
    assertNotNull(storedInfo);
    assertTrue(DataTemplateUtil.areEqual(storedInfo, newInfo));

    Status storedStatus =
        (Status)
            entityService
                .getLatestAspectsForUrn(opContext, urn, Set.of(STATUS_ASPECT), false)
                .get(STATUS_ASPECT);
    assertNotNull(storedStatus);
    assertTrue(storedStatus.isRemoved());

    EntityAspect infoRow =
        aspectDao.getAspect(opContext, urn.toString(), CORP_USER_INFO_ASPECT, 0L);
    EntityAspect statusRow = aspectDao.getAspect(opContext, urn.toString(), STATUS_ASPECT, 0L);
    SystemMetadata infoMeta = SystemMetadataUtils.parseSystemMetadata(infoRow.getSystemMetadata());
    SystemMetadata statusMeta =
        SystemMetadataUtils.parseSystemMetadata(statusRow.getSystemMetadata());
    assertEquals(
        infoMeta.getVersion(), "2", "conflicted aspect must land exactly once (seed v1 + 1 write)");
    assertEquals(
        statusMeta.getVersion(),
        "2",
        "committed sibling must land exactly once — no double-commit on the scoped retry");
  }
}
