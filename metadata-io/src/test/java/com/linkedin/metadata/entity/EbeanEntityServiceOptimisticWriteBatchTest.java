package com.linkedin.metadata.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
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
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.lock.EntityWriteLock;
import com.linkedin.metadata.entity.retention.RetentionTestUtils;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for optimistic write batching enabled flag. Verifies that:
 *
 * <ul>
 *   <li>Batch mode engages when the flag is enabled (multiple aspects committed in one batch call)
 *   <li>Batch conflicts trigger scoped retry per-URN, skipping already-committed siblings
 *   <li>Batch mode can be disabled, reverting to sequential single-aspect path
 * </ul>
 *
 * <p>These tests run against embedded H2 with optimistic locking + scoped retry + batch mode all
 * enabled. The batch-specific stub forces a conflict on the first batch call via
 * updateAspectsConditionalBatch (if batching is on) to verify retry logic and committed-key dedup.
 */
public class EbeanEntityServiceOptimisticWriteBatchTest {

  private static final AuditStamp TEST_AUDIT_STAMP = AspectGenerationUtils.createAuditStamp();
  private static final String CORP_USER_INFO_ASPECT =
      AspectGenerationUtils.getAspectName(new CorpUserInfo());
  private static final String STATUS_ASPECT = AspectGenerationUtils.getAspectName(new Status());

  private EbeanAspectDao aspectDao;
  private EntityServiceImpl entityService;
  private EventProducer mockProducer;
  private MetricUtils spyMetrics;
  private OperationContext opContext;

  public EbeanEntityServiceOptimisticWriteBatchTest() throws EntityRegistryException {}

  @BeforeMethod
  public void setup() {
    mockProducer = mock(EventProducer.class);
    UpdateIndicesService mockUpdateIndicesService = mock(UpdateIndicesService.class);

    Database server =
        EbeanTestUtils.createTestServer(
            EbeanEntityServiceOptimisticWriteBatchTest.class.getSimpleName());

    // OL + scoped retry + batch: enable all three for this test suite.
    EbeanConfiguration config =
        EbeanConfiguration.builder()
            .optimisticLockingEnabled(true)
            .scopedRetryEnabled(true)
            .optimisticWriteBatchEnabled(true)
            .optimisticWriteBatchMinSize(1)
            .build();

    // Real MetricUtils wrapped in a spy so batch metric increments can be verified.
    spyMetrics = spy(MetricUtils.builder().registry(new SimpleMeterRegistry()).build());

    // Spy the real DAO so the entire path runs against H2 and only batch-specific seams are
    // intercepted.
    aspectDao =
        spy(
            new EbeanAspectDao(
                PrimaryStorageTestUtils.ebeanResolver(server),
                config,
                spyMetrics,
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
    // Keep only current version so history-insert races are out of scope.
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
    assertTrue(aspectDao.isOptimisticWriteBatchEnabled());
  }

  @AfterMethod
  public void cleanup() {
    EbeanTestUtils.shutdownDatabaseFromAspectDao(aspectDao);
  }

  private void seedBothAspects(Urn urn, String email, boolean removed) {
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(
            com.linkedin.util.Pair.of(
                CORP_USER_INFO_ASPECT,
                (RecordTemplate) AspectGenerationUtils.createCorpUserInfo(email)),
            com.linkedin.util.Pair.of(
                STATUS_ASPECT, (RecordTemplate) new Status().setRemoved(removed))),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));
  }

  private void assertStoredEmail(Urn urn, String expectedEmail) {
    CorpUserInfo storedInfo =
        (CorpUserInfo)
            entityService
                .getLatestAspectsForUrn(opContext, urn, Set.of(CORP_USER_INFO_ASPECT), false)
                .get(CORP_USER_INFO_ASPECT);
    assertNotNull(storedInfo);
    assertEquals(storedInfo.getEmail(), expectedEmail);
  }

  private void assertStoredRemoved(Urn urn, boolean expectedRemoved) {
    Status storedStatus =
        (Status)
            entityService
                .getLatestAspectsForUrn(opContext, urn, Set.of(STATUS_ASPECT), false)
                .get(STATUS_ASPECT);
    assertNotNull(storedStatus);
    assertEquals(storedStatus.isRemoved(), expectedRemoved);
  }

  @Test
  public void batchedMultiAspectUpdatesCommitAndCallBatch() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olBatchCommit");

    // Seed both aspects at SystemMetadata.version=1.
    seedBothAspects(urn, "seed@test.com", false);

    reset(mockProducer);

    // Ingest NEW values for both aspects in ONE call — batching should engage.
    CorpUserInfo newInfo = AspectGenerationUtils.createCorpUserInfo("writer@test.com");
    Status newStatus = new Status().setRemoved(true);
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(
            com.linkedin.util.Pair.of(CORP_USER_INFO_ASPECT, (RecordTemplate) newInfo),
            com.linkedin.util.Pair.of(STATUS_ASPECT, (RecordTemplate) newStatus)),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Verify final state: both aspects updated to the new values.
    assertStoredEmail(urn, "writer@test.com");
    assertStoredRemoved(urn, true);

    // Verify version history: both aspects have two versions (seed v1 + new v2).
    EntityAspect infoRow =
        aspectDao.getAspect(opContext, urn.toString(), CORP_USER_INFO_ASPECT, 1L);
    assertNotNull(infoRow, "version 1 (seed) should exist for corpUserInfo");

    EntityAspect statusRow = aspectDao.getAspect(opContext, urn.toString(), STATUS_ASPECT, 1L);
    assertNotNull(statusRow, "version 1 (seed) should exist for status");

    // Verify batch path was engaged: updateAspectsConditionalBatch should have been called.
    verify(aspectDao, atLeast(1)).updateAspectsConditionalBatch(any(), any(), any());
  }

  @Test
  public void conflictInBatchTriggersScopedRetryAndSkipsCommittedSibling() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olBatchConflict");

    // Seed both aspects at version=1.
    seedBothAspects(urn, "seed@test.com", false);

    reset(mockProducer);

    // Install stub on updateAspectsConditionalBatch: ON THE FIRST CALL ONLY, force corpUserInfo
    // to conflict while genuinely committing the STATUS sibling via single-row CAS.
    final AtomicInteger batchCalls = new AtomicInteger();
    doAnswer(
            inv -> {
              List<ConditionalAspectUpdate> updates = inv.getArgument(2);
              if (batchCalls.getAndIncrement() == 0) {
                List<ConditionalUpdateResult> out = new ArrayList<>();
                for (ConditionalAspectUpdate u : updates) {
                  if (CORP_USER_INFO_ASPECT.equals(u.getNewAspect().getAspectName())) {
                    // Simulate lost CAS race: DB row untouched, version mismatch.
                    out.add(ConditionalUpdateResult.CONFLICT);
                  } else {
                    // Commit the sibling via real single-row CAS.
                    Optional<EntityAspect> r =
                        aspectDao.updateAspectConditional(
                            inv.getArgument(0),
                            inv.getArgument(1),
                            u.getNewAspect(),
                            u.getExpectedSystemMetadataVersion());
                    out.add(
                        r.isPresent()
                            ? ConditionalUpdateResult.UPDATED
                            : ConditionalUpdateResult.CONFLICT);
                  }
                }
                return out;
              }
              // Fall back to real method on retry and beyond.
              return inv.callRealMethod();
            })
        .when(aspectDao)
        .updateAspectsConditionalBatch(any(), any(), any());

    // Ingest new values for both aspects.
    CorpUserInfo newInfo = AspectGenerationUtils.createCorpUserInfo("writer@test.com");
    Status newStatus = new Status().setRemoved(true);
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(
            com.linkedin.util.Pair.of(CORP_USER_INFO_ASPECT, (RecordTemplate) newInfo),
            com.linkedin.util.Pair.of(STATUS_ASPECT, (RecordTemplate) newStatus)),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Verify both aspects ended up committed with new values (despite the forced conflict).
    assertStoredEmail(urn, "writer@test.com");
    assertStoredRemoved(urn, true);

    // Verify scoped retry happened: updateAspectsConditionalBatch called at least twice
    // (first pass batch + retry pass batch, since minSize=1 the 1-item retry also batches).
    verify(aspectDao, atLeast(2)).updateAspectsConditionalBatch(any(), any(), any());
  }

  @Test
  public void batchDisabledFlagUsesSequentialPath() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olBatchDisabled");

    // Build a SECOND local DAO + EntityService with batching DISABLED.
    Database server2 =
        EbeanTestUtils.createTestServer(
            EbeanEntityServiceOptimisticWriteBatchTest.class.getSimpleName() + "_Sequential");

    EbeanConfiguration configNoBatch =
        EbeanConfiguration.builder()
            .optimisticLockingEnabled(true)
            .scopedRetryEnabled(true)
            .optimisticWriteBatchEnabled(false) // DISABLED
            .build();

    EbeanAspectDao localAspectDao =
        spy(
            new EbeanAspectDao(
                PrimaryStorageTestUtils.ebeanResolver(server2),
                configNoBatch,
                null,
                List.of(),
                null,
                /* optimisticLocking */ true));
    localAspectDao.setWritable(true);

    PreProcessHooks localPreProcessHooks = new PreProcessHooks();
    localPreProcessHooks.setUiEnabled(true);
    EntityServiceImpl localEntityService =
        new EntityServiceImpl(
            localAspectDao,
            mock(EventProducer.class),
            localPreProcessHooks,
            new EntityServiceConfiguration()
                .setAlwaysEmitChangeLog(false)
                .setCdcModeChangeLog(false)
                .setEnableBrowseV2(true),
            null);
    localEntityService.setUpdateIndicesService(mock(UpdateIndicesService.class));

    // Seed one aspect.
    localEntityService.ingestAspects(
        opContext,
        urn,
        List.of(
            com.linkedin.util.Pair.of(
                CORP_USER_INFO_ASPECT,
                (RecordTemplate) AspectGenerationUtils.createCorpUserInfo("seed@test.com"))),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Update the aspect.
    CorpUserInfo newInfo = AspectGenerationUtils.createCorpUserInfo("writer@test.com");
    localEntityService.ingestAspects(
        opContext,
        urn,
        List.of(com.linkedin.util.Pair.of(CORP_USER_INFO_ASPECT, (RecordTemplate) newInfo)),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Verify the aspect updated correctly.
    CorpUserInfo storedInfo =
        (CorpUserInfo)
            localEntityService
                .getLatestAspectsForUrn(opContext, urn, Set.of(CORP_USER_INFO_ASPECT), false)
                .get(CORP_USER_INFO_ASPECT);
    assertNotNull(storedInfo);
    assertEquals(storedInfo.getEmail(), "writer@test.com");

    // Verify batch path was NOT used: updateAspectsConditionalBatch should never be called.
    verify(localAspectDao, never()).updateAspectsConditionalBatch(any(), any(), any());

    // Cleanup local server.
    EbeanTestUtils.shutdownDatabaseFromAspectDao(localAspectDao);
  }

  @Test
  public void batchWithoutScopedRetryUsesSequentialPath() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olBatchNoScoped");

    // Build a SECOND local DAO + EntityService with scoped retry DISABLED
    // even though batching is ENABLED. Batching only operates on the scoped-retry path,
    // so it should remain inert and fall back to sequential.
    Database server2 =
        EbeanTestUtils.createTestServer(
            EbeanEntityServiceOptimisticWriteBatchTest.class.getSimpleName() + "_NoScopedRetry");

    EbeanConfiguration configNoBatch =
        EbeanConfiguration.builder()
            .optimisticLockingEnabled(true)
            .scopedRetryEnabled(false) // DISABLED - batching should not engage
            .optimisticWriteBatchEnabled(true) // ON, but should stay inert
            .optimisticWriteBatchMinSize(1)
            .build();

    EbeanAspectDao localAspectDao =
        spy(
            new EbeanAspectDao(
                PrimaryStorageTestUtils.ebeanResolver(server2),
                configNoBatch,
                null,
                List.of(),
                null,
                /* optimisticLocking */ true));
    localAspectDao.setWritable(true);

    PreProcessHooks localPreProcessHooks = new PreProcessHooks();
    localPreProcessHooks.setUiEnabled(true);
    EntityServiceImpl localEntityService =
        new EntityServiceImpl(
            localAspectDao,
            mock(EventProducer.class),
            localPreProcessHooks,
            new EntityServiceConfiguration()
                .setAlwaysEmitChangeLog(false)
                .setCdcModeChangeLog(false)
                .setEnableBrowseV2(true),
            null);
    localEntityService.setUpdateIndicesService(mock(UpdateIndicesService.class));

    // Seed one aspect.
    localEntityService.ingestAspects(
        opContext,
        urn,
        List.of(
            com.linkedin.util.Pair.of(
                CORP_USER_INFO_ASPECT,
                (RecordTemplate) AspectGenerationUtils.createCorpUserInfo("seed@test.com"))),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Update the aspect.
    CorpUserInfo newInfo = AspectGenerationUtils.createCorpUserInfo("writer@test.com");
    localEntityService.ingestAspects(
        opContext,
        urn,
        List.of(com.linkedin.util.Pair.of(CORP_USER_INFO_ASPECT, (RecordTemplate) newInfo)),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Verify the aspect updated correctly.
    CorpUserInfo storedInfo =
        (CorpUserInfo)
            localEntityService
                .getLatestAspectsForUrn(opContext, urn, Set.of(CORP_USER_INFO_ASPECT), false)
                .get(CORP_USER_INFO_ASPECT);
    assertNotNull(storedInfo);
    assertEquals(storedInfo.getEmail(), "writer@test.com");

    // Verify batch path was NOT used: updateAspectsConditionalBatch should never be called.
    verify(localAspectDao, never()).updateAspectsConditionalBatch(any(), any(), any());

    // Cleanup local server.
    EbeanTestUtils.shutdownDatabaseFromAspectDao(localAspectDao);
  }

  @Test
  public void newAspectInsertIsNotBatched() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olBatchInsert");

    // Ingest ONE new aspect on a fresh URN that has never been seeded.
    // No existing version-0 row -> plan kind INSERT_NEW -> sequential (not batched).
    CorpUserInfo newInfo = AspectGenerationUtils.createCorpUserInfo("new@test.com");
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(com.linkedin.util.Pair.of(CORP_USER_INFO_ASPECT, (RecordTemplate) newInfo)),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Verify batch path was NOT used: updateAspectsConditionalBatch should never be called.
    verify(aspectDao, never()).updateAspectsConditionalBatch(any(), any(), any());

    // Verify the aspect was inserted with correct value.
    CorpUserInfo storedInfo =
        (CorpUserInfo)
            entityService
                .getLatestAspectsForUrn(opContext, urn, Set.of(CORP_USER_INFO_ASPECT), false)
                .get(CORP_USER_INFO_ASPECT);
    assertNotNull(storedInfo);
    assertEquals(storedInfo.getEmail(), "new@test.com");
  }

  @Test
  public void legacyNullVersionRowIsNotBatched() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olBatchLegacy");

    // Seed one aspect with SystemMetadata that has NO version field (legacy row).
    // Use createSystemMetadata(int, String, String, String, AuditStamp) with version=null
    // and SetMode.IGNORE_NULL will leave the version field unset.
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(
            com.linkedin.util.Pair.of(
                CORP_USER_INFO_ASPECT,
                (RecordTemplate) AspectGenerationUtils.createCorpUserInfo("legacy@test.com"))),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(
            1625792689, "run-123", null, null, TEST_AUDIT_STAMP));

    reset(mockProducer);

    // Now update that aspect with batch enabled. The existing row has null version,
    // so the read-modify-write will see LEGACY_UNCONDITIONAL -> sequential path (not batched).
    CorpUserInfo updatedInfo = AspectGenerationUtils.createCorpUserInfo("updated@test.com");
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(com.linkedin.util.Pair.of(CORP_USER_INFO_ASPECT, (RecordTemplate) updatedInfo)),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Verify batch path was NOT used: legacy path is sequential.
    verify(aspectDao, never()).updateAspectsConditionalBatch(any(), any(), any());

    // Verify the update applied (last-writer-wins).
    CorpUserInfo storedInfo =
        (CorpUserInfo)
            entityService
                .getLatestAspectsForUrn(opContext, urn, Set.of(CORP_USER_INFO_ASPECT), false)
                .get(CORP_USER_INFO_ASPECT);
    assertNotNull(storedInfo);
    assertEquals(storedInfo.getEmail(), "updated@test.com");
  }

  @Test
  public void batchEmitsBatchSizeAndExecutionsMetrics() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olBatchMetrics");

    // Seed both aspects at SystemMetadata.version=1.
    seedBothAspects(urn, "seed@test.com", false);

    reset(mockProducer);

    // Ingest NEW values for both aspects in ONE call — batching should engage and emit metrics.
    CorpUserInfo newInfo = AspectGenerationUtils.createCorpUserInfo("metrics@test.com");
    Status newStatus = new Status().setRemoved(true);
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(
            com.linkedin.util.Pair.of(CORP_USER_INFO_ASPECT, (RecordTemplate) newInfo),
            com.linkedin.util.Pair.of(STATUS_ASPECT, (RecordTemplate) newStatus)),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Verify batch-level metrics were recorded.
    String batchSizeMetric =
        MetricRegistry.name(EbeanAspectDao.class, "optimistic_lock_batch_size");
    String batchExecutionsMetric =
        MetricRegistry.name(EbeanAspectDao.class, "optimistic_lock_batch_executions");

    verify(spyMetrics, times(1)).increment(eq(batchSizeMetric), eq(2));
    verify(spyMetrics, times(1)).increment(eq(batchExecutionsMetric), eq(1));

    // Verify final state: both aspects updated to the new values.
    assertStoredEmail(urn, "metrics@test.com");
    assertStoredRemoved(urn, true);
  }

  @Test
  public void batchingWorksUnderWriteGate() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olBatchWriteGate");

    // Seed both aspects at SystemMetadata.version=1.
    seedBothAspects(urn, "seed@test.com", false);

    reset(mockProducer);

    // Inject a simple in-process write gate to test orthogonality.
    entityService.setEntityWriteLock(new TestEntityWriteLock());

    // Ingest NEW values for both aspects in ONE call with the gate engaged.
    CorpUserInfo newInfo = AspectGenerationUtils.createCorpUserInfo("gate@test.com");
    Status newStatus = new Status().setRemoved(true);
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(
            com.linkedin.util.Pair.of(CORP_USER_INFO_ASPECT, (RecordTemplate) newInfo),
            com.linkedin.util.Pair.of(STATUS_ASPECT, (RecordTemplate) newStatus)),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    // Verify both aspects committed with new values (gate did not interfere with batching).
    assertStoredEmail(urn, "gate@test.com");
    assertStoredRemoved(urn, true);

    // Verify batch path was engaged even with gate present.
    verify(aspectDao, atLeast(1)).updateAspectsConditionalBatch(any(), any(), any());
  }

  /** Simple in-process write gate for testing (no external Hazelcast). */
  private static class TestEntityWriteLock implements EntityWriteLock {
    @Override
    public LockHandle acquire(
        io.datahubproject.metadata.context.OperationContext opContext,
        java.util.Collection<String> keys) {
      // No-op acquire for single-threaded test; gate is active but does not contend.
      return () -> {};
    }

    @Override
    public boolean isActive() {
      return true; // Mark as an active gate so the write path engages it.
    }
  }

  @Test
  public void duplicateAspectKeyInOneBatchIsNotBatched() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olBatchDuplicate");

    // Seed the aspect once at version=1.
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(
            com.linkedin.util.Pair.of(
                CORP_USER_INFO_ASPECT,
                (RecordTemplate) AspectGenerationUtils.createCorpUserInfo("seed@test.com"))),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    reset(mockProducer);

    // Build a batch with TWO ChangeItems for the SAME (urn, CORP_USER_INFO_ASPECT)
    // with DIFFERENT values. The second write should win (last-writer-wins).
    CorpUserInfo firstValue = AspectGenerationUtils.createCorpUserInfo("first@test.com");
    CorpUserInfo secondValue = AspectGenerationUtils.createCorpUserInfo("second@test.com");

    ChangeItemImpl item1 =
        ChangeItemImpl.builder()
            .urn(urn)
            .aspectName(CORP_USER_INFO_ASPECT)
            .recordTemplate(firstValue)
            .systemMetadata(AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP))
            .auditStamp(TEST_AUDIT_STAMP)
            .build(opContext.getAspectRetriever());

    ChangeItemImpl item2 =
        ChangeItemImpl.builder()
            .urn(urn)
            .aspectName(CORP_USER_INFO_ASPECT)
            .recordTemplate(secondValue)
            .systemMetadata(AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP))
            .auditStamp(TEST_AUDIT_STAMP)
            .build(opContext.getAspectRetriever());

    AspectsBatchImpl batch =
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(List.of(item1, item2))
            .build(opContext);

    entityService.ingestAspects(opContext, batch, true, true);

    // Verify batch path was NOT used: duplicate (urn,aspect) key is excluded from batching.
    verify(aspectDao, never()).updateAspectsConditionalBatch(any(), any(), any());

    // Verify final stored value is the last write's value (second@test.com), no exception.
    assertStoredEmail(urn, "second@test.com");
  }
}
