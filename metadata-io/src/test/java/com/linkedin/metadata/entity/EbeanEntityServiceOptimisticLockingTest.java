package com.linkedin.metadata.entity;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
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
import java.util.List;
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
 * EntityService-level integration tests for {@code OPTIMISTIC_LOCKING_ENABLED}.
 *
 * <p>Separate from {@link EbeanEntityServiceTest} so the default suite stays on pessimistic locking
 * while these exercise the real ingest path with CAS + txn retry.
 */
public class EbeanEntityServiceOptimisticLockingTest {

  private static final AuditStamp TEST_AUDIT_STAMP = AspectGenerationUtils.createAuditStamp();

  private EbeanAspectDao aspectDao;
  private EntityServiceImpl entityService;
  private EventProducer mockProducer;
  private UpdateIndicesService mockUpdateIndicesService;
  private OperationContext opContext;

  public EbeanEntityServiceOptimisticLockingTest() throws EntityRegistryException {}

  @BeforeMethod
  public void setup() {
    mockProducer = mock(EventProducer.class);
    mockUpdateIndicesService = mock(UpdateIndicesService.class);

    Database server =
        EbeanTestUtils.createTestServer(
            EbeanEntityServiceOptimisticLockingTest.class.getSimpleName());
    aspectDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            null,
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
    // Keep only current version so history-insert races are out of scope for EntityService tests.
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
  }

  @AfterMethod
  public void cleanup() {
    EbeanTestUtils.shutdownDatabaseFromAspectDao(aspectDao);
  }

  @Test
  public void testIdempotentReingestIsSkippedNoOpWithoutMcl() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olNoOp");
    CorpUserInfo info = AspectGenerationUtils.createCorpUserInfo("noop@test.com");
    String aspectName = AspectGenerationUtils.getAspectName(info);
    SystemMetadata metadata = AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP);

    entityService.ingestAspects(
        opContext,
        urn,
        List.of(Pair.of(aspectName, (RecordTemplate) info)),
        TEST_AUDIT_STAMP,
        metadata);

    reset(mockProducer);

    // Same payload + same system metadata versioning path → SKIPPED_NOOP at DAO, null from
    // ingestAspectToLocalDB, no MCL produced.
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(Pair.of(aspectName, (RecordTemplate) info)),
        TEST_AUDIT_STAMP,
        metadata);

    verify(mockProducer, times(0))
        .produceMetadataChangeLog(
            org.mockito.ArgumentMatchers.any(OperationContext.class),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any());
    verifyNoMoreInteractions(mockProducer);

    EntityAspect stored = aspectDao.getAspect(opContext, urn.toString(), aspectName, 0L);
    assertNotNull(stored);
    SystemMetadata storedMeta = SystemMetadataUtils.parseSystemMetadata(stored.getSystemMetadata());
    assertEquals(storedMeta.getVersion(), "1");
  }

  @Test
  public void testConcurrentIngestRetriesWithoutRetryLimitReached() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olConcurrent");
    String aspectName = AspectGenerationUtils.getAspectName(new CorpUserInfo());

    // Seed version-0 at SystemMetadata.version=1
    CorpUserInfo seed = AspectGenerationUtils.createCorpUserInfo("seed@test.com");
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(Pair.of(aspectName, (RecordTemplate) seed)),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    AtomicInteger successes = new AtomicInteger();
    AtomicReference<Throwable> firstError = new AtomicReference<>();
    CountDownLatch ready = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(2);
    ExecutorService pool = Executors.newFixedThreadPool(2);

    for (int i = 0; i < 2; i++) {
      final String email = "writer" + i + "@test.com";
      pool.submit(
          () -> {
            try {
              ready.await();
              CorpUserInfo write = AspectGenerationUtils.createCorpUserInfo(email);
              entityService.ingestAspects(
                  opContext,
                  AspectsBatchImpl.builder()
                      .retrieverContext(opContext.getRetrieverContext())
                      .items(
                          List.of(
                              ChangeItemImpl.builder()
                                  .urn(urn)
                                  .aspectName(aspectName)
                                  .recordTemplate(write)
                                  .systemMetadata(
                                      AspectGenerationUtils.createSystemMetadata(
                                          1, TEST_AUDIT_STAMP))
                                  .auditStamp(TEST_AUDIT_STAMP)
                                  .build(opContext.getAspectRetriever())))
                      .build(opContext),
                  true,
                  true);
              successes.incrementAndGet();
            } catch (Throwable t) {
              firstError.compareAndSet(null, t);
            } finally {
              done.countDown();
            }
          });
    }

    ready.countDown();
    assertTrue(done.await(90, TimeUnit.SECONDS), "concurrent ingest should finish");
    pool.shutdownNow();

    assertNull(
        firstError.get(),
        "concurrent optimistic ingest should not hit RetryLimitReached: " + firstError.get());
    assertEquals(successes.get(), 2);

    RecordTemplate latest =
        entityService
            .getLatestAspectsForUrn(opContext, urn, java.util.Set.of(aspectName), false)
            .get(aspectName);
    assertNotNull(latest);
    assertTrue(
        DataTemplateUtil.areEqual(
                latest, AspectGenerationUtils.createCorpUserInfo("writer0@test.com"))
            || DataTemplateUtil.areEqual(
                latest, AspectGenerationUtils.createCorpUserInfo("writer1@test.com")));

    EntityAspect stored = aspectDao.getAspect(opContext, urn.toString(), aspectName, 0L);
    SystemMetadata storedMeta = SystemMetadataUtils.parseSystemMetadata(stored.getSystemMetadata());
    assertEquals(
        storedMeta.getVersion(),
        "3",
        "seed(v1) + two conflicting updates should leave SystemMetadata.version=3");
  }

  @Test
  public void testLegacyNullVersionRowCanStillBeUpdated() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:corpuser:olLegacyNullVersion");
    String aspectName = AspectGenerationUtils.getAspectName(new Status());

    // Insert a legacy row with no SystemMetadata.version (bypassing EntityService versioning).
    io.ebean.Database server = aspectDao.getServer();
    com.linkedin.metadata.entity.ebean.EbeanAspectV2 aspectRecord =
        new com.linkedin.metadata.entity.ebean.EbeanAspectV2();
    aspectRecord.setKey(
        new com.linkedin.metadata.entity.ebean.EbeanAspectV2.PrimaryKey(
            urn.toString(), aspectName, 0L));
    aspectRecord.setMetadata("{\"removed\":false}");
    aspectRecord.setCreatedBy(TEST_AUDIT_STAMP.getActor().toString());
    aspectRecord.setCreatedFor(null);
    aspectRecord.setCreatedOn(new java.sql.Timestamp(TEST_AUDIT_STAMP.getTime()));
    aspectRecord.setSystemMetadata("{\"runId\":\"legacy-run\"}");
    server.save(aspectRecord);

    EntityAspect before = aspectDao.getAspect(opContext, urn.toString(), aspectName, 0L);
    assertNotNull(before);
    assertNull(SystemMetadataUtils.parseSystemMetadata(before.getSystemMetadata()).getVersion());

    entityService.ingestAspects(
        opContext,
        urn,
        List.of(Pair.of(aspectName, (RecordTemplate) new Status().setRemoved(true))),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));

    Status latest =
        (Status)
            entityService
                .getLatestAspectsForUrn(opContext, urn, java.util.Set.of(aspectName), false)
                .get(aspectName);
    assertTrue(latest.isRemoved());

    // The plain UPDATE must stamp SystemMetadata.version so subsequent writes use CAS instead of
    // falling back to last-writer-wins again.
    EntityAspect after = aspectDao.getAspect(opContext, urn.toString(), aspectName, 0L);
    assertNotNull(after);
    assertNotNull(
        SystemMetadataUtils.parseSystemMetadata(after.getSystemMetadata()).getVersion(),
        "expected the legacy row's update to stamp SystemMetadata.version, enabling CAS on"
            + " subsequent writes");
  }
}
