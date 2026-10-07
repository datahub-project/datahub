package com.linkedin.metadata.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertNotNull;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserEditableInfo;
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
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import com.linkedin.retention.VersionBasedRetention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.aspect.AspectTestUtils;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A real (H2) Ebean store behind a real {@link EntityServiceImpl}, with a mocked MCL producer to
 * capture what a delete emits. Fresh in-memory database per {@link #build}; the production default
 * version retention (20) so superseded values become history rows. Tests use their own urns and
 * explicit audit-stamp times, so {@code aspectCreated} is deterministic.
 */
final class CeilingDeleteH2Harness {
  private static final AtomicInteger SERVER_SEQUENCE = new AtomicInteger();
  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:ceiling-delete-test");

  final Database server;
  final EbeanAspectDao aspectDao;
  final EntityServiceImpl entityService;
  final EventProducer producer;
  final OperationContext opContext;

  private CeilingDeleteH2Harness(
      Database server,
      EbeanAspectDao aspectDao,
      EntityServiceImpl entityService,
      EventProducer producer,
      OperationContext opContext) {
    this.server = server;
    this.aspectDao = aspectDao;
    this.entityService = entityService;
    this.producer = producer;
    this.opContext = opContext;
  }

  static CeilingDeleteH2Harness build(boolean optimisticLocking) {
    final EventProducer producer = mock(EventProducer.class);
    final MetricUtils metricUtils = mock(MetricUtils.class);
    final Database server =
        EbeanTestUtils.createTestServer(
            CeilingDeleteH2Harness.class.getSimpleName() + "_" + SERVER_SEQUENCE.incrementAndGet());
    final EbeanAspectDao aspectDao =
        spy(
            new EbeanAspectDao(
                PrimaryStorageTestUtils.ebeanResolver(server),
                EbeanConfiguration.testDefault,
                null,
                List.of(),
                null,
                new PlainAspectTableResolver(),
                new PassThroughScopedTransactionFactory(server),
                optimisticLocking));
    aspectDao.setWritable(true);
    final PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    final EntityServiceImpl entityService =
        new EntityServiceImpl(
            aspectDao,
            producer,
            preProcessHooks,
            new EntityServiceConfiguration()
                .setAlwaysEmitChangeLog(false)
                .setCdcModeChangeLog(false)
                .setEnableBrowseV2(true),
            metricUtils);
    entityService.setUpdateIndicesService(mock(UpdateIndicesService.class));
    final EbeanRetentionService<ChangeItemImpl> retentionService =
        new EbeanRetentionService<>(
            entityService,
            server,
            1000,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server),
            RetentionTestUtils.systemEntityClient(entityService, producer, metricUtils));
    entityService.setRetentionService(retentionService);
    final EntityRegistry registry =
        AspectTestUtils.enhanceRegistryWithTestPlugins(
            TestOperationContexts.defaultEntityRegistry());
    final OperationContext opContext =
        TestOperationContexts.systemContext(
            null,
            null,
            null,
            () -> registry,
            () ->
                RetrieverContext.builder()
                    .aspectRetriever(
                        EntityServiceAspectRetriever.builder()
                            .entityService(entityService)
                            .entityRegistry(registry)
                            .build())
                    .cachingAspectRetriever(
                        TestOperationContexts.emptyActiveUsersAspectRetriever(() -> registry))
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .build(),
            null,
            ctx ->
                ((EntityServiceAspectRetriever) ctx.getAspectRetriever())
                    .setSystemOperationContext(ctx),
            null);
    // Production default (datahub-upgrade boot/retention.yaml): keep 20 versions of every aspect.
    retentionService.setRetention(
        opContext,
        null,
        null,
        new DataHubRetentionConfig()
            .setRetention(
                new Retention().setVersion(new VersionBasedRetention().setMaxVersions(20))));
    return new CeilingDeleteH2Harness(server, aspectDao, entityService, producer, opContext);
  }

  void upsert(Urn urn, String aspectName, RecordTemplate aspect, long timeMillis) {
    final AuditStamp stamp = new AuditStamp().setActor(ACTOR).setTime(timeMillis);
    entityService.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(
                List.of(
                    ChangeItemImpl.builder()
                        .urn(urn)
                        .aspectName(aspectName)
                        .recordTemplate(aspect)
                        .systemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
                        .auditStamp(stamp)
                        .build(opContext.getAspectRetriever())))
            .build(opContext),
        true,
        true);
  }

  static CorpUserEditableInfo editable(String aboutMe) {
    return new CorpUserEditableInfo().setAboutMe(aboutMe);
  }

  /** The {@code systemMetadata.version} stored on one row; fails if the row does not exist. */
  long storedVersion(Urn urn, String aspectName, long rowVersion) {
    final EntityAspect row = aspectDao.getAspect(opContext, urn.toString(), aspectName, rowVersion);
    assertNotNull(row, aspectName + " row " + rowVersion + " of " + urn);
    return Long.parseLong(
        RecordUtils.toRecordTemplate(SystemMetadata.class, row.getSystemMetadata()).getVersion());
  }

  boolean isKeyDelete(MetadataChangeLog mcl, Urn urn) {
    return mcl != null
        && mcl.getChangeType() == ChangeType.DELETE
        && opContext.getKeyAspectName(urn).equals(mcl.getAspectName());
  }

  static boolean isAspectDelete(MetadataChangeLog mcl, String aspectName) {
    return mcl != null
        && mcl.getChangeType() == ChangeType.DELETE
        && aspectName.equals(mcl.getAspectName());
  }

  /**
   * The deepest per-thread nesting of DAO transactions from now on, counted on the single entry
   * point both {@code runInTransactionWithRetry} overloads go through (fork {@code
   * EbeanAspectDao.java:1855-1876}, OSS {@code :1791-1812}).
   */
  AtomicInteger trackTransactionNesting() {
    final ThreadLocal<int[]> depth = ThreadLocal.withInitial(() -> new int[1]);
    final AtomicInteger maxDepth = new AtomicInteger();
    doAnswer(
            invocation -> {
              maxDepth.accumulateAndGet(++depth.get()[0], Math::max);
              try {
                return invocation.callRealMethod();
              } finally {
                depth.get()[0]--;
              }
            })
        .when(aspectDao)
        .runInTransactionWithRetryUnlocked(any(), any(), any(), anyInt());
    return maxDepth;
  }
}
