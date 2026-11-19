package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.ASSERTION_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.datahub.util.RecordUtils;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.hooks.AssertionInfoMutator;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Re-ingesting an assertionInfo that differs only in {@code source.created} (clients stamp it with
 * wall-clock time on every run) must keep the original created stamp and emit no MCL. The stored
 * version may still advance, because write mutation hooks run after the batch's version pass; that
 * history is bounded by version retention.
 */
public class AssertionInfoSourceCreatedNoOpTest {

  private static final AuditStamp TEST_AUDIT_STAMP = AspectGenerationUtils.createAuditStamp();
  private static final Urn ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:created-noop");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");

  @DataProvider
  public Object[][] optimisticLocking() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "optimisticLocking")
  public void testReingestDifferingOnlyInSourceCreatedIsNoOp(boolean optimisticLocking)
      throws Exception {
    Database server =
        EbeanTestUtils.createTestServer(
            AssertionInfoSourceCreatedNoOpTest.class.getSimpleName() + optimisticLocking);
    EbeanAspectDao aspectDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            null,
            List.of(),
            null,
            optimisticLocking);
    aspectDao.setWritable(true);
    try {
      EventProducer mockProducer = mock(EventProducer.class);
      EntityServiceImpl entityService =
          new EntityServiceImpl(
              aspectDao,
              mockProducer,
              new PreProcessHooks(),
              new EntityServiceConfiguration()
                  .setAlwaysEmitChangeLog(false)
                  .setCdcModeChangeLog(false),
              mock(MetricUtils.class));
      entityService.setUpdateIndicesService(mock(UpdateIndicesService.class));

      // Production registers AssertionInfoMutator via Spring, so add it to the test registry.
      EntityRegistry registry = spy(TestOperationContexts.defaultEntityRegistry());
      doReturn(
              List.of(
                  new AssertionInfoMutator()
                      .setConfig(
                          AspectPluginConfig.builder()
                              .className(AssertionInfoMutator.class.getName())
                              .enabled(true)
                              .supportedOperations(List.of("UPSERT"))
                              .supportedEntityAspectNames(
                                  List.of(
                                      AspectPluginConfig.EntityAspectName.builder()
                                          .entityName(ASSERTION_ENTITY_NAME)
                                          .aspectName(ASSERTION_INFO_ASPECT_NAME)
                                          .build()))
                              .build())))
          .when(registry)
          .getAllMutationHooks();

      OperationContext opContext =
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

      ingest(entityService, opContext, assertionInfo(1000L));
      reset(mockProducer);

      ingest(entityService, opContext, assertionInfo(2000L));

      verify(mockProducer, never())
          .produceMetadataChangeLog(any(OperationContext.class), any(), any(), any());
      EntityAspect stored =
          aspectDao.getAspect(opContext, ASSERTION_URN.toString(), ASSERTION_INFO_ASPECT_NAME, 0L);
      AssertionInfo storedInfo =
          RecordUtils.toRecordTemplate(AssertionInfo.class, stored.getMetadata());
      assertEquals(storedInfo.getSource().getCreated().getTime().longValue(), 1000L);
    } finally {
      EbeanTestUtils.shutdownDatabaseFromAspectDao(aspectDao);
    }
  }

  private static void ingest(
      EntityServiceImpl entityService, OperationContext opContext, AssertionInfo info) {
    entityService.ingestAspects(
        opContext,
        ASSERTION_URN,
        List.of(Pair.of(ASSERTION_INFO_ASPECT_NAME, (RecordTemplate) info)),
        TEST_AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata(1, TEST_AUDIT_STAMP));
  }

  private static AssertionInfo assertionInfo(long createdTime) {
    return new AssertionInfo()
        .setType(AssertionType.FRESHNESS)
        .setFreshnessAssertion(
            new FreshnessAssertionInfo()
                .setType(FreshnessAssertionType.DATASET_CHANGE)
                .setEntity(DATASET_URN)
                .setSchedule(
                    new FreshnessAssertionSchedule()
                        .setType(FreshnessAssertionScheduleType.SINCE_THE_LAST_CHECK)))
        .setEntityUrn(DATASET_URN)
        .setSource(
            new AssertionSource()
                .setType(AssertionSourceType.EXTERNAL)
                .setCreated(
                    new AuditStamp()
                        .setTime(createdTime)
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:__datahub_system"))));
  }
}
