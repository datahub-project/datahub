package com.linkedin.gms.factory.entity.update.indices;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.MappingsBuilderFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.gms.factory.search.SearchDocumentTransformerFactory;
import com.linkedin.gms.factory.search.SettingsBuilderFactory;
import com.linkedin.gms.factory.search.filter.QueryFilterRewriterChainFactory;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.service.TimeseriesWriteThrottleCache;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testng.annotations.Test;

/**
 * Loads {@link UpdateIndicesServiceFactory} with the production {@code application.yaml} and the
 * real search factories its {@code @Import} pulls in. Checks that {@code entityClient.impl} picks
 * the GMS or non-GMS variant, that the diff-mode and structured-property switches reach the
 * service, and that the strategies on the context receive change events. Search, graph, timeseries
 * and system-metadata stores are mocked.
 */
public class UpdateIndicesServiceFactoryTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");
  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private static ApplicationContextRunner runner() {
    EntityRegistry entityRegistry = OP_CONTEXT.getEntityRegistry();
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(
            UpdateIndicesServiceFactory.class,
            IndexConventionFactory.class,
            SettingsBuilderFactory.class,
            MappingsBuilderFactory.class,
            QueryFilterRewriterChainFactory.class,
            SearchDocumentTransformerFactory.class,
            SingleClusterSearch.class)
        .withBean("entityRegistry", EntityRegistry.class, () -> entityRegistry)
        .withBean(
            "searchClientShim", SearchClientShim.class, UpdateIndicesServiceFactoryTest::client)
        .withBean(
            "baseElasticSearchComponents",
            BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class,
            () ->
                mock(
                    BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class,
                    RETURNS_MOCKS))
        .withBean(GraphService.class, () -> mock(GraphService.class))
        .withBean(TimeseriesAspectService.class, () -> mock(TimeseriesAspectService.class))
        .withBean(SystemMetadataService.class, () -> mock(SystemMetadataService.class))
        .withBean(
            TimeseriesWriteThrottleCache.class,
            () ->
                new TimeseriesWriteThrottleCache(
                    TimeseriesWriteThrottleConfiguration.builder().build()));
  }

  @Test
  public void testProductionDefaultsBuildTheGmsServiceAndRegisterIt() {
    EntityService<?> entityService = mock(EntityService.class);
    runner()
        .withBean("entityService", EntityService.class, () -> entityService)
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasBean("searchIndicesServiceGMS");
              assertThat(context).doesNotHaveBean("searchIndicesServiceNonGMS");
              UpdateIndicesService service = service(context);
              verify(entityService).setUpdateIndicesService(service);

              assertThat(service.isSearchDiffMode()).isTrue();
              assertThat(service.isStructuredPropertiesHookEnabled()).isTrue();
              assertThat(service.isStructuredPropertiesWriteEnabled()).isTrue();
              assertThat(service.getUpdateGraphIndicesService().isGraphDiffMode()).isTrue();
              assertThat(service.getUpdateGraphIndicesService().isGraphStatusEnabled()).isFalse();
            });
  }

  @Test
  public void testSwitchesReachTheService() {
    runner()
        .withBean("entityService", EntityService.class, () -> mock(EntityService.class))
        .withPropertyValues(
            "featureFlags.searchServiceDiffModeEnabled=false",
            "featureFlags.graphServiceDiffModeEnabled=false",
            "structuredProperties.enabled=false",
            "structuredProperties.writeEnabled=false",
            "elasticsearch.search.graph.graphStatusEnabled=true")
        .run(
            context -> {
              UpdateIndicesService service = service(context);
              assertThat(service.isSearchDiffMode()).isFalse();
              assertThat(service.isStructuredPropertiesHookEnabled()).isFalse();
              assertThat(service.isStructuredPropertiesWriteEnabled()).isFalse();
              assertThat(service.getUpdateGraphIndicesService().isGraphDiffMode()).isFalse();
              assertThat(service.getUpdateGraphIndicesService().isGraphStatusEnabled()).isTrue();
            });
  }

  @Test
  public void testRestliEntityClientBuildsTheNonGmsServiceWithoutAnEntityService() {
    runner()
        .withPropertyValues(
            "entityClient.impl=restli",
            "featureFlags.searchServiceDiffModeEnabled=false",
            "featureFlags.graphServiceDiffModeEnabled=false",
            "structuredProperties.writeEnabled=false",
            "elasticsearch.search.graph.graphStatusEnabled=true")
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasBean("searchIndicesServiceNonGMS");
              assertThat(context).doesNotHaveBean("searchIndicesServiceGMS");
              UpdateIndicesService service = service(context);
              assertThat(service.isSearchDiffMode()).isFalse();
              assertThat(service.isStructuredPropertiesWriteEnabled()).isFalse();
              assertThat(service.getUpdateGraphIndicesService().isGraphDiffMode()).isFalse();
              assertThat(service.getUpdateGraphIndicesService().isGraphStatusEnabled()).isTrue();
            });
  }

  @Test
  public void testEveryStrategyOnTheContextReceivesChangeEvents() {
    assertEveryStrategyReceivesChangeEvents(
        runner().withBean("entityService", EntityService.class, () -> mock(EntityService.class)));
  }

  @Test
  public void testEveryStrategyReceivesChangeEventsOnTheNonGmsService() {
    assertEveryStrategyReceivesChangeEvents(
        runner().withPropertyValues("entityClient.impl=restli"));
  }

  /** V2, V3 and upgrade strategies all get the event, with the configured structured-props flag. */
  @SuppressWarnings("unchecked")
  private static void assertEveryStrategyReceivesChangeEvents(ApplicationContextRunner runner) {
    UpdateIndicesStrategy v2 = enabledStrategy();
    UpdateIndicesStrategy v3 = enabledStrategy();
    UpdateIndicesStrategy upgrade = enabledStrategy();
    runner
        .withBean("updateIndicesV2Strategy", UpdateIndicesStrategy.class, () -> v2)
        .withBean("updateIndicesV3Strategy", UpdateIndicesStrategy.class, () -> v3)
        .withBean("updateIndicesUpgradeStrategy", UpdateIndicesStrategy.class, () -> upgrade)
        .withPropertyValues("structuredProperties.enabled=false")
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              service(context).handleChangeEvents(OP_CONTEXT, List.of(statusChange()));
              for (UpdateIndicesStrategy strategy : List.of(v2, v3, upgrade)) {
                ArgumentCaptor<Map<Urn, List<MCLItem>>> grouped =
                    ArgumentCaptor.forClass(Map.class);
                verify(strategy).processBatch(any(), grouped.capture(), eq(false));
                assertThat(grouped.getValue()).containsOnlyKeys(DATASET_URN);
                assertThat(grouped.getValue().get(DATASET_URN))
                    .extracting(MCLItem::getAspectName)
                    .contains(STATUS_ASPECT_NAME);
              }
            });
  }

  private static MetadataChangeLog statusChange() {
    return new MetadataChangeLog()
        .setEntityType(DATASET_ENTITY_NAME)
        .setEntityUrn(DATASET_URN)
        .setChangeType(ChangeType.UPSERT)
        .setAspectName(STATUS_ASPECT_NAME)
        .setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)))
        .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
  }

  private static UpdateIndicesService service(ApplicationContext context) {
    return context.getBean(UpdateIndicesService.class);
  }

  private static UpdateIndicesStrategy enabledStrategy() {
    UpdateIndicesStrategy strategy = mock(UpdateIndicesStrategy.class);
    when(strategy.isEnabled()).thenReturn(true);
    return strategy;
  }

  private static SearchClientShim<?> client() {
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(client.partialNgramConfig()).thenReturn(Es8SearchClientShim.PARTIAL_NGRAM_CONFIG);
    return client;
  }

  /**
   * The production single-cluster registry over the mocked client. Bulk processor and index builder
   * talk to the cluster, so they are mocks. Not a {@code @Configuration}, so no component scan
   * picks it up.
   */
  static class SingleClusterSearch {
    @Bean("searchClusterRegistry")
    SearchClusterRegistry searchClusterRegistry(
        ConfigurationProvider configurationProvider,
        @Qualifier("searchClientShim") SearchClientShim<?> client) {
      return SearchClusterRegistry.singleCluster(
          configurationProvider.getElasticSearch(),
          client,
          mock(ESBulkProcessor.class),
          mock(ESIndexBuilder.class));
    }
  }
}
