package com.linkedin.gms.factory.entity.update.indices;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.MappingsBuilderFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.gms.factory.search.SettingsBuilderFactory;
import com.linkedin.gms.factory.search.filter.QueryFilterRewriterChainFactory;
import com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.TimeseriesWriteThrottleCache;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.testng.annotations.Test;

/**
 * Pins the beans {@link UpdateIndicesServiceFactory} requires. The context has no {@link
 * TimeseriesAspectService} and no {@link SearchDocumentTransformer}: the service never uses them,
 * so neither variant may require them. Only this factory is pinned: the V2/V3 strategy beans from
 * {@link UpdateIndicesStrategyFactory} still need both, and they are left off this context.
 */
public class UpdateIndicesServiceFactoryDependenciesTest {

  private static ApplicationContextRunner runner() {
    EntityRegistry entityRegistry =
        TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();
    return new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withUserConfiguration(
            ConfigurationProvider.class,
            UpdateIndicesServiceFactory.class,
            IndexConventionFactory.class,
            SettingsBuilderFactory.class,
            MappingsBuilderFactory.class,
            QueryFilterRewriterChainFactory.class,
            SingleClusterSearch.class)
        .withBean("entityRegistry", EntityRegistry.class, () -> entityRegistry)
        .withBean(
            "searchClientShim",
            SearchClientShim.class,
            UpdateIndicesServiceFactoryDependenciesTest::client)
        .withBean(
            "baseElasticSearchComponents",
            BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class,
            () ->
                mock(
                    BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class,
                    RETURNS_MOCKS))
        .withBean(GraphService.class, () -> mock(GraphService.class))
        .withBean(SystemMetadataService.class, () -> mock(SystemMetadataService.class))
        .withBean(
            TimeseriesWriteThrottleCache.class,
            () ->
                new TimeseriesWriteThrottleCache(
                    TimeseriesWriteThrottleConfiguration.builder().build()));
  }

  @Test
  public void testGmsServiceStartsWithoutTimeseriesServiceOrDocumentTransformer() {
    EntityService<?> entityService = mock(EntityService.class);
    runner()
        .withBean("entityService", EntityService.class, () -> entityService)
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasBean("searchIndicesServiceGMS");
              assertThat(context)
                  .doesNotHaveBean(TimeseriesAspectService.class)
                  .doesNotHaveBean(SearchDocumentTransformer.class);
              verify(entityService)
                  .setUpdateIndicesService(context.getBean(UpdateIndicesService.class));
            });
  }

  @Test
  public void testNonGmsServiceStartsWithoutTimeseriesServiceOrDocumentTransformer() {
    runner()
        .withPropertyValues("entityClient.impl=restli")
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasBean("searchIndicesServiceNonGMS");
              assertThat(context)
                  .doesNotHaveBean(TimeseriesAspectService.class)
                  .doesNotHaveBean(SearchDocumentTransformer.class)
                  .doesNotHaveBean(EntityService.class);
            });
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
