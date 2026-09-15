package com.datahub.gms.servlet;

import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ComponentScan(
    basePackages = {
      "com.linkedin.gms.factory.common",
      "com.linkedin.gms.factory.config",
      "com.linkedin.gms.factory.entityregistry",
      "com.linkedin.gms.factory.plugins",
      "com.linkedin.gms.factory.system_telemetry"
    })
public class ConfigServletTestContext {

  @Bean("systemOperationContext")
  @Primary
  public OperationContext systemOperationContext(
      @Autowired(required = false) SearchClientShim<?> searchClientShim) {
    OperationContext context = TestOperationContexts.systemContextNoSearchAuthorization();
    if (searchClientShim == null) {
      return context;
    }
    return TestOperationContexts.withFixedSearchClient(context, searchClientShim);
  }

  @Bean
  @Primary
  @Qualifier("entityService")
  public EntityService<?> entityService() {
    return Mockito.mock(EntityService.class);
  }

  /**
   * {@code factory.common} loads {@link
   * com.linkedin.gms.factory.common.ElasticSearchGraphServiceFactory}, which now requires a
   * registry. This context does not scan {@code factory.search}, so provide a stub instead of a
   * live client against localhost:9200.
   */
  @Bean(name = "searchClusterRegistry")
  @Primary
  public SearchClusterRegistry searchClusterRegistry(
      @Autowired(required = false) SearchClientShim<?> searchClientShim) {
    SearchClientShim<?> client =
        searchClientShim != null ? searchClientShim : Mockito.mock(SearchClientShim.class);
    SearchClusterRegistry registry = Mockito.mock(SearchClusterRegistry.class);
    ESBulkProcessor bulkProcessor = Mockito.mock(ESBulkProcessor.class);
    ESIndexBuilder indexBuilder = Mockito.mock(ESIndexBuilder.class);
    ElasticSearchConfiguration config = Mockito.mock(ElasticSearchConfiguration.class);
    BulkProcessorConfiguration bulkConfig = Mockito.mock(BulkProcessorConfiguration.class);
    Mockito.when(bulkConfig.getNumRetries()).thenReturn(1);
    Mockito.when(config.getBulkProcessor()).thenReturn(bulkConfig);
    Mockito.doReturn(client).when(registry).clientFor(Mockito.any(SearchComponent.class));
    Mockito.when(registry.bulkProcessorFor(Mockito.any(SearchComponent.class)))
        .thenReturn(bulkProcessor);
    Mockito.when(registry.indexBuilderFor(Mockito.any(SearchComponent.class)))
        .thenReturn(indexBuilder);
    Mockito.when(registry.configFor(Mockito.any(SearchComponent.class))).thenReturn(config);
    return registry;
  }
}
