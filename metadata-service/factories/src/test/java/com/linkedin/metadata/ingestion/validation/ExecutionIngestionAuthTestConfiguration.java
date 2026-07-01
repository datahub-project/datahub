package com.linkedin.metadata.ingestion.validation;

import static com.linkedin.gms.factory.common.IndexConventionFactory.INDEX_CONVENTION_BEAN;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.context.SystemOperationContextFactory;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.MappingsBuilderFactory;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.metadata.services.SecretService;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

// All dependencies are @Bean methods rather than @MockitoBean fields because @MockitoBean on a
// @Configuration class requires an existing bean definition to override — but in this test
// context many factories are not loaded, so no definitions exist for these types/names.
@Configuration
@ComponentScan(
    basePackages = {
      "com.linkedin.gms.factory.config",
      "com.linkedin.gms.factory.auth",
      "com.linkedin.gms.factory.entityclient",
    })
@Import({MappingsBuilderFactory.class, SystemOperationContextFactory.class})
public class ExecutionIngestionAuthTestConfiguration {

  @Bean
  public EntityRegistry entityRegistry() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    return new ConfigEntityRegistry(
        ExecutionIngestionAuthSystemUserTest.class
            .getClassLoader()
            .getResourceAsStream("entity-registry.yml"));
  }

  @Bean(name = INDEX_CONVENTION_BEAN)
  @Primary
  public IndexConvention indexConvention() {
    return Mockito.mock(IndexConvention.class);
  }

  @Bean(name = "entityService")
  @Primary
  @SuppressWarnings("unchecked")
  public EntityService<?> entityService() {
    return Mockito.mock(EntityService.class);
  }

  @Bean(name = "graphClient")
  @Primary
  public GraphClient graphClient() {
    return Mockito.mock(GraphClient.class);
  }

  @Bean
  @Primary
  public GraphService graphService() {
    return Mockito.mock(GraphService.class);
  }

  @Bean(name = "searchService")
  @Primary
  public SearchService searchService() {
    return Mockito.mock(SearchService.class);
  }

  @Bean(name = "baseElasticSearchComponents")
  public BaseElasticSearchComponentsFactory.BaseElasticSearchComponents baseElasticSearchComponents(
      @Qualifier(INDEX_CONVENTION_BEAN) IndexConvention mockIndexConvention) {
    return new BaseElasticSearchComponentsFactory.BaseElasticSearchComponents(
        null, // config
        null, // searchClient
        mockIndexConvention,
        null, // bulkProcessor
        null // indexBuilder
        );
  }

  @Bean(name = "deleteEntityService")
  @Primary
  public DeleteEntityService deleteEntityService() {
    return Mockito.mock(DeleteEntityService.class);
  }

  @Bean(name = "entitySearchService")
  @Primary
  public EntitySearchService entitySearchService() {
    return Mockito.mock(EntitySearchService.class);
  }

  @Bean(name = "cachingEntitySearchService")
  @Primary
  public CachingEntitySearchService cachingEntitySearchService() {
    return Mockito.mock(CachingEntitySearchService.class);
  }

  @Bean(name = "timeseriesAspectService")
  @Primary
  public TimeseriesAspectService timeseriesAspectService() {
    return Mockito.mock(TimeseriesAspectService.class);
  }

  @Bean(name = "relationshipSearchService")
  @Primary
  public LineageSearchService lineageSearchService() {
    return Mockito.mock(LineageSearchService.class);
  }

  @Bean(name = "kafkaEventProducer")
  @Primary
  public EventProducer eventProducer() {
    return Mockito.mock(EventProducer.class);
  }

  @Bean
  @Primary
  public RollbackService rollbackService() {
    return Mockito.mock(RollbackService.class);
  }

  @Bean
  @Primary
  public SystemTelemetryContext systemTelemetryContext() {
    return Mockito.mock(SystemTelemetryContext.class);
  }

  @Bean
  @Primary
  public MetricUtils metricUtils() {
    return Mockito.mock(MetricUtils.class);
  }

  @Bean
  @Primary
  public RestrictedService restrictedService() {
    return Mockito.mock(RestrictedService.class);
  }

  @Bean(name = "systemEntityClient")
  @Primary
  public SystemEntityClient systemEntityClient() {
    return Mockito.mock(SystemEntityClient.class);
  }

  @Bean(name = "dataHubSecretService")
  @Primary
  public SecretService dataHubSecretService() {
    return Mockito.mock(SecretService.class);
  }

  @Bean(name = "searchClientShim")
  @Primary
  @SuppressWarnings("unchecked")
  public SearchClientShim<?> searchClientShim() {
    SearchClientShim<?> mock = Mockito.mock(SearchClientShim.class);
    Mockito.when(mock.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    return mock;
  }
}
