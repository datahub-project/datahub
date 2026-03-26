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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

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

  @MockitoBean(name = INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @MockitoBean(name = "entityService")
  private EntityService<?> entityService;

  @MockitoBean(name = "graphClient")
  private GraphClient graphClient;

  @MockitoBean private GraphService graphService;

  @MockitoBean(name = "searchService")
  private SearchService searchService;

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

  @MockitoBean(name = "deleteEntityService")
  private DeleteEntityService deleteEntityService;

  @MockitoBean(name = "entitySearchService")
  private EntitySearchService entitySearchService;

  @MockitoBean(name = "cachingEntitySearchService")
  private CachingEntitySearchService cachingEntitySearchService;

  @MockitoBean(name = "timeseriesAspectService")
  private TimeseriesAspectService timeseriesAspectService;

  @MockitoBean(name = "relationshipSearchService")
  private LineageSearchService lineageSearchService;

  @MockitoBean(name = "kafkaEventProducer")
  private EventProducer eventProducer;

  @MockitoBean private RollbackService rollbackService;

  @MockitoBean private SystemTelemetryContext systemTelemetryContext;

  @MockitoBean private MetricUtils metricUtils;

  @MockitoBean private RestrictedService restrictedService;

  @MockitoBean(name = "systemEntityClient")
  private SystemEntityClient systemEntityClient;

  @MockitoBean(name = "dataHubSecretService")
  private SecretService service;

  @MockitoBean(name = "searchClientShim")
  private SearchClientShim<?> searchClientShim;
}
