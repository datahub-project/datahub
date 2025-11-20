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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

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

  @MockBean(name = INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @MockBean(name = "entityService")
  private EntityService<?> entityService;

  @MockBean(name = "graphClient")
  private GraphClient graphClient;

  @MockBean private GraphService graphService;

  @MockBean(name = "searchService")
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

  @MockBean(name = "deleteEntityService")
  private DeleteEntityService deleteEntityService;

  @MockBean(name = "entitySearchService")
  private EntitySearchService entitySearchService;

  @MockBean(name = "cachingEntitySearchService")
  private CachingEntitySearchService cachingEntitySearchService;

  @MockBean(name = "timeseriesAspectService")
  private TimeseriesAspectService timeseriesAspectService;

  @MockBean(name = "relationshipSearchService")
  private LineageSearchService lineageSearchService;

  @MockBean(name = "kafkaEventProducer")
  private EventProducer eventProducer;

  @MockBean private RollbackService rollbackService;

  @MockBean private SystemTelemetryContext systemTelemetryContext;

  @MockBean private MetricUtils metricUtils;

  @MockBean private RestrictedService restrictedService;

  @MockBean(name = "systemEntityClient")
  private SystemEntityClient systemEntityClient;

  @MockBean(name = "dataHubSecretService")
  private SecretService service;

  @MockBean(name = "searchClientShim")
  private SearchClientShim<?> searchClientShim;
}
