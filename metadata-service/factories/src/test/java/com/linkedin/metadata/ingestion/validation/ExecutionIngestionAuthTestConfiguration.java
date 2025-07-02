package com.linkedin.metadata.ingestion.validation;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(
    basePackages = {
      "com.linkedin.gms.factory.config",
      "com.linkedin.gms.factory.context",
      "com.linkedin.gms.factory.auth",
      "com.linkedin.gms.factory.entityclient"
    })
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

  @Qualifier("entityService")
  @MockBean
  private EntityService<?> entityService;

  @Qualifier("graphClient")
  @MockBean
  private GraphClient graphClient;

  @MockBean private GraphService graphService;

  @Qualifier("searchService")
  @MockBean
  private SearchService searchService;

  @Qualifier("baseElasticSearchComponents")
  @MockBean
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents
      baseElasticSearchComponents;

  @Qualifier("deleteEntityService")
  @MockBean
  private DeleteEntityService deleteEntityService;

  @Qualifier("entitySearchService")
  @MockBean
  private EntitySearchService entitySearchService;

  @Qualifier("cachingEntitySearchService")
  @MockBean
  private CachingEntitySearchService cachingEntitySearchService;

  @Qualifier("timeseriesAspectService")
  @MockBean
  private TimeseriesAspectService timeseriesAspectService;

  @Qualifier("relationshipSearchService")
  @MockBean
  private LineageSearchService lineageSearchService;

  @Qualifier("kafkaEventProducer")
  @MockBean
  private EventProducer eventProducer;

  @MockBean private RollbackService rollbackService;

  @MockBean private io.datahubproject.metadata.context.TraceContext traceContext;
}
