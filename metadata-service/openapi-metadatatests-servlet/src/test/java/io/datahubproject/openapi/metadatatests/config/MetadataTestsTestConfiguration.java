package io.datahubproject.openapi.metadatatests.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.throttle.ManualThrottleSensor;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.events.ExternalEventsController;
import io.datahubproject.openapi.generated.ScrollTestEntityResponseV2;
import io.datahubproject.openapi.generated.TestEntityRequestV2;
import io.datahubproject.openapi.generated.TestEntityResponseV2;
import io.datahubproject.openapi.health.HealthCheckController;
import io.datahubproject.openapi.operations.elastic.ElasticsearchController;
import io.datahubproject.openapi.v2.controller.RelationshipController;
import io.datahubproject.openapi.v2.controller.TimelineControllerV2;
import io.datahubproject.openapi.v2.delegates.EntityApiDelegateImpl;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.concurrent.ExecutorService;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@EnableWebMvc
@TestConfiguration
@Import(ConfigurationProvider.class)
public class MetadataTestsTestConfiguration {

  @MockBean TraceContext traceContext;

  @Bean
  public TracingInterceptor tracingInterceptor(final TraceContext traceContext) {
    return new TracingInterceptor(traceContext);
  }

  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper(new YAMLFactory());
  }

  @MockBean public EntityServiceImpl entityService;

  @Bean(name = "systemOperationContext")
  public OperationContext systemOperationContext() {
    return TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @MockBean public SearchService searchService;

  @MockBean public EntitySearchService entitySearchService;

  @MockBean public QueryEngine queryEngine;

  @MockBean public ActionApplier actionApplier;

  @MockBean public ElasticSearchGraphService graphService;

  @Bean
  public AuthorizerChain authorizerChain() {
    AuthorizerChain authorizerChain = Mockito.mock(AuthorizerChain.class);

    Authentication authentication = Mockito.mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    when(authorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
    AuthenticationContext.setAuthentication(authentication);

    return authorizerChain;
  }

  @MockBean(name = "elasticSearchSystemMetadataService")
  public SystemMetadataService systemMetadataService;

  @MockBean public TimelineService timelineService;

  @MockBean public CachingEntitySearchService cachingEntitySearchService;

  @MockBean public TimeseriesAspectService timeseriesAspectService;

  @Bean("entityRegistry")
  @Primary
  public EntityRegistry entityRegistry() throws EntityRegistryException {
    ConfigEntityRegistry standard =
        new ConfigEntityRegistry(
            MetadataTestsTestConfiguration.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));
    MergedEntityRegistry entityRegistry =
        new MergedEntityRegistry(SnapshotEntityRegistry.getInstance()).apply(standard);

    return entityRegistry;
  }

  @Bean
  public boolean restApiAuthorizationEnabled() {
    return false;
  }

  /* Controllers not under this module */
  @MockBean
  public EntityApiDelegateImpl<
          TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2>
      entityApiDelegate;

  @MockBean public TimelineControllerV2 timelineController;
  @MockBean public RelationshipController relationshipsController;
  @MockBean public HealthCheckController healthCheckController;
  @MockBean public ElasticsearchController operationsController;
  @MockBean public ExternalEventsController externalEventsController;
  @MockBean public TestEngine testEngine;
  @MockBean public ManualThrottleSensor manualThrottleSensor;

  @Qualifier("metadataTestsActionsExecutorService")
  @MockBean
  public ExecutorService metadataTestsActionsExecutorService;
}
