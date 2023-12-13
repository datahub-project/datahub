package io.datahubproject.openapi.metadatatests.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.timeline.TimelineService;
import io.datahubproject.openapi.delegates.EntityApiDelegateImpl;
import io.datahubproject.openapi.generated.ScrollTestEntityResponseV2;
import io.datahubproject.openapi.generated.TestEntityRequestV2;
import io.datahubproject.openapi.generated.TestEntityResponseV2;
import io.datahubproject.openapi.health.HealthCheckController;
import io.datahubproject.openapi.relationships.RelationshipsController;
import io.datahubproject.openapi.timeline.TimelineController;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class MetadataTestsTestConfiguration {

  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper(new YAMLFactory());
  }

  @Bean
  @Primary
  public EntityService entityService(final EntityRegistry mockRegistry) {
    EntityService entityService = mock(EntityServiceImpl.class);
    when(entityService.getEntityRegistry()).thenReturn(mockRegistry);
    return entityService;
  }

  @MockBean public SearchService searchService;

  @MockBean public EntitySearchService entitySearchService;

  @MockBean public QueryEngine queryEngine;

  @MockBean public ActionApplier actionApplier;

  @MockBean public GraphService graphService;

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

  /* Controllers not under this module */
  @MockBean
  public EntityApiDelegateImpl<
          TestEntityRequestV2, TestEntityResponseV2, ScrollTestEntityResponseV2>
      entityApiDelegate;

  @MockBean public TimelineController timelineController;

  @MockBean public RelationshipsController relationshipsController;

  @MockBean public HealthCheckController healthCheckController;
}
