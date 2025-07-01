package io.datahubproject.openapi.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.nullable;
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
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.models.registry.PluginEntityRegistryLoader;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeline.TimelineService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.openapi.dto.UrnResponseMap;
import io.datahubproject.openapi.generated.EntityResponse;
import io.datahubproject.openapi.v1.entities.EntitiesController;
import io.datahubproject.openapi.v1.relationships.RelationshipsController;
import io.datahubproject.openapi.v2.controller.TimelineControllerV2;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.ResponseEntity;

@TestConfiguration
public class OpenAPIEntityTestConfiguration {
  @MockBean TraceContext traceContext;

  @Bean
  public TracingInterceptor tracingInterceptor(final TraceContext traceContext) {
    return new TracingInterceptor(traceContext);
  }

  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper(new YAMLFactory());
  }

  @MockBean EntityService<?> entityService;

  @Bean
  @Primary
  public SearchService searchService() {
    SearchService searchService = mock(SearchService.class);
    when(searchService.scrollAcrossEntities(
            any(OperationContext.class), anyList(), any(), any(), any(), any(), any(), anyInt()))
        .thenReturn(new ScrollResult().setEntities(new SearchEntityArray()));

    return searchService;
  }

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

  @Bean("entityRegistry")
  @Primary
  public EntityRegistry entityRegistry() throws EntityRegistryException, InterruptedException {
    /*
      Considered a few different approach to loading a custom model. Chose this method
      to as closely match a production configuration rather than direct project to project
      dependency.
    */
    PluginEntityRegistryLoader custom =
        new PluginEntityRegistryLoader(getClass().getResource("/custom-model").getFile(), 60, null);

    ConfigEntityRegistry standard =
        new ConfigEntityRegistry(
            OpenAPIEntityTestConfiguration.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));
    MergedEntityRegistry entityRegistry =
        new MergedEntityRegistry(SnapshotEntityRegistry.getInstance()).apply(standard);
    custom.withBaseRegistry(entityRegistry).start(true);

    return entityRegistry;
  }

  /* Controllers not under this module */
  @Bean
  @Primary
  public EntitiesController entitiesController() {
    EntitiesController entitiesController = mock(EntitiesController.class);
    when(entitiesController.getEntities(nullable(HttpServletRequest.class), any(), any()))
        .thenAnswer(
            params -> {
              String[] urns = params.getArgument(1);
              String[] aspects = params.getArgument(2);
              return ResponseEntity.ok(
                  UrnResponseMap.builder()
                      .responses(
                          Arrays.stream(urns)
                              .map(urn -> Map.entry(urn, EntityResponse.builder().urn(urn).build()))
                              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                      .build());
            });

    return entitiesController;
  }

  @MockBean public TimelineControllerV2 timelineControllerV2;

  @MockBean public RelationshipsController relationshipsController;

  @Bean(name = "systemOperationContext")
  public OperationContext operationContext(final EntityRegistry entityRegistry) {
    return TestOperationContexts.systemContextNoSearchAuthorization(entityRegistry);
  }
}
