package io.datahubproject.openapi.config;

import static io.datahubproject.openapi.delegates.DatahubUsageEventsImpl.DATAHUB_USAGE_INDEX;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.models.registry.*;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.Optional;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchResponse;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class OpenAPIAnalyticsTestConfiguration {
  @MockBean TracingInterceptor tracingInterceptor;

  @Bean(name = "systemOperationContext")
  public OperationContext systemOperationContext() {
    return TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Bean
  @Primary
  public ElasticSearchService datahubUsageEventsApiDelegate() throws IOException {
    ElasticSearchService elasticSearchService = mock(ElasticSearchService.class);
    SearchResponse mockResp = mock(SearchResponse.class);
    when(elasticSearchService.raw(
            any(OperationContext.class), eq(DATAHUB_USAGE_INDEX), anyString()))
        .thenReturn(Optional.of(mockResp));
    return elasticSearchService;
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

  @Bean("entityRegistry")
  @Primary
  public EntityRegistry entityRegistry() throws EntityRegistryException, InterruptedException {
    return new ConfigEntityRegistry(
        OpenAPIAnalyticsTestConfiguration.class
            .getClassLoader()
            .getResourceAsStream("entity-registry.yml"));
  }
}
