package com.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.GraphQLConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLQueryConfiguration;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import com.linkedin.metadata.ratelimit.model.RateLimitSource;
import com.linkedin.metadata.usage.instrumentation.UsageMetricsSessionEnricher;
import com.linkedin.metadata.usage.store.UsageAggregationStore;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphQLControllerRateLimitOrderTest {

  private GraphQLController controller;
  private RateLimitEngine rateLimitEngine;
  private UsageAggregationStore usageRollupStore;
  private MockedStatic<AuthenticationContext> authenticationContextMock;

  @BeforeMethod
  public void setUp() throws Exception {
    controller = new GraphQLController();
    rateLimitEngine = mock(RateLimitEngine.class);
    usageRollupStore = mock(UsageAggregationStore.class);
    UsageMetricsSessionEnricher enricher = new UsageMetricsSessionEnricher(usageRollupStore, true);

    OperationContext systemContext = TestOperationContexts.systemContextNoValidate();
    ConfigurationProvider configurationProvider = new ConfigurationProvider();
    GraphQLConfiguration graphQL = new GraphQLConfiguration();
    GraphQLQueryConfiguration queryConfig = new GraphQLQueryConfiguration();
    queryConfig.setMaxVisitedUrns(100);
    queryConfig.setMaxParentDepth(5);
    graphQL.setQuery(queryConfig);
    configurationProvider.setGraphQL(graphQL);

    controller._engine = mock(GraphQLEngine.class);
    controller._authorizerChain = mock(AuthorizerChain.class);
    controller.configurationProvider = configurationProvider;
    controller.metricUtils = mock(MetricUtils.class);
    controller.rateLimitEngine = rateLimitEngine;
    controller.graphqlUsageClassificationRegistry = mock(GraphqlUsageClassificationRegistry.class);
    controller.usageMetricsSessionEnricher = enricher;
    setSystemOperationContext(controller, systemContext);

    authenticationContextMock = Mockito.mockStatic(AuthenticationContext.class);
    authenticationContextMock
        .when(AuthenticationContext::getAuthentication)
        .thenReturn(new Authentication(new Actor(ActorType.USER, "datahub"), "test"));
  }

  @AfterMethod
  public void tearDown() {
    authenticationContextMock.close();
  }

  @Test
  public void testRateLimitedRequestDoesNotRecordUsage() {
    when(rateLimitEngine.evaluateAndAcquireGraphQL(
            anyString(), anyString(), anyString(), any(), any()))
        .thenReturn(
            RateLimitDecision.builder()
                .allowed(false)
                .source(RateLimitSource.GRAPHQL_GATE)
                .retryAfterSeconds(60)
                .build());

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRequestURI()).thenReturn("/api/graphql");
    when(request.getMethod()).thenReturn("POST");

    String body =
        "{\"query\":\"query { dataset(urn: \\\"urn:li:dataset:(a,b,c)\\\") { urn } }\","
            + "\"operationName\":\"dataset\"}";
    HttpEntity<String> entity = new HttpEntity<>(body);

    CompletableFuture<ResponseEntity<String>> future = controller.postGraphQL(request, entity);
    ResponseEntity<String> response = future.join();

    assertEquals(response.getStatusCode(), HttpStatus.TOO_MANY_REQUESTS);
    verify(usageRollupStore, never()).recordRequest(any());
  }

  private static void setSystemOperationContext(
      GraphQLController controller, OperationContext systemContext) throws Exception {
    Field field = GraphQLController.class.getDeclaredField("systemOperationContext");
    field.setAccessible(true);
    field.set(controller, systemContext);
  }
}
