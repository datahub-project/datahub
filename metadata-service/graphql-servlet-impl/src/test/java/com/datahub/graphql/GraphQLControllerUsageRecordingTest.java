package com.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.GraphQLConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLQueryConfiguration;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import com.linkedin.metadata.ratelimit.model.RateLimitLease;
import com.linkedin.metadata.ratelimit.model.RateLimitSource;
import com.linkedin.metadata.usage.instrumentation.UsageMetricsSessionEnricher;
import com.linkedin.metadata.usage.registry.graphql.GraphqlUsageClassificationRegistryBuilder;
import com.linkedin.metadata.usage.store.UsageAggregationStore;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphQLControllerUsageRecordingTest {

  private static final String ME_QUERY =
      "query smokeUsageAggregationMe { me { corpUser { urn } } }";

  private GraphQLController controller;
  private RateLimitEngine rateLimitEngine;
  private UsageAggregationStore usageRollupStore;
  private MockedStatic<AuthenticationContext> authenticationContextMock;

  @BeforeMethod
  public void setUp() throws Exception {
    controller = new GraphQLController();
    rateLimitEngine = mock(RateLimitEngine.class);
    usageRollupStore = mock(UsageAggregationStore.class);
    when(usageRollupStore.recordRequest(any())).thenReturn(true);

    UsageMetricsSessionEnricher enricher = new UsageMetricsSessionEnricher(usageRollupStore, true);
    OperationContext systemContext =
        TestOperationContexts.Builder.builder()
            .configSupplier(
                () -> OperationContextConfig.builder().sessionContextEnricher(enricher).build())
            .systemTelemetryContextSupplier(() -> SystemTelemetryContext.TEST.toBuilder().build())
            .buildSystemContext();

    ConfigurationProvider configurationProvider = new ConfigurationProvider();
    GraphQLConfiguration graphQL = new GraphQLConfiguration();
    GraphQLQueryConfiguration queryConfig = new GraphQLQueryConfiguration();
    queryConfig.setMaxVisitedUrns(100);
    queryConfig.setMaxParentDepth(5);
    graphQL.setQuery(queryConfig);
    configurationProvider.setGraphQL(graphQL);

    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    GraphqlUsageClassificationRegistry registry =
        GraphqlUsageClassificationRegistryBuilder.fromManifest(
            new UsageOperationsLoader(yamlMapper).loadBundled());

    GraphQLEngine engine = mock(GraphQLEngine.class);
    controller._engine = engine;
    controller._authorizerChain = mock(AuthorizerChain.class);
    controller.configurationProvider = configurationProvider;
    controller.metricUtils = mock(MetricUtils.class);
    controller.rateLimitEngine = rateLimitEngine;
    controller.graphqlUsageClassificationRegistry = registry;
    controller.usageMetricsSessionEnricher = enricher;
    setSystemOperationContext(controller, systemContext);

    authenticationContextMock = Mockito.mockStatic(AuthenticationContext.class);
    authenticationContextMock
        .when(AuthenticationContext::getAuthentication)
        .thenReturn(new Authentication(new Actor(ActorType.USER, "datahub"), "test"));

    when(rateLimitEngine.evaluateAndAcquireGraphQL(
            anyString(), anyString(), anyString(), any(), any()))
        .thenReturn(
            RateLimitDecision.builder().allowed(true).source(RateLimitSource.GRAPHQL_GATE).build());
    when(rateLimitEngine.toLease(any())).thenReturn(mock(RateLimitLease.class));

    Map<String, Object> data =
        Map.of("me", Map.of("corpUser", Map.of("urn", "urn:li:corpuser:datahub")));
    ExecutionResult executionResult =
        new ExecutionResultImpl(data, Collections.emptyList(), Collections.emptyMap());
    when(engine.execute(anyString(), any(), anyMap(), any())).thenReturn(executionResult);
  }

  @AfterMethod
  public void tearDown() {
    authenticationContextMock.close();
  }

  @Test
  public void testSuccessfulGraphqlRequestRecordsRequestOnceAndOutputBytesOnce() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRequestURI()).thenReturn("/api/graphql");
    when(request.getMethod()).thenReturn("POST");
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    when(request.getHeader(anyString())).thenReturn(null);
    when(request.getContentLengthLong()).thenReturn(-1L);

    String body =
        "{\"query\":\""
            + ME_QUERY.replace("\n", " ")
            + "\",\"operationName\":\"smokeUsageAggregationMe\"}";
    HttpEntity<String> entity = new HttpEntity<>(body);

    CompletableFuture<ResponseEntity<String>> future = controller.postGraphQL(request, entity);
    ResponseEntity<String> response = future.join();

    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertTrue(response.getBody() != null && !response.getBody().isEmpty());

    verify(usageRollupStore, times(1)).recordRequest(any());

    ArgumentCaptor<Long> outputBytesCaptor = ArgumentCaptor.forClass(Long.class);
    verify(usageRollupStore, times(1)).recordResponse(any(), outputBytesCaptor.capture());
    Long recordedBytes = outputBytesCaptor.getValue();
    assertTrue(recordedBytes != null && recordedBytes > 0);
    assertEquals(recordedBytes.longValue(), response.getBody().length());
  }

  private static void setSystemOperationContext(
      GraphQLController controller, OperationContext systemContext) throws Exception {
    Field field = GraphQLController.class.getDeclaredField("systemOperationContext");
    field.setAccessible(true);
    field.set(controller, systemContext);
  }
}
