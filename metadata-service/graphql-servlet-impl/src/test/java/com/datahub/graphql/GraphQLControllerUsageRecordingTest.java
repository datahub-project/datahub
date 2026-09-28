package com.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.concurrent.ConcurrentHashMap;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.async.DeferredResultProcessingInterceptor;
import org.springframework.web.context.request.async.WebAsyncUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphQLControllerUsageRecordingTest {

  private static final String ME_QUERY =
      "query smokeUsageAggregationMe { me { corpUser { urn } } }";

  private GraphQLController controller;
  private RateLimitEngine rateLimitEngine;
  private RateLimitLease rateLimitLease;
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
    controller.aspectMappingRegistry =
        mock(com.linkedin.datahub.graphql.AspectMappingRegistry.class);
    controller.graphQLResponseMapper = new ObjectMapper();
    setSystemOperationContext(controller, systemContext);

    authenticationContextMock = Mockito.mockStatic(AuthenticationContext.class);
    authenticationContextMock
        .when(AuthenticationContext::getAuthentication)
        .thenReturn(new Authentication(new Actor(ActorType.USER, "datahub"), "test"));

    when(rateLimitEngine.evaluateAndAcquireGraphQL(
            anyString(), anyString(), anyString(), any(), any()))
        .thenReturn(
            RateLimitDecision.builder().allowed(true).source(RateLimitSource.GRAPHQL_GATE).build());
    rateLimitLease = mock(RateLimitLease.class);
    when(rateLimitEngine.toLease(any())).thenReturn(rateLimitLease);

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
  public void testBufferedResponseRecordsRequestOnceAndOutputBytesOnce() {
    // gate defaults off → buffered String path.
    ResponseEntity<Object> response = executeMeQuery();

    assertEquals(response.getStatusCode(), HttpStatus.OK);
    Object body = response.getBody();
    assertTrue(body instanceof String && !((String) body).isEmpty());

    verify(usageRollupStore, times(1)).recordRequest(any());

    ArgumentCaptor<Long> outputBytesCaptor = ArgumentCaptor.forClass(Long.class);
    verify(usageRollupStore, times(1)).recordResponse(any(), outputBytesCaptor.capture());
    assertEquals(outputBytesCaptor.getValue().longValue(), ((String) body).length());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStreamingResponseDefersByteRecordingAndLeaseReleaseToConverter()
      throws Exception {
    controller.configurationProvider.getGraphQL().getQuery().setStreamResponse(true);

    HttpServletRequest request = request();
    ResponseEntity<Object> response = executeMeQuery(request);

    assertEquals(response.getStatusCode(), HttpStatus.OK);
    Object body = response.getBody();
    assertTrue(body instanceof GraphQLResponseBody);
    GraphQLResponseBody streamed = (GraphQLResponseBody) body;

    // Marker carries the full response tree; the converter serializes it later.
    Map<String, Object> data = (Map<String, Object>) streamed.spec().get("data");
    Map<String, Object> corpUser =
        (Map<String, Object>) ((Map<String, Object>) data.get("me")).get("corpUser");
    assertEquals(corpUser.get("urn"), "urn:li:corpuser:datahub");

    // Bytes aren't known until the converter writes, so the byte metric hasn't fired yet.
    verify(usageRollupStore, times(1)).recordRequest(any());
    verify(usageRollupStore, never()).recordResponse(any(), anyLong());
    verify(rateLimitEngine, never()).release(any(), anyBoolean());

    streamed.onBytesWritten().accept(4242L);
    ArgumentCaptor<Long> outputBytesCaptor = ArgumentCaptor.forClass(Long.class);
    verify(usageRollupStore, times(1)).recordResponse(any(), outputBytesCaptor.capture());
    assertEquals(outputBytesCaptor.getValue().longValue(), 4242L);

    streamed.onWriteFinished().accept(true);
    verify(rateLimitEngine, times(1)).release(rateLimitLease, true);

    // Normal request after-completion is a safety net; it must not double-release.
    DeferredResultProcessingInterceptor interceptor =
        WebAsyncUtils.getAsyncManager(request)
            .getDeferredResultInterceptor(GraphQLController.RATE_LIMIT_RELEASE_INTERCEPTOR_KEY);
    assertTrue(interceptor != null);
    interceptor.afterCompletion(null, null);
    verify(rateLimitEngine, times(1)).release(rateLimitLease, true);
  }

  @Test
  public void testStreamingAsyncCompletionReleasesLeaseWhenConverterNeverRuns() throws Exception {
    controller.configurationProvider.getGraphQL().getQuery().setStreamResponse(true);

    HttpServletRequest request = request();
    ResponseEntity<Object> response = executeMeQuery(request);
    assertTrue(response.getBody() instanceof GraphQLResponseBody);
    verify(rateLimitEngine, never()).release(any(), anyBoolean());

    DeferredResultProcessingInterceptor interceptor =
        WebAsyncUtils.getAsyncManager(request)
            .getDeferredResultInterceptor(GraphQLController.RATE_LIMIT_RELEASE_INTERCEPTOR_KEY);
    assertTrue(interceptor != null);
    interceptor.afterCompletion(null, null);

    verify(rateLimitEngine, times(1)).release(rateLimitLease, false);

    // A late converter callback after timeout must not double-release.
    ((GraphQLResponseBody) response.getBody()).onWriteFinished().accept(true);
    verify(rateLimitEngine, times(1)).release(rateLimitLease, false);
  }

  @Test
  public void testStreamingFailedWriteReleasesLeaseExactlyOnce() throws Exception {
    controller.configurationProvider.getGraphQL().getQuery().setStreamResponse(true);

    HttpServletRequest request = request();
    GraphQLResponseBody streamed = (GraphQLResponseBody) executeMeQuery(request).getBody();
    verify(rateLimitEngine, never()).release(any(), anyBoolean());

    // Both client abort and serialization failure are unsuccessful writes.
    streamed.onWriteFinished().accept(false);
    verify(rateLimitEngine, times(1)).release(rateLimitLease, false);

    // Neither a duplicate callback nor request after-completion may corrupt limiter inflight state.
    streamed.onWriteFinished().accept(false);
    WebAsyncUtils.getAsyncManager(request)
        .getDeferredResultInterceptor(GraphQLController.RATE_LIMIT_RELEASE_INTERCEPTOR_KEY)
        .afterCompletion(null, null);
    verify(rateLimitEngine, times(1)).release(rateLimitLease, false);
  }

  @Test
  public void testBufferedSerializationFailureReturnsServiceUnavailable() throws Exception {
    // Buffered path (gate off): if serialization throws, the response is 503, not a broken body.
    ObjectMapper failing = mock(ObjectMapper.class);
    when(failing.writeValueAsString(any())).thenThrow(new IllegalArgumentException("boom"));
    controller.graphQLResponseMapper = failing;

    ResponseEntity<Object> response = executeMeQuery();

    assertEquals(response.getStatusCode(), HttpStatus.SERVICE_UNAVAILABLE);
  }

  private ResponseEntity<Object> executeMeQuery() {
    return executeMeQuery(request());
  }

  private ResponseEntity<Object> executeMeQuery(HttpServletRequest request) {
    String body =
        "{\"query\":\""
            + ME_QUERY.replace("\n", " ")
            + "\",\"operationName\":\"smokeUsageAggregationMe\"}";
    HttpEntity<String> entity = new HttpEntity<>(body);

    return controller.postGraphQL(request, entity).join();
  }

  private static HttpServletRequest request() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    Map<String, Object> attributes = new ConcurrentHashMap<>();
    when(request.getRequestURI()).thenReturn("/api/graphql");
    when(request.getMethod()).thenReturn("POST");
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    when(request.getAttribute(anyString()))
        .thenAnswer(invocation -> attributes.get(invocation.getArgument(0)));
    doAnswer(
            invocation -> {
              attributes.put(invocation.getArgument(0), invocation.getArgument(1));
              return null;
            })
        .when(request)
        .setAttribute(anyString(), any());
    return request;
  }

  private static void setSystemOperationContext(
      GraphQLController controller, OperationContext systemContext) throws Exception {
    Field field = GraphQLController.class.getDeclaredField("systemOperationContext");
    field.setAccessible(true);
    field.set(controller, systemContext);
  }
}
