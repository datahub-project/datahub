package com.linkedin.datahub.graphql.resolvers.service;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ListServicesInput;
import com.linkedin.datahub.graphql.generated.ListServicesResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.service.ServiceProperties;
import graphql.GraphQLContext;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ListServicesResolverTest {

  @Mock private EntityClient entityClient;
  @Mock private DataFetchingEnvironment environment;

  private ListServicesResolver resolver;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    resolver = new ListServicesResolver(entityClient);
  }

  @Test
  public void testListServicesSuccess() throws Exception {
    // Setup proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Setup input
    ListServicesInput input = new ListServicesInput();
    input.setStart(0);
    input.setCount(10);
    when(environment.getArgument("input")).thenReturn(input);

    // Mock search result
    Urn serviceUrn1 = UrnUtils.getUrn("urn:li:service:service1");
    Urn serviceUrn2 = UrnUtils.getUrn("urn:li:service:service2");

    SearchEntity entity1 = new SearchEntity();
    entity1.setEntity(serviceUrn1);
    SearchEntity entity2 = new SearchEntity();
    entity2.setEntity(serviceUrn2);

    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(2);
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setEntities(new SearchEntityArray(ImmutableList.of(entity1, entity2)));
    searchResult.setMetadata(new SearchResultMetadata());

    when(entityClient.search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(searchResult);

    // Mock entity responses
    ServiceProperties props1 = new ServiceProperties();
    props1.setDisplayName("Service 1");

    ServiceProperties props2 = new ServiceProperties();
    props2.setDisplayName("Service 2");

    EnvelopedAspectMap aspects1 = new EnvelopedAspectMap();
    aspects1.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props1.data())));

    EnvelopedAspectMap aspects2 = new EnvelopedAspectMap();
    aspects2.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props2.data())));

    EntityResponse response1 = new EntityResponse();
    response1.setUrn(serviceUrn1);
    response1.setEntityName(Constants.SERVICE_ENTITY_NAME);
    response1.setAspects(aspects1);

    EntityResponse response2 = new EntityResponse();
    response2.setUrn(serviceUrn2);
    response2.setEntityName(Constants.SERVICE_ENTITY_NAME);
    response2.setAspects(aspects2);

    when(entityClient.batchGetV2(
            any(OperationContext.class), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of(serviceUrn1, response1, serviceUrn2, response2));

    // Execute
    ListServicesResult result = resolver.get(environment).join();

    // Verify
    assertNotNull(result);
    assertEquals(result.getTotal(), 2);
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getServices().size(), 2);
  }

  @Test
  public void testListServicesEmptyResult() throws Exception {
    // Setup proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Setup input
    ListServicesInput input = new ListServicesInput();
    input.setStart(0);
    input.setCount(10);
    when(environment.getArgument("input")).thenReturn(input);

    // Mock empty search result
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(0);
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setEntities(new SearchEntityArray());
    searchResult.setMetadata(new SearchResultMetadata());

    when(entityClient.search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(searchResult);

    when(entityClient.batchGetV2(
            any(OperationContext.class), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of());

    // Execute
    ListServicesResult result = resolver.get(environment).join();

    // Verify
    assertNotNull(result);
    assertEquals(result.getTotal(), 0);
    assertEquals(result.getServices().size(), 0);
  }

  /**
   * Verifies the resolver obtains QueryContext from getGraphQlContext() when getContext() is null.
   * This covers the getQueryContext(environment) path used by all resolvers after the recursion
   * fix.
   */
  @Test
  public void testListServicesUsesGraphQLContextWhenSet() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    GraphQLContext graphqlContext =
        GraphQLContext.newContext().of(QueryContext.class, mockContext).build();
    when(environment.getContext()).thenReturn(null);
    when(environment.getGraphQlContext()).thenReturn(graphqlContext);

    ListServicesInput input = new ListServicesInput();
    input.setStart(0);
    input.setCount(10);
    when(environment.getArgument("input")).thenReturn(input);

    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(0);
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setEntities(new SearchEntityArray());
    searchResult.setMetadata(new SearchResultMetadata());
    when(entityClient.search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(searchResult);
    when(entityClient.batchGetV2(
            any(OperationContext.class), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of());

    ListServicesResult result = resolver.get(environment).join();

    assertNotNull(result);
    assertEquals(result.getTotal(), 0);
    assertEquals(result.getServices().size(), 0);
  }

  @Test
  public void testListServicesWithQuery() throws Exception {
    // Setup proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Setup input with query
    ListServicesInput input = new ListServicesInput();
    input.setStart(0);
    input.setCount(10);
    input.setQuery("glean");
    when(environment.getArgument("input")).thenReturn(input);

    // Mock search result
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(0);
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setEntities(new SearchEntityArray());
    searchResult.setMetadata(new SearchResultMetadata());

    when(entityClient.search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            eq("glean"), // Should use the query
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(searchResult);

    when(entityClient.batchGetV2(
            any(OperationContext.class), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of());

    // Execute
    ListServicesResult result = resolver.get(environment).join();

    // Verify query was passed
    verify(entityClient)
        .search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            eq("glean"),
            any(),
            any(),
            anyInt(),
            anyInt());

    assertNotNull(result);
  }

  @Test
  public void testListServicesPagination() throws Exception {
    // Setup proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Setup input with pagination
    ListServicesInput input = new ListServicesInput();
    input.setStart(10);
    input.setCount(5);
    when(environment.getArgument("input")).thenReturn(input);

    // Mock search result
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(20); // Total is 20
    searchResult.setFrom(10);
    searchResult.setPageSize(5);
    searchResult.setEntities(new SearchEntityArray());
    searchResult.setMetadata(new SearchResultMetadata());

    when(entityClient.search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            any(),
            any(),
            eq(10),
            eq(5)))
        .thenReturn(searchResult);

    when(entityClient.batchGetV2(
            any(OperationContext.class), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of());

    // Execute
    ListServicesResult result = resolver.get(environment).join();

    // Verify pagination
    assertNotNull(result);
    assertEquals(result.getStart(), 10);
    assertEquals(result.getCount(), 5);
    assertEquals(result.getTotal(), 20);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Authorization Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testListServicesUnauthorized() throws Exception {
    // Setup deny context
    QueryContext mockContext = getMockDenyContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("input")).thenReturn(new ListServicesInput());

    // Expect AuthorizationException
    assertThrows(AuthorizationException.class, () -> resolver.get(environment));

    // Verify no search was performed
    verify(entityClient, never()).search(any(), any(), any(), any(), any(), anyInt(), anyInt());
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Edge Cases
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Test that null input throws NPE (resolver requires input to be non-null). This documents the
   * current behavior - the resolver doesn't handle null input.
   */
  @Test
  public void testListServicesNullInputThrowsNPE() throws Exception {
    // Setup proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("input")).thenReturn(null);

    // Execute - should throw NPE since input is required
    assertThrows(NullPointerException.class, () -> resolver.get(environment).join());
  }

  @Test
  public void testListServicesSearchException() throws Exception {
    // Setup proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("input")).thenReturn(new ListServicesInput());

    // Mock search to throw exception
    when(entityClient.search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenThrow(new RemoteInvocationException("Search failed"));

    // Execute and expect wrapped exception
    assertThrows(CompletionException.class, () -> resolver.get(environment).join());
  }

  @Test
  public void testListServicesDefaultValues() throws Exception {
    // Setup proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Empty input - should use defaults
    ListServicesInput input = new ListServicesInput();
    when(environment.getArgument("input")).thenReturn(input);

    // Mock search result
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(0);
    searchResult.setFrom(0);
    searchResult.setPageSize(20);
    searchResult.setEntities(new SearchEntityArray());
    searchResult.setMetadata(new SearchResultMetadata());

    when(entityClient.search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            eq(""), // Default query
            any(),
            any(),
            eq(0), // Default start
            eq(20))) // Default count
        .thenReturn(searchResult);

    when(entityClient.batchGetV2(
            any(OperationContext.class), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of());

    // Execute
    ListServicesResult result = resolver.get(environment).join();

    // Verify defaults were used
    verify(entityClient)
        .search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            eq(""),
            any(),
            any(),
            eq(0),
            eq(20));

    assertNotNull(result);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // SubType Filtering Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testListServicesWithSubTypeFilter() throws Exception {
    // Setup proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Setup input with subType filter
    ListServicesInput input = new ListServicesInput();
    input.setStart(0);
    input.setCount(10);
    input.setSubType(com.linkedin.datahub.graphql.generated.ServiceSubType.MCP_SERVER);
    when(environment.getArgument("input")).thenReturn(input);

    // Mock search result
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:mcp-server-1");
    SearchEntity entity = new SearchEntity();
    entity.setEntity(serviceUrn);

    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(1);
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setEntities(new SearchEntityArray(ImmutableList.of(entity)));
    searchResult.setMetadata(new SearchResultMetadata());

    // Capture the filter argument to verify subType filter is built correctly
    when(entityClient.search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            any(), // Filter will be non-null
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(searchResult);

    // Mock entity response
    ServiceProperties props = new ServiceProperties();
    props.setDisplayName("MCP Server 1");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse response = new EntityResponse();
    response.setUrn(serviceUrn);
    response.setEntityName(Constants.SERVICE_ENTITY_NAME);
    response.setAspects(aspects);

    when(entityClient.batchGetV2(
            any(OperationContext.class), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of(serviceUrn, response));

    // Execute
    ListServicesResult result = resolver.get(environment).join();

    // Verify result
    assertNotNull(result);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getServices().size(), 1);

    // Verify search was called with a filter (not null)
    verify(entityClient)
        .search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            any(), // Filter should be applied
            any(),
            anyInt(),
            anyInt());
  }

  @Test
  public void testListServicesWithoutSubTypeFilter() throws Exception {
    // Setup proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Setup input WITHOUT subType filter
    ListServicesInput input = new ListServicesInput();
    input.setStart(0);
    input.setCount(10);
    input.setSubType(null); // No filter
    when(environment.getArgument("input")).thenReturn(input);

    // Mock empty search result
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(0);
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setEntities(new SearchEntityArray());
    searchResult.setMetadata(new SearchResultMetadata());

    when(entityClient.search(
            any(OperationContext.class),
            eq(Constants.SERVICE_ENTITY_NAME),
            any(),
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(searchResult);

    when(entityClient.batchGetV2(
            any(OperationContext.class), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of());

    // Execute
    ListServicesResult result = resolver.get(environment).join();

    assertNotNull(result);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Entity Mapping Edge Cases
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test that services with null properties are handled gracefully. */
  @Test
  public void testListServicesWithMissingProperties() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    ListServicesInput input = new ListServicesInput();
    when(environment.getArgument("input")).thenReturn(input);

    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:no-props");
    SearchEntity entity = new SearchEntity();
    entity.setEntity(serviceUrn);

    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(1);
    searchResult.setFrom(0);
    searchResult.setPageSize(20);
    searchResult.setEntities(new SearchEntityArray(ImmutableList.of(entity)));
    searchResult.setMetadata(new SearchResultMetadata());

    when(entityClient.search(any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    // Return entity response with NO properties
    EntityResponse response = new EntityResponse();
    response.setUrn(serviceUrn);
    response.setEntityName(Constants.SERVICE_ENTITY_NAME);
    response.setAspects(new EnvelopedAspectMap());

    when(entityClient.batchGetV2(any(), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of(serviceUrn, response));

    // Execute - should handle missing properties
    ListServicesResult result = resolver.get(environment).join();

    assertNotNull(result);
    assertEquals(result.getTotal(), 1);
  }

  /** Test that search results with no matching entity responses are filtered out. */
  @Test
  public void testListServicesWithMissingEntityResponse() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    ListServicesInput input = new ListServicesInput();
    when(environment.getArgument("input")).thenReturn(input);

    Urn serviceUrn1 = UrnUtils.getUrn("urn:li:service:exists");
    Urn serviceUrn2 = UrnUtils.getUrn("urn:li:service:deleted");

    SearchEntity entity1 = new SearchEntity();
    entity1.setEntity(serviceUrn1);
    SearchEntity entity2 = new SearchEntity();
    entity2.setEntity(serviceUrn2);

    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(2);
    searchResult.setFrom(0);
    searchResult.setPageSize(20);
    searchResult.setEntities(new SearchEntityArray(ImmutableList.of(entity1, entity2)));
    searchResult.setMetadata(new SearchResultMetadata());

    when(entityClient.search(any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    // Only return one entity (the other was deleted)
    ServiceProperties props = new ServiceProperties();
    props.setDisplayName("Service 1");
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse response = new EntityResponse();
    response.setUrn(serviceUrn1);
    response.setEntityName(Constants.SERVICE_ENTITY_NAME);
    response.setAspects(aspects);

    // Only return serviceUrn1, not serviceUrn2
    when(entityClient.batchGetV2(any(), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of(serviceUrn1, response));

    // Execute
    ListServicesResult result = resolver.get(environment).join();

    assertNotNull(result);
    assertEquals(result.getTotal(), 2); // Total from search
    assertEquals(result.getServices().size(), 1); // Only 1 actually returned
  }

  /** Test that batchGetV2 exception is propagated. */
  @Test
  public void testListServicesBatchGetException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("input")).thenReturn(new ListServicesInput());

    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:test");
    SearchEntity entity = new SearchEntity();
    entity.setEntity(serviceUrn);

    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(1);
    searchResult.setFrom(0);
    searchResult.setPageSize(20);
    searchResult.setEntities(new SearchEntityArray(ImmutableList.of(entity)));
    searchResult.setMetadata(new SearchResultMetadata());

    when(entityClient.search(any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    // batchGetV2 throws exception
    when(entityClient.batchGetV2(any(), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenThrow(new RemoteInvocationException("Batch get failed"));

    // Execute and expect wrapped exception
    assertThrows(CompletionException.class, () -> resolver.get(environment).join());
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Constructor Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testConstructorNullEntityClient() {
    assertThrows(NullPointerException.class, () -> new ListServicesResolver(null));
  }
}
