package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;
import static org.testng.AssertJUnit.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListRemoteExecutorPoolsInput;
import com.linkedin.datahub.graphql.generated.ListRemoteExecutorPoolsResult;
import com.linkedin.datahub.graphql.generated.RemoteExecutorPool;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ListRemoteExecutorPoolsResolverTest {

  @Mock private EntityClient entityClient;
  @Mock private DataFetchingEnvironment environment;
  @Mock private QueryContext queryContext;
  @Mock private OperationContext operationContext;

  private ListRemoteExecutorPoolsResolver resolver;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    resolver = new ListRemoteExecutorPoolsResolver(entityClient);
    when(environment.getContext()).thenReturn(queryContext);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
  }

  @Test
  public void testListRemoteExecutorPools() throws Exception {
    // Setup input with query
    ListRemoteExecutorPoolsInput input = new ListRemoteExecutorPoolsInput();
    input.setStart(0);
    input.setCount(2);
    input.setQuery("test-query");
    when(environment.getArgument("input")).thenReturn(input);

    // Setup mock URN
    Urn executorUrn = Urn.createFromString("urn:li:remoteExecutorPool:test-pool");

    // Setup search result
    SearchResult searchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(executorUrn);
    searchResult.setEntities(new SearchEntityArray(List.of(searchEntity)));
    searchResult.setNumEntities(1);
    when(entityClient.search(
            eq(operationContext),
            eq(REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            eq("test-query"),
            any(Filter.class),
            any(List.class),
            eq(0),
            eq(2)))
        .thenReturn(searchResult);

    // Execute resolver
    CompletableFuture<ListRemoteExecutorPoolsResult> futureResult = resolver.get(environment);
    ListRemoteExecutorPoolsResult result = futureResult.get();

    // Verify results
    assertNotNull(result);
    assertEquals(0, result.getStart());
    assertEquals(1, result.getCount());
    assertEquals(1, result.getTotal());

    List<RemoteExecutorPool> executors = result.getRemoteExecutorPools();
    assertEquals(1, executors.size());

    RemoteExecutorPool executor = executors.get(0);
    assertEquals(executorUrn.toString(), executor.getUrn());
    assertEquals(EntityType.REMOTE_EXECUTOR_POOL, executor.getType());
  }

  @Test
  public void testListRemoteExecutorPoolsWithDefaultQuery() throws Exception {
    // Setup input without query (should default to "*")
    ListRemoteExecutorPoolsInput input = new ListRemoteExecutorPoolsInput();
    input.setStart(0);
    input.setCount(2);
    when(environment.getArgument("input")).thenReturn(input);

    // Setup mock URN
    Urn executorUrn = Urn.createFromString("urn:li:remoteExecutorPool:test-pool");

    // Setup search result
    SearchResult searchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(executorUrn);
    searchResult.setEntities(new SearchEntityArray(List.of(searchEntity)));
    searchResult.setNumEntities(1);
    when(entityClient.search(
            eq(operationContext),
            eq(REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            eq("*"),
            any(Filter.class),
            any(List.class),
            eq(0),
            eq(2)))
        .thenReturn(searchResult);

    // Execute resolver
    CompletableFuture<ListRemoteExecutorPoolsResult> futureResult = resolver.get(environment);
    ListRemoteExecutorPoolsResult result = futureResult.get();

    // Verify results
    assertNotNull(result);
    assertEquals(1, result.getCount());
    List<RemoteExecutorPool> executors = result.getRemoteExecutorPools();
    assertEquals(1, executors.size());
  }

  @Test
  public void testListRemoteExecutorPoolsEmpty() throws Exception {
    // Setup input with defaults
    ListRemoteExecutorPoolsInput input = new ListRemoteExecutorPoolsInput();
    when(environment.getArgument("input")).thenReturn(input);

    // Return empty search results
    SearchResult emptySearchResult = new SearchResult();
    emptySearchResult.setEntities(new SearchEntityArray());
    emptySearchResult.setNumEntities(0);
    when(entityClient.search(
            eq(operationContext),
            eq(REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            eq("*"),
            any(Filter.class),
            any(List.class),
            anyInt(),
            anyInt()))
        .thenReturn(emptySearchResult);

    // Execute resolver
    CompletableFuture<ListRemoteExecutorPoolsResult> futureResult = resolver.get(environment);
    ListRemoteExecutorPoolsResult result = futureResult.get();

    // Verify empty results
    assertNotNull(result);
    assertEquals(0, result.getStart());
    assertEquals(0, result.getCount());
    assertTrue(result.getRemoteExecutorPools().isEmpty());
  }

  @Test
  public void testListRemoteExecutorPoolsWithError() throws Exception {
    // Setup input
    ListRemoteExecutorPoolsInput input = new ListRemoteExecutorPoolsInput();
    when(environment.getArgument("input")).thenReturn(input);

    // Simulate error in entityClient
    when(entityClient.search(
            any(OperationContext.class),
            anyString(),
            anyString(),
            any(Filter.class),
            any(List.class),
            anyInt(),
            anyInt()))
        .thenThrow(new RemoteInvocationException("Test error"));

    // Execute and verify exception
    CompletableFuture<ListRemoteExecutorPoolsResult> futureResult = resolver.get(environment);

    ExecutionException exception = expectThrows(ExecutionException.class, () -> futureResult.get());
    assertTrue(exception.getCause() instanceof RuntimeException);
    assertEquals("Failed to list remote executor pools", exception.getCause().getMessage());
  }
}
