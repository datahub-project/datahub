package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;
import static org.testng.AssertJUnit.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListRemoteExecutorsResult;
import com.linkedin.datahub.graphql.generated.RemoteExecutor;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
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

public class ListRemoteExecutorsResolverTest {

  @Mock private EntityClient entityClient;
  @Mock private DataFetchingEnvironment environment;
  @Mock private QueryContext queryContext;
  @Mock private OperationContext operationContext;
  @Mock private Entity sourceEntity;

  private ListRemoteExecutorsResolver resolver;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    resolver = new ListRemoteExecutorsResolver(entityClient);
    when(environment.getContext()).thenReturn(queryContext);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
  }

  @Test
  public void testListRemoteExecutors() throws Exception {
    // Setup input parameters
    int start = 0;
    int count = 2;
    when(environment.getArgument("start")).thenReturn(start);
    when(environment.getArgument("count")).thenReturn(count);

    // Setup mock URN
    Urn executorUrn = Urn.createFromString("urn:li:remoteExecutor:test-executor");

    // Setup search result
    SearchResult searchResult = new SearchResult();
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(executorUrn);
    searchResult.setEntities(new SearchEntityArray(List.of(searchEntity)));
    searchResult.setNumEntities(1);

    // Verify search parameters
    List<SortCriterion> expectedSort =
        List.of(new SortCriterion().setField("reportedAt").setOrder(SortOrder.DESCENDING));

    when(entityClient.filter(
            eq(operationContext),
            eq(REMOTE_EXECUTOR_ENTITY_NAME),
            any(Filter.class),
            eq(expectedSort),
            eq(start),
            eq(count)))
        .thenReturn(searchResult);

    // Execute resolver
    CompletableFuture<ListRemoteExecutorsResult> futureResult = resolver.get(environment);
    ListRemoteExecutorsResult result = futureResult.get();

    // Verify results
    assertNotNull(result);
    assertEquals(start, result.getStart());
    assertEquals(1, result.getCount());
    assertEquals(1, result.getTotal());

    List<RemoteExecutor> executors = result.getRemoteExecutors();
    assertEquals(1, executors.size());

    RemoteExecutor executor = executors.get(0);
    assertEquals(executorUrn.toString(), executor.getUrn());
    assertEquals(EntityType.REMOTE_EXECUTOR, executor.getType());
  }

  @Test
  public void testListRemoteExecutorsWithPool() throws Exception {
    // Setup pool source
    String poolUrn = "urn:li:remoteExecutorPool:test-pool";
    when(environment.getSource()).thenReturn(sourceEntity);
    when(sourceEntity.getUrn()).thenReturn(poolUrn);

    // Setup basic parameters
    when(environment.getArgument("start")).thenReturn(0);
    when(environment.getArgument("count")).thenReturn(10);

    // Setup empty search results
    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(new SearchEntityArray());
    searchResult.setNumEntities(0);

    when(entityClient.filter(any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    // Execute resolver
    CompletableFuture<ListRemoteExecutorsResult> futureResult = resolver.get(environment);
    ListRemoteExecutorsResult result = futureResult.get();

    // Verify empty results
    assertNotNull(result);
    assertEquals(0, result.getStart());
    assertEquals(0, result.getCount());
    assertEquals(0, result.getTotal());
    assertTrue(result.getRemoteExecutors().isEmpty());
  }

  @Test
  public void testListRemoteExecutorsWithError() throws Exception {
    // Setup basic parameters
    when(environment.getArgument("start")).thenReturn(0);
    when(environment.getArgument("count")).thenReturn(10);

    // Simulate error in entityClient
    when(entityClient.filter(any(), any(), any(), any(), anyInt(), anyInt()))
        .thenThrow(new RemoteInvocationException("Test error"));

    // Execute and verify exception
    CompletableFuture<ListRemoteExecutorsResult> futureResult = resolver.get(environment);

    ExecutionException exception = expectThrows(ExecutionException.class, () -> futureResult.get());
    assertTrue(exception.getCause() instanceof RuntimeException);
    assertEquals("Failed to list remote executors", exception.getCause().getMessage());
  }
}
