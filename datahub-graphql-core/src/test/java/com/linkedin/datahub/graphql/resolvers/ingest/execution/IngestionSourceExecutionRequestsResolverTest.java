package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.IngestionSourceExecutionRequests;
import com.linkedin.datahub.graphql.resolvers.load.LatestIngestionSourceExecutionsBatchLoader;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IngestionSourceExecutionRequestsResolverTest {

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = mock(EntityClient.class);

    // Mock filter response
    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.any(Filter.class),
                Mockito.any(List.class),
                Mockito.eq(0),
                Mockito.eq(10)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(10)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableList.of(
                            new SearchEntity().setEntity(TEST_EXECUTION_REQUEST_URN)))));

    IngestionSourceExecutionRequestsResolver resolver =
        new IngestionSourceExecutionRequestsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(0);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(10);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    IngestionSource parentSource = new IngestionSource();
    parentSource.setUrn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentSource);

    // Data Assertions
    IngestionSourceExecutionRequests executionRequests = resolver.get(mockEnv).get();
    assertEquals((int) executionRequests.getStart(), 0);
    assertEquals((int) executionRequests.getCount(), 10);
    assertEquals((int) executionRequests.getTotal(), 1);

    assertEquals(
        executionRequests.getExecutionRequests().get(0).getUrn(),
        TEST_EXECUTION_REQUEST_URN.toString());
    assertEquals(
        executionRequests.getExecutionRequests().get(0).getType(), EntityType.EXECUTION_REQUEST);
  }

  @Test
  public void testHotPathUsesDataLoaderWhenBatchEnabled() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    IngestionSourceExecutionRequestsResolver resolver =
        new IngestionSourceExecutionRequestsResolver(mockClient, true);

    IngestionSourceExecutionRequests batched = new IngestionSourceExecutionRequests();
    batched.setStart(0);
    batched.setCount(1);
    batched.setTotal(4);
    batched.setExecutionRequests(List.of());

    @SuppressWarnings("unchecked")
    DataLoader<String, IngestionSourceExecutionRequests> loader = mock(DataLoader.class);
    Mockito.when(loader.load(TEST_INGESTION_SOURCE_URN.toString()))
        .thenReturn(CompletableFuture.completedFuture(batched));

    DataLoaderRegistry registry = mock(DataLoaderRegistry.class);
    Mockito.doReturn(loader)
        .when(registry)
        .getDataLoader(LatestIngestionSourceExecutionsBatchLoader.LOADER_NAME);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(0);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(1);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getDataLoaderRegistry()).thenReturn(registry);
    IngestionSource parentSource = new IngestionSource();
    parentSource.setUrn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentSource);

    IngestionSourceExecutionRequests result = resolver.get(mockEnv).get();
    assertEquals((int) result.getTotal(), 4);
    Mockito.verify(loader, Mockito.times(1)).load(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.verify(mockClient, Mockito.never())
        .filter(any(), any(), any(), any(), Mockito.anyInt(), any());
  }

  @Test
  public void testFlagOffKeepsFilterEvenForStartZeroCountOne() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
                Mockito.any(Filter.class),
                Mockito.any(List.class),
                Mockito.eq(0),
                Mockito.eq(1)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableList.of(
                            new SearchEntity().setEntity(TEST_EXECUTION_REQUEST_URN)))));

    // Single-arg ctor keeps batchLoadEnabled=false
    IngestionSourceExecutionRequestsResolver resolver =
        new IngestionSourceExecutionRequestsResolver(mockClient);

    @SuppressWarnings("unchecked")
    DataLoader<String, IngestionSourceExecutionRequests> loader = mock(DataLoader.class);
    DataLoaderRegistry registry = mock(DataLoaderRegistry.class);
    Mockito.doReturn(loader)
        .when(registry)
        .getDataLoader(LatestIngestionSourceExecutionsBatchLoader.LOADER_NAME);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(0);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(1);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getDataLoaderRegistry()).thenReturn(registry);
    IngestionSource parentSource = new IngestionSource();
    parentSource.setUrn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentSource);

    IngestionSourceExecutionRequests result = resolver.get(mockEnv).get();
    assertEquals((int) result.getTotal(), 1);
    Mockito.verify(loader, Mockito.never()).load(any());
    Mockito.verify(mockClient, Mockito.times(1))
        .filter(
            any(),
            Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            Mockito.any(Filter.class),
            Mockito.any(List.class),
            Mockito.eq(0),
            Mockito.eq(1));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = mock(EntityClient.class);
    IngestionSourceExecutionRequestsResolver resolver =
        new IngestionSourceExecutionRequestsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(0);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(10);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    IngestionSource parentSource = new IngestionSource();
    parentSource.setUrn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentSource);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    Mockito.verify(mockClient, Mockito.times(0))
        .list(Mockito.any(), Mockito.any(), Mockito.anyMap(), Mockito.anyInt(), Mockito.anyInt());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    IngestionSourceExecutionRequestsResolver resolver =
        new IngestionSourceExecutionRequestsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(0);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(10);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    IngestionSource parentSource = new IngestionSource();
    parentSource.setUrn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentSource);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}
