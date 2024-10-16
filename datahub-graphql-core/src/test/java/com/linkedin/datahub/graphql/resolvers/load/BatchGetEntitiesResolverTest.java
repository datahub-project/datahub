package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchGetEntitiesResolverTest {
  private EntityClient _entityClient;
  private EntityService _entityService;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityService = mock(EntityService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _entityClient = mock(EntityClient.class);
  }

  List<Entity> getRequestEntities(List<String> urnList) {

    return urnList.stream()
        .map(
            urn -> {
              if (urn.startsWith("urn:li:dataset")) {
                Dataset entity = new Dataset();
                entity.setUrn(urn);
                return entity;
              } else if (urn.startsWith("urn:li:dashboard")) {
                Dashboard entity = new Dashboard();
                entity.setUrn(urn);
                return entity;
              } else {
                throw new RuntimeException("Can't handle urn " + urn);
              }
            })
        .collect(Collectors.toList());
  }

  @Test
  /** Tests that if responses come back out of order, we stitch them back correctly */
  public void testReordering() throws Exception {
    Function entityProvider = mock(Function.class);
    List<Entity> inputEntities =
        getRequestEntities(ImmutableList.of("urn:li:dataset:1", "urn:li:dataset:2"));
    when(entityProvider.apply(any())).thenReturn(inputEntities);
    BatchGetEntitiesResolver resolver =
        new BatchGetEntitiesResolver(
            ImmutableList.of(new DatasetType(_entityClient)), entityProvider);

    DataLoaderRegistry mockDataLoaderRegistry = mock(DataLoaderRegistry.class);
    when(_dataFetchingEnvironment.getDataLoaderRegistry()).thenReturn(mockDataLoaderRegistry);
    DataLoader mockDataLoader = mock(DataLoader.class);
    when(mockDataLoaderRegistry.getDataLoader(any())).thenReturn(mockDataLoader);

    Dataset mockResponseEntity1 = new Dataset();
    mockResponseEntity1.setUrn("urn:li:dataset:1");

    Dataset mockResponseEntity2 = new Dataset();
    mockResponseEntity2.setUrn("urn:li:dataset:2");

    CompletableFuture mockFuture =
        CompletableFuture.completedFuture(
            ImmutableList.of(mockResponseEntity2, mockResponseEntity1));
    when(mockDataLoader.loadMany(any())).thenReturn(mockFuture);
    when(_entityService.exists(any(), any(List.class), eq(true)))
        .thenAnswer(args -> Set.of(args.getArgument(0)));
    List<Entity> batchGetResponse = resolver.get(_dataFetchingEnvironment).join();
    assertEquals(batchGetResponse.size(), 2);
    assertEquals(batchGetResponse.get(0), mockResponseEntity1);
    assertEquals(batchGetResponse.get(1), mockResponseEntity2);
  }

  @Test
  /** Tests that if input list contains duplicates, we stitch them back correctly */
  public void testDuplicateUrns() throws Exception {
    Function entityProvider = mock(Function.class);
    List<Entity> inputEntities =
        getRequestEntities(ImmutableList.of("urn:li:dataset:foo", "urn:li:dataset:foo"));
    when(entityProvider.apply(any())).thenReturn(inputEntities);
    BatchGetEntitiesResolver resolver =
        new BatchGetEntitiesResolver(
            ImmutableList.of(new DatasetType(_entityClient)), entityProvider);

    DataLoaderRegistry mockDataLoaderRegistry = mock(DataLoaderRegistry.class);
    when(_dataFetchingEnvironment.getDataLoaderRegistry()).thenReturn(mockDataLoaderRegistry);
    DataLoader mockDataLoader = mock(DataLoader.class);
    when(mockDataLoaderRegistry.getDataLoader(any())).thenReturn(mockDataLoader);

    Dataset mockResponseEntity = new Dataset();
    mockResponseEntity.setUrn("urn:li:dataset:foo");

    CompletableFuture mockFuture =
        CompletableFuture.completedFuture(ImmutableList.of(mockResponseEntity));
    when(mockDataLoader.loadMany(any())).thenReturn(mockFuture);
    when(_entityService.exists(any(), any(List.class), eq(true)))
        .thenAnswer(args -> Set.of(args.getArgument(0)));
    List<Entity> batchGetResponse = resolver.get(_dataFetchingEnvironment).join();
    assertEquals(batchGetResponse.size(), 2);
    assertEquals(batchGetResponse.get(0), mockResponseEntity);
    assertEquals(batchGetResponse.get(1), mockResponseEntity);
  }
}
