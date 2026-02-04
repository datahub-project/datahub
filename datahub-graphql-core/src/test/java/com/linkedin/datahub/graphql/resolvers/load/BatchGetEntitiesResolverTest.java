package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.Arrays;
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
  /** Tests that results are returned in the same order as input entities */
  public void testOrdering() throws Exception {
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

    // DataLoader returns results in same order as input keys
    CompletableFuture mockFuture =
        CompletableFuture.completedFuture(
            ImmutableList.of(mockResponseEntity1, mockResponseEntity2));
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

  @Test
  /**
   * Tests that Restricted entities returned from entity types (for users who can't view) are passed
   * through correctly
   */
  public void testRestrictedEntityPassThrough() throws Exception {
    Function entityProvider = mock(Function.class);
    List<Entity> inputEntities =
        getRequestEntities(ImmutableList.of("urn:li:dataset:visible", "urn:li:dataset:restricted"));
    when(entityProvider.apply(any())).thenReturn(inputEntities);

    BatchGetEntitiesResolver resolver =
        new BatchGetEntitiesResolver(
            ImmutableList.of(new DatasetType(_entityClient)), entityProvider);

    DataLoaderRegistry mockDataLoaderRegistry = mock(DataLoaderRegistry.class);
    when(_dataFetchingEnvironment.getDataLoaderRegistry()).thenReturn(mockDataLoaderRegistry);
    DataLoader mockDataLoader = mock(DataLoader.class);
    when(mockDataLoaderRegistry.getDataLoader(any())).thenReturn(mockDataLoader);

    Dataset mockVisibleEntity = new Dataset();
    mockVisibleEntity.setUrn("urn:li:dataset:visible");

    // Second entity is a Restricted entity (returned by entity type's batchLoad)
    Restricted mockRestrictedEntity = new Restricted();
    mockRestrictedEntity.setUrn("urn:li:restricted:encrypted");
    mockRestrictedEntity.setType(EntityType.RESTRICTED);

    // Wrap in DataFetcherResult as entity types do
    DataFetcherResult<Entity> visibleResult =
        DataFetcherResult.<Entity>newResult().data(mockVisibleEntity).build();
    DataFetcherResult<Entity> restrictedResult =
        DataFetcherResult.<Entity>newResult().data(mockRestrictedEntity).build();

    CompletableFuture mockFuture =
        CompletableFuture.completedFuture(Arrays.asList(visibleResult, restrictedResult));
    when(mockDataLoader.loadMany(any())).thenReturn(mockFuture);

    List<Entity> batchGetResponse = resolver.get(_dataFetchingEnvironment).join();

    assertEquals(batchGetResponse.size(), 2);
    // First entity should be the visible dataset
    assertEquals(batchGetResponse.get(0), mockVisibleEntity);

    // Second entity should be the Restricted entity passed through
    assertTrue(batchGetResponse.get(1) instanceof Restricted);
    Restricted restrictedEntity = (Restricted) batchGetResponse.get(1);
    assertEquals(restrictedEntity.getType(), EntityType.RESTRICTED);
  }

  @Test
  /** Tests that DataFetcherResult with null data (entity doesn't exist) returns null */
  public void testDataFetcherResultWithNullDataReturnsNull() throws Exception {
    Function entityProvider = mock(Function.class);
    List<Entity> inputEntities =
        getRequestEntities(ImmutableList.of("urn:li:dataset:exists", "urn:li:dataset:notfound"));
    when(entityProvider.apply(any())).thenReturn(inputEntities);

    BatchGetEntitiesResolver resolver =
        new BatchGetEntitiesResolver(
            ImmutableList.of(new DatasetType(_entityClient)), entityProvider);

    DataLoaderRegistry mockDataLoaderRegistry = mock(DataLoaderRegistry.class);
    when(_dataFetchingEnvironment.getDataLoaderRegistry()).thenReturn(mockDataLoaderRegistry);
    DataLoader mockDataLoader = mock(DataLoader.class);
    when(mockDataLoaderRegistry.getDataLoader(any())).thenReturn(mockDataLoader);

    Dataset mockExistingEntity = new Dataset();
    mockExistingEntity.setUrn("urn:li:dataset:exists");

    // First entity wrapped in DataFetcherResult with data
    DataFetcherResult<Entity> resultWithData =
        DataFetcherResult.<Entity>newResult().data(mockExistingEntity).build();

    // Second entity: DataFetcherResult with null data (entity doesn't exist in DB)
    DataFetcherResult<Entity> resultWithNullData =
        DataFetcherResult.<Entity>newResult().data(null).build();

    CompletableFuture mockFuture =
        CompletableFuture.completedFuture(Arrays.asList(resultWithData, resultWithNullData));
    when(mockDataLoader.loadMany(any())).thenReturn(mockFuture);

    List<Entity> batchGetResponse = resolver.get(_dataFetchingEnvironment).join();

    assertEquals(batchGetResponse.size(), 2);
    // First entity should be the existing dataset
    assertEquals(batchGetResponse.get(0), mockExistingEntity);

    // Second entity should be null (entity doesn't exist)
    assertNull(batchGetResponse.get(1));
  }

  @Test
  /** Tests that null results from the loader are passed through as null */
  public void testNullResultsPassedThrough() throws Exception {
    Function entityProvider = mock(Function.class);
    List<Entity> inputEntities =
        getRequestEntities(ImmutableList.of("urn:li:dataset:exists", "urn:li:dataset:null"));
    when(entityProvider.apply(any())).thenReturn(inputEntities);

    BatchGetEntitiesResolver resolver =
        new BatchGetEntitiesResolver(
            ImmutableList.of(new DatasetType(_entityClient)), entityProvider);

    DataLoaderRegistry mockDataLoaderRegistry = mock(DataLoaderRegistry.class);
    when(_dataFetchingEnvironment.getDataLoaderRegistry()).thenReturn(mockDataLoaderRegistry);
    DataLoader mockDataLoader = mock(DataLoader.class);
    when(mockDataLoaderRegistry.getDataLoader(any())).thenReturn(mockDataLoader);

    Dataset mockExistingEntity = new Dataset();
    mockExistingEntity.setUrn("urn:li:dataset:exists");

    // Second entity is null from the loader
    CompletableFuture mockFuture =
        CompletableFuture.completedFuture(Arrays.asList(mockExistingEntity, null));
    when(mockDataLoader.loadMany(any())).thenReturn(mockFuture);

    List<Entity> batchGetResponse = resolver.get(_dataFetchingEnvironment).join();

    assertEquals(batchGetResponse.size(), 2);
    // First entity should be the existing dataset
    assertEquals(batchGetResponse.get(0), mockExistingEntity);

    // Second entity should be null (passed through from loader)
    assertNull(batchGetResponse.get(1));
  }
}
