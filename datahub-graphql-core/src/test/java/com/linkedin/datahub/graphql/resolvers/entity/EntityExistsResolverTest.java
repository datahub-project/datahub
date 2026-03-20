package com.linkedin.datahub.graphql.resolvers.entity;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.entitytype.EntityExistsType;
import com.linkedin.metadata.entity.EntityService;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityExistsResolverTest {
  private static final String ENTITY_URN_STRING = "urn:li:corpuser:test";
  private static final String URN_1 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table1,PROD)";
  private static final String URN_2 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table2,PROD)";
  private static final String URN_3 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table3,PROD)";

  private EntityService _entityService;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private EntityExistsResolver _resolver;
  private DataLoaderRegistry _dataRegistry;
  private EntityExistsType entityExitsType;

  @BeforeMethod
  public void setupTest() {
    _entityService = mock(EntityService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _dataRegistry = mock(DataLoaderRegistry.class);
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    when(_dataFetchingEnvironment.getContext()).thenReturn(queryContext);

    _resolver = new EntityExistsResolver(_entityService);
    entityExitsType = new EntityExistsType(_entityService);
  }

  @Test
  public void testFailsNullEntity() {
    when(_dataFetchingEnvironment.getArgument("urn")).thenReturn(null);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPasses() throws Exception {
    when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(ENTITY_URN_STRING);
    when(_dataFetchingEnvironment.getDataLoaderRegistry()).thenReturn(_dataRegistry);
    DataLoader<Object, Object> exitsLoader = mock(DataLoader.class);
    when(_dataRegistry.getDataLoader(eq("ENTITY_EXISTS"))).thenReturn(exitsLoader);
    when(_entityService.exists(any(OperationContext.class), any(Collection.class)))
        .thenAnswer(args -> args.getArgument(1));
    when(exitsLoader.load(eq(ENTITY_URN_STRING)))
        .thenReturn(CompletableFuture.completedFuture(true));
    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testBatchLoadingWithMixedResults() throws Exception {
    // Test batch loading with mixed existing/non-existing entities
    // Validates: true, false, true pattern to ensure order is correct
    QueryContext queryContext = mock(QueryContext.class);
    OperationContext operationContext = mock(OperationContext.class);
    when(queryContext.getOperationContext()).thenReturn(operationContext);

    // URN_1 and URN_3 exist, URN_2 does not
    Set<Urn> existingUrns = Set.of(Urn.createFromString(URN_1), Urn.createFromString(URN_3));

    when(_entityService.exists(eq(operationContext), any(Set.class))).thenReturn(existingUrns);

    List<DataFetcherResult<Boolean>> results =
        entityExitsType.batchLoad(List.of(URN_1, URN_2, URN_3), queryContext);

    // Verify mixed results (true, false, true)
    assertTrue(results.get(0).getData(), "URN_1 should exist");
    assertFalse(results.get(1).getData(), "URN_2 should not exist");
    assertTrue(results.get(2).getData(), "URN_3 should exist");
  }

  @Test
  public void testOrderPreservationInBatch() throws Exception {
    // Test that results are returned in same order as loaded, not HashSet order
    // Critical fix: validates parsedUrns List maintains order vs urnSet
    QueryContext queryContext = mock(QueryContext.class);
    OperationContext operationContext = mock(OperationContext.class);
    when(queryContext.getOperationContext()).thenReturn(operationContext);

    Set<Urn> allUrns =
        Set.of(
            Urn.createFromString(URN_1), Urn.createFromString(URN_2), Urn.createFromString(URN_3));

    when(_entityService.exists(eq(operationContext), any(Set.class))).thenReturn(allUrns);

    // Load in reverse order (URN_3, URN_1, URN_2)
    // Without order preservation, results would come back in different order
    List<String> loadOrder = Arrays.asList(URN_3, URN_1, URN_2);
    List<DataFetcherResult<Boolean>> results = entityExitsType.batchLoad(loadOrder, queryContext);
    // Results must be in same order as loaded input, not hash order
    assertTrue(results.get(0).getData(), "Index 0 should be URN_3");
    assertTrue(results.get(1).getData(), "Index 1 should be URN_1");
    assertTrue(results.get(2).getData(), "Index 2 should be URN_2");
  }

  @Test
  public void testBatchingReducesQueryCount() throws Exception {
    // Test N+1 optimization: 3 loads = 1 entityService.exists() call, not 3
    QueryContext queryContext = mock(QueryContext.class);
    OperationContext operationContext = mock(OperationContext.class);
    when(queryContext.getOperationContext()).thenReturn(operationContext);

    Set<Urn> allUrns =
        Set.of(
            Urn.createFromString(URN_1), Urn.createFromString(URN_2), Urn.createFromString(URN_3));

    when(_entityService.exists(eq(operationContext), any(Set.class))).thenReturn(allUrns);

    // Load three URNs (normally would be 3 separate queries)
    List<String> loadOrder = Arrays.asList(URN_1, URN_2, URN_3);
    List<DataFetcherResult<Boolean>> results = entityExitsType.batchLoad(loadOrder, queryContext);

    // Verify entityService.exists called ONCE with all 3 URNs (batched)
    verify(_entityService, times(1))
        .exists(eq(operationContext), argThat((Set<Urn> set) -> set.size() == 3));
  }
}
