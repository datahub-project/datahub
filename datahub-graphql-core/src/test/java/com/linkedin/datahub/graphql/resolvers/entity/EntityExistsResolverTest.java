package com.linkedin.datahub.graphql.resolvers.entity;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityExistsResolverTest {
  private static final String ENTITY_URN_STRING = "urn:li:corpuser:test";

  private EntityService _entityService;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private EntityExistsResolver _resolver;
  private DataLoaderRegistry _dataRegistry;

  @BeforeMethod
  public void setupTest() {
    _entityService = mock(EntityService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _dataRegistry = mock(DataLoaderRegistry.class);
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    when(_dataFetchingEnvironment.getContext()).thenReturn(queryContext);

    _resolver = new EntityExistsResolver(_entityService);
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
    when(_dataRegistry.getDataLoader(eq("entityExists"))).thenReturn(exitsLoader);
    when(_entityService.exists(any(OperationContext.class), any(Collection.class)))
        .thenAnswer(args -> args.getArgument(1));
    when(exitsLoader.load(eq(ENTITY_URN_STRING)))
        .thenReturn(CompletableFuture.completedFuture(true));
    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }
}
