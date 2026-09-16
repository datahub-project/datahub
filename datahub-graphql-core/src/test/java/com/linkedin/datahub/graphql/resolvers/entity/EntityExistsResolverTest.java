package com.linkedin.datahub.graphql.resolvers.entity;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.loaders.EntityExistsBatchLoader;
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

  @BeforeMethod
  public void setupTest() {
    _entityService = mock(EntityService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);

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
    when(_entityService.exists(any(OperationContext.class), any(Collection.class)))
        .thenAnswer(args -> args.getArgument(1));

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }

  /** Hydrating an entity should go through the loader. */
  @Test
  public void testHydrationPathUsesBatchLoaderWhenEnabled() throws Exception {
    final FeatureFlags flags = new FeatureFlags();
    flags.setEntityExistsBatchLoadEnabled(true);

    final Entity source = mock(Entity.class);
    when(source.getUrn()).thenReturn(ENTITY_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(null);
    when(_dataFetchingEnvironment.getSource()).thenReturn(source);

    final DataLoader<Urn, Boolean> loader = mock(DataLoader.class);
    when(loader.load(any(Urn.class))).thenReturn(CompletableFuture.completedFuture(true));
    final DataLoaderRegistry registry = mock(DataLoaderRegistry.class);
    // doReturn: getDataLoader's return type is inferred, so thenReturn cannot bind it.
    doReturn(loader).when(registry).getDataLoader(EntityExistsBatchLoader.LOADER_NAME);
    when(_dataFetchingEnvironment.getDataLoaderRegistry()).thenReturn(registry);

    assertTrue(
        new EntityExistsResolver(_entityService, flags).get(_dataFetchingEnvironment).join());

    verify(loader, times(1)).load(UrnUtils.getUrn(ENTITY_URN_STRING));
    verify(_entityService, never()).exists(any(OperationContext.class), any(Collection.class));
  }

  /** A caller-supplied urn stays on its own call, so a bad urn cannot fail a whole batch. */
  @Test
  public void testExplicitUrnArgumentStaysUnbatched() throws Exception {
    final FeatureFlags flags = new FeatureFlags();
    flags.setEntityExistsBatchLoadEnabled(true);

    when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(ENTITY_URN_STRING);
    when(_entityService.exists(any(OperationContext.class), any(Collection.class)))
        .thenAnswer(args -> args.getArgument(1));

    assertTrue(
        new EntityExistsResolver(_entityService, flags).get(_dataFetchingEnvironment).join());

    verify(_entityService, times(1)).exists(any(OperationContext.class), any(Collection.class));
    verify(_dataFetchingEnvironment, never()).getDataLoaderRegistry();
  }

  /** The legacy single-arg constructor has no flags, so it must keep the per-entity path. */
  @Test
  public void testHydrationPathUnbatchedWhenConstructedWithoutFlags() throws Exception {
    final Entity source = mock(Entity.class);
    when(source.getUrn()).thenReturn(ENTITY_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(null);
    when(_dataFetchingEnvironment.getSource()).thenReturn(source);
    when(_entityService.exists(any(OperationContext.class), any(Collection.class)))
        .thenAnswer(args -> args.getArgument(1));

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    verify(_entityService, times(1)).exists(any(OperationContext.class), any(Collection.class));
    verify(_dataFetchingEnvironment, never()).getDataLoaderRegistry();
  }

  @Test
  public void testHydrationPathUnbatchedWhenFlagDisabled() throws Exception {
    final FeatureFlags flags = new FeatureFlags();
    flags.setEntityExistsBatchLoadEnabled(false);

    final Entity source = mock(Entity.class);
    when(source.getUrn()).thenReturn(ENTITY_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("urn"))).thenReturn(null);
    when(_dataFetchingEnvironment.getSource()).thenReturn(source);
    when(_entityService.exists(any(OperationContext.class), any(Collection.class)))
        .thenAnswer(args -> args.getArgument(1));

    assertTrue(
        new EntityExistsResolver(_entityService, flags).get(_dataFetchingEnvironment).join());

    verify(_entityService, times(1)).exists(any(OperationContext.class), any(Collection.class));
  }
}
