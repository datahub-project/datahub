package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.TimeSeriesAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link TimeSeriesAspectResolver#get(DataFetchingEnvironment)}.
 *
 * <p>Covers: DataLoader path, fallback-to-client (loader absent), fallback-to-client (batch
 * disabled), and authorization denial.
 */
public class TimeSeriesAspectResolverTest {

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)";
  private static final String CHART_URN = "urn:li:chart:(looker,chart1)";

  // ---------- helpers ----------

  /** Builds a minimal mock DataFetchingEnvironment for a given entity urn and context. */
  private static DataFetchingEnvironment mockEnv(
      final String urn,
      final EntityType entityType,
      final QueryContext queryContext,
      final DataLoaderRegistry registry) {

    final Entity source = Mockito.mock(Entity.class);
    Mockito.when(source.getUrn()).thenReturn(urn);
    Mockito.when(source.getType()).thenReturn(entityType);

    final DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getSource()).thenReturn(source);
    Mockito.when(env.getContext()).thenReturn(queryContext);
    Mockito.when(env.getDataLoaderRegistry()).thenReturn(registry);

    // All time/limit arguments default to null
    Mockito.when(env.getArgumentOrDefault(eq("startTimeMillis"), nullable(Long.class)))
        .thenReturn(null);
    Mockito.when(env.getArgumentOrDefault(eq("endTimeMillis"), nullable(Long.class)))
        .thenReturn(null);
    Mockito.when(env.getArgumentOrDefault(eq("limit"), nullable(Integer.class))).thenReturn(null);
    Mockito.when(env.getArgument("filter")).thenReturn(null);

    return env;
  }

  // ---------- tests ----------

  /**
   * When the DataLoader is present in the registry and batch loading is enabled, the resolver must
   * delegate to {@code loader.load(key)} and NOT invoke {@code
   * EntityClient.getTimeseriesAspectValues}.
   */
  @Test
  public void testGetUsesDataLoaderWhenAvailable() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final QueryContext allowCtx = TestUtils.getMockAllowContext();

    // Set up a mock DataLoader that returns one aspect
    @SuppressWarnings("unchecked")
    final DataLoader<TimeseriesAspectBatchLoader.Key, List<EnvelopedAspect>> loader =
        Mockito.mock(DataLoader.class);
    final EnvelopedAspect envelopedAspect = Mockito.mock(EnvelopedAspect.class);
    Mockito.when(loader.load(any()))
        .thenReturn(CompletableFuture.completedFuture(List.of(envelopedAspect)));

    final DataLoaderRegistry registry = Mockito.mock(DataLoaderRegistry.class);
    Mockito.doReturn(loader).when(registry).getDataLoader(TimeseriesAspectBatchLoader.LOADER_NAME);

    // The mapper converts the EnvelopedAspect to a concrete TimeSeriesAspect subtype
    final TimeSeriesAspect mappedAspect = Mockito.mock(TimeSeriesAspect.class);

    final TimeSeriesAspectResolver resolver =
        new TimeSeriesAspectResolver(
            mockClient,
            Constants.CHART_ENTITY_NAME,
            "chartStatistics",
            (ctx, aspect) -> mappedAspect,
            /* sort= */ null,
            /* batchLoadEnabled= */ true);

    final DataFetchingEnvironment env = mockEnv(CHART_URN, EntityType.CHART, allowCtx, registry);

    final List<TimeSeriesAspect> result = resolver.get(env).get();

    // DataLoader must have been called once with a key for this urn
    Mockito.verify(loader, Mockito.times(1)).load(any(TimeseriesAspectBatchLoader.Key.class));
    // The fallback client path must NOT have been invoked
    Mockito.verify(mockClient, Mockito.never())
        .getTimeseriesAspectValues(any(), any(), any(), any(), any(), any(), any(), any(), any());

    assertEquals(result.size(), 1);
    assertEquals(result.get(0), mappedAspect);
  }

  /**
   * When the DataLoaderRegistry returns {@code null} for the loader name, the resolver must fall
   * back to a direct {@code EntityClient.getTimeseriesAspectValues} call.
   */
  @Test
  public void testGetFallsBackToClientWhenLoaderAbsent() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final QueryContext allowCtx = TestUtils.getMockAllowContext();

    // Registry does not have a loader registered
    final DataLoaderRegistry registry = Mockito.mock(DataLoaderRegistry.class);
    Mockito.when(registry.getDataLoader(TimeseriesAspectBatchLoader.LOADER_NAME)).thenReturn(null);

    final EnvelopedAspect envelopedAspect = Mockito.mock(EnvelopedAspect.class);
    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(List.of(envelopedAspect));

    final TimeSeriesAspect mappedAspect = Mockito.mock(TimeSeriesAspect.class);

    final TimeSeriesAspectResolver resolver =
        new TimeSeriesAspectResolver(
            mockClient,
            Constants.CHART_ENTITY_NAME,
            "chartStatistics",
            (ctx, aspect) -> mappedAspect,
            /* sort= */ null,
            /* batchLoadEnabled= */ true);

    final DataFetchingEnvironment env = mockEnv(CHART_URN, EntityType.CHART, allowCtx, registry);

    final List<TimeSeriesAspect> result = resolver.get(env).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .getTimeseriesAspectValues(any(), any(), any(), any(), any(), any(), any(), any(), any());
    assertEquals(result.size(), 1);
    assertEquals(result.get(0), mappedAspect);
  }

  /**
   * When batch loading is explicitly disabled at construction time, the resolver must use the
   * client fallback even when a DataLoader is present in the registry.
   */
  @Test
  public void testGetFallsBackToClientWhenBatchLoadDisabled() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final QueryContext allowCtx = TestUtils.getMockAllowContext();

    // A loader IS registered, but batchLoadEnabled=false — it must be ignored.
    @SuppressWarnings("unchecked")
    final DataLoader<TimeseriesAspectBatchLoader.Key, List<EnvelopedAspect>> loader =
        Mockito.mock(DataLoader.class);
    final DataLoaderRegistry registry = Mockito.mock(DataLoaderRegistry.class);
    Mockito.doReturn(loader).when(registry).getDataLoader(TimeseriesAspectBatchLoader.LOADER_NAME);

    final EnvelopedAspect envelopedAspect = Mockito.mock(EnvelopedAspect.class);
    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(List.of(envelopedAspect));

    final TimeSeriesAspect mappedAspect = Mockito.mock(TimeSeriesAspect.class);

    // batchLoadEnabled = false
    final TimeSeriesAspectResolver resolver =
        new TimeSeriesAspectResolver(
            mockClient,
            Constants.CHART_ENTITY_NAME,
            "chartStatistics",
            (ctx, aspect) -> mappedAspect,
            /* sort= */ null,
            /* batchLoadEnabled= */ false);

    final DataFetchingEnvironment env = mockEnv(CHART_URN, EntityType.CHART, allowCtx, registry);

    final List<TimeSeriesAspect> result = resolver.get(env).get();

    // The DataLoader must never be consulted
    Mockito.verify(loader, Mockito.never()).load(any());
    // The client fallback must have fired
    Mockito.verify(mockClient, Mockito.times(1))
        .getTimeseriesAspectValues(any(), any(), any(), any(), any(), any(), any(), any(), any());
    assertEquals(result.size(), 1);
    assertEquals(result.get(0), mappedAspect);
  }

  /**
   * When the DataLoader delivers an empty list for a key — the value {@code batchLoad} emits for
   * URNs with no matching timeseries documents — the resolver must return an empty list rather than
   * crashing. This is the standard DataLoader positional contract: every slot is filled, missing
   * data gets {@code emptyList()}.
   */
  @Test
  public void testGetDataLoaderEmptyResultReturnsEmptyList() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final QueryContext allowCtx = TestUtils.getMockAllowContext();

    @SuppressWarnings("unchecked")
    final DataLoader<TimeseriesAspectBatchLoader.Key, List<EnvelopedAspect>> loader =
        Mockito.mock(DataLoader.class);
    Mockito.when(loader.load(any()))
        .thenReturn(CompletableFuture.completedFuture(java.util.Collections.emptyList()));

    final DataLoaderRegistry registry = Mockito.mock(DataLoaderRegistry.class);
    Mockito.doReturn(loader).when(registry).getDataLoader(TimeseriesAspectBatchLoader.LOADER_NAME);

    final TimeSeriesAspectResolver resolver =
        new TimeSeriesAspectResolver(
            mockClient,
            Constants.CHART_ENTITY_NAME,
            "chartStatistics",
            (ctx, aspect) -> Mockito.mock(TimeSeriesAspect.class),
            /* sort= */ null,
            /* batchLoadEnabled= */ true);

    final DataFetchingEnvironment env = mockEnv(CHART_URN, EntityType.CHART, allowCtx, registry);

    final List<TimeSeriesAspect> result = resolver.get(env).get();

    assertTrue(
        result.isEmpty(), "Expected empty list when DataLoader returns no documents for URN");
    Mockito.verify(mockClient, Mockito.never())
        .getTimeseriesAspectValues(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  /**
   * Simulates a search-results page where one dataset's profile is accessible and another's is not.
   * The resolver is called once per entity; each call performs its own auth check before
   * dispatching to the DataLoader. The unauthorized URN must short-circuit to emptyList without
   * submitting a key, so it cannot receive or contaminate data for the authorized URN that is
   * batched in the same request.
   */
  @Test
  public void testGetMixedAuthorizationDoesNotCrossContaminate() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    @SuppressWarnings("unchecked")
    final DataLoader<TimeseriesAspectBatchLoader.Key, List<EnvelopedAspect>> loader =
        Mockito.mock(DataLoader.class);
    final EnvelopedAspect envelopedAspect = Mockito.mock(EnvelopedAspect.class);
    Mockito.when(loader.load(any()))
        .thenReturn(CompletableFuture.completedFuture(List.of(envelopedAspect)));

    final DataLoaderRegistry registry = Mockito.mock(DataLoaderRegistry.class);
    Mockito.doReturn(loader).when(registry).getDataLoader(TimeseriesAspectBatchLoader.LOADER_NAME);

    final TimeSeriesAspect mappedAspect = Mockito.mock(TimeSeriesAspect.class);

    // Both calls use the same resolver (dataset + datasetProfile triggers the privilege check).
    final TimeSeriesAspectResolver resolver =
        new TimeSeriesAspectResolver(
            mockClient,
            Constants.DATASET_ENTITY_NAME,
            Constants.DATASET_PROFILE_ASPECT_NAME,
            (ctx, aspect) -> mappedAspect,
            /* sort= */ null,
            /* batchLoadEnabled= */ true);

    // URN_1 is accessible; URN_2 is not — models a page where the viewer has profile-view
    // permission on some datasets but not others.
    final String secondDatasetUrn = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.restricted,PROD)";
    final DataFetchingEnvironment authorizedEnv =
        mockEnv(DATASET_URN, EntityType.DATASET, TestUtils.getMockAllowContext(), registry);
    final DataFetchingEnvironment unauthorizedEnv =
        mockEnv(
            secondDatasetUrn,
            EntityType.DATASET,
            TestUtils.getMockDenyContextWithOperationContext(),
            registry);

    final List<TimeSeriesAspect> authorizedResult = resolver.get(authorizedEnv).get();
    final List<TimeSeriesAspect> unauthorizedResult = resolver.get(unauthorizedEnv).get();

    // Authorized URN got its data via the DataLoader.
    assertEquals(authorizedResult.size(), 1);
    assertEquals(authorizedResult.get(0), mappedAspect);

    // Unauthorized URN returned empty — no data, no leakage.
    assertTrue(unauthorizedResult.isEmpty());

    // DataLoader.load() was called exactly once: only the authorized URN submitted a key.
    Mockito.verify(loader, Mockito.times(1)).load(any(TimeseriesAspectBatchLoader.Key.class));
    Mockito.verify(mockClient, Mockito.never())
        .getTimeseriesAspectValues(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  /**
   * When the actor is not authorized to view the aspect (dataset + datasetProfile combination
   * triggers the VIEW_DATASET_PROFILE_PRIVILEGE check), the resolver must return an empty list
   * without touching the DataLoader or the client.
   */
  @Test
  public void testGetReturnsEmptyListWhenNotAuthorized() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    // A deny context whose OperationContext denies all authorizations
    final QueryContext denyCtx = TestUtils.getMockDenyContextWithOperationContext();

    @SuppressWarnings("unchecked")
    final DataLoader<TimeseriesAspectBatchLoader.Key, List<EnvelopedAspect>> loader =
        Mockito.mock(DataLoader.class);
    final DataLoaderRegistry registry = Mockito.mock(DataLoaderRegistry.class);
    Mockito.doReturn(loader).when(registry).getDataLoader(TimeseriesAspectBatchLoader.LOADER_NAME);

    // dataset + datasetProfile triggers the privilege check
    final TimeSeriesAspectResolver resolver =
        new TimeSeriesAspectResolver(
            mockClient,
            Constants.DATASET_ENTITY_NAME,
            Constants.DATASET_PROFILE_ASPECT_NAME,
            (ctx, aspect) -> Mockito.mock(TimeSeriesAspect.class));

    final DataFetchingEnvironment env = mockEnv(DATASET_URN, EntityType.DATASET, denyCtx, registry);

    final List<TimeSeriesAspect> result = resolver.get(env).get();

    assertTrue(result.isEmpty(), "Expected empty list for unauthorized actor");
    Mockito.verify(loader, Mockito.never()).load(any());
    Mockito.verify(mockClient, Mockito.never())
        .getTimeseriesAspectValues(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }
}
