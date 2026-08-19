package com.linkedin.datahub.graphql.resolvers.siblings;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.loaders.SiblingsSearchBatchLoader;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SiblingsSearchResolverTest {

  private static final String TEST_URN =
      "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders,PROD)";

  private EntityClient _entityClient;
  private ViewService _viewService;
  private DataLoader<SiblingsSearchBatchLoader.Key, ScrollResults> _loader;
  private DataLoaderRegistry _registry;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() throws Exception {
    _entityClient = Mockito.mock(EntityClient.class);
    _viewService = Mockito.mock(ViewService.class);
    _loader = Mockito.mock(DataLoader.class);
    Mockito.when(_loader.load(any()))
        .thenReturn(CompletableFuture.completedFuture(new ScrollResults()));

    _registry = Mockito.mock(DataLoaderRegistry.class);
    // doReturn avoids generic-inference issues on the generic getDataLoader(String) signature.
    Mockito.doReturn(_loader).when(_registry).getDataLoader(SiblingsSearchBatchLoader.LOADER_NAME);

    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(),
                any(),
                any(),
                nullable(Filter.class),
                nullable(String.class),
                nullable(String.class),
                any(),
                nullable(Integer.class),
                any()))
        .thenReturn(
            new ScrollResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setPageSize(0)
                .setMetadata(new SearchResultMetadata()));
  }

  private DataFetchingEnvironment env(final Map<String, Object> input) {
    final Dataset source = new Dataset();
    source.setUrn(TEST_URN);

    final QueryContext context = getMockAllowContext();
    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(source);
    Mockito.when(mockEnv.getContext()).thenReturn(context);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(input);
    Mockito.when(mockEnv.getDataLoaderRegistry()).thenReturn(_registry);
    return mockEnv;
  }

  private static Map<String, Object> input(final Object scrollId) {
    final Map<String, Object> input = new HashMap<>();
    input.put("query", "*");
    input.put("count", 1);
    if (scrollId != null) {
      input.put("scrollId", scrollId);
    }
    return input;
  }

  private static FeatureFlags flags(final boolean batchEnabled) {
    final FeatureFlags flags = new FeatureFlags();
    flags.setSiblingsSearchBatchLoadEnabled(batchEnabled);
    return flags;
  }

  private void verifyTookDirectPath() throws Exception {
    Mockito.verify(_entityClient, Mockito.times(1))
        .scrollAcrossEntities(
            any(),
            any(),
            any(),
            nullable(Filter.class),
            nullable(String.class),
            nullable(String.class),
            any(),
            nullable(Integer.class),
            any());
    Mockito.verify(_loader, Mockito.never()).load(any());
  }

  @Test
  public void testBatchesWhenFlagEnabled() throws Exception {
    new SiblingsSearchResolver(_entityClient, _viewService, flags(true))
        .get(env(input(null)))
        .get();

    Mockito.verify(_loader, Mockito.times(1)).load(any());
    Mockito.verifyNoInteractions(_entityClient);
  }

  @Test
  public void testFallsBackToDirectSearchWhenFlagDisabled() throws Exception {
    new SiblingsSearchResolver(_entityClient, _viewService, flags(false))
        .get(env(input(null)))
        .get();

    verifyTookDirectPath();
  }

  /** A scroll cursor is per-query state a grouped search cannot produce, so it must not batch. */
  @Test
  public void testScrollIdForcesDirectSearchEvenWhenFlagEnabled() throws Exception {
    new SiblingsSearchResolver(_entityClient, _viewService, flags(true))
        .get(env(input("some-scroll-id")))
        .get();

    verifyTookDirectPath();
  }

  @Test
  public void testNoFeatureFlagsKeepsBatchingOff() throws Exception {
    new SiblingsSearchResolver(_entityClient, _viewService).get(env(input(null))).get();

    verifyTookDirectPath();
  }
}
