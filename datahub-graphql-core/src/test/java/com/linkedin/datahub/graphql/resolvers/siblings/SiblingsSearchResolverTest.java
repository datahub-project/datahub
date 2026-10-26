package com.linkedin.datahub.graphql.resolvers.siblings;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.loaders.SiblingsSearchBatchLoader;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import graphql.execution.MergedField;
import graphql.language.Field;
import graphql.language.SelectionSet;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.mockito.ArgumentCaptor;
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
  private ScrollResults _loaderResult;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() throws Exception {
    _entityClient = Mockito.mock(EntityClient.class);
    _viewService = Mockito.mock(ViewService.class);
    _loader = Mockito.mock(DataLoader.class);
    _loaderResult = new ScrollResults();
    Mockito.when(_loader.load(any())).thenReturn(CompletableFuture.completedFuture(_loaderResult));

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
    Mockito.when(mockEnv.getMergedField())
        .thenReturn(mergedSiblingsField("total", "count", "searchResults"));
    Mockito.when(mockEnv.getFragmentsByName()).thenReturn(Map.of());
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
    final ScrollResults results =
        new SiblingsSearchResolver(_entityClient, _viewService, flags(true))
            .get(env(input(null)))
            .get();

    // The scope default is asserted on the unbatched path elsewhere; assert it here too, because
    // on this path it reaches the search through the key rather than the client.
    final ArgumentCaptor<SiblingsSearchBatchLoader.Key> key =
        ArgumentCaptor.forClass(SiblingsSearchBatchLoader.Key.class);
    Mockito.verify(_loader, Mockito.times(1)).load(key.capture());
    assertEquals(key.getValue().getUrn(), TEST_URN);
    assertEquals(key.getValue().getEntityNames(), List.of("dataset"));
    // The loader's answer is what the caller gets, not a fresh empty result.
    assertSame(results, _loaderResult);
    Mockito.verifyNoInteractions(_entityClient);
  }

  /** An explicit type has to survive into the key, not be replaced by the default scope. */
  @Test
  public void testBatchedKeyCarriesExplicitTypes() throws Exception {
    final Map<String, Object> in = input(null);
    in.put("types", List.of(EntityType.CHART.name()));

    new SiblingsSearchResolver(_entityClient, _viewService, flags(true)).get(env(in)).get();

    final ArgumentCaptor<SiblingsSearchBatchLoader.Key> key =
        ArgumentCaptor.forClass(SiblingsSearchBatchLoader.Key.class);
    Mockito.verify(_loader).load(key.capture());
    assertEquals(key.getValue().getEntityNames(), List.of("chart"));
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

  /**
   * The siblings aspect exists only on dataset, so an unscoped search would query every default
   * index for a field they cannot carry. No caller passes types, so the resolver supplies the
   * scope.
   */
  @Test
  public void testDefaultsToSiblingCapableEntityTypesWhenTypesOmitted() throws Exception {
    assertEquals(searchedEntityNames(null), List.of("dataset"));
  }

  /** An explicit type wins over the default. Uses CHART so the two cannot coincide. */
  @Test
  public void testExplicitTypesAreRespected() throws Exception {
    assertEquals(searchedEntityNames(List.of(EntityType.CHART)), List.of("chart"));
  }

  /** Runs the unbatched path and returns the entity names handed to the search client. */
  @SuppressWarnings("unchecked")
  private List<String> searchedEntityNames(final List<EntityType> inputTypes) throws Exception {
    final Map<String, Object> in = input(null);
    if (inputTypes != null) {
      in.put(
          "types",
          inputTypes.stream().map(Enum::name).collect(java.util.stream.Collectors.toList()));
    }

    // No feature flags => unbatched path, so the names reach the client directly.
    new SiblingsSearchResolver(_entityClient, _viewService).get(env(in)).get();

    final ArgumentCaptor<List> names = ArgumentCaptor.forClass(List.class);
    Mockito.verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            names.capture(),
            any(),
            nullable(Filter.class),
            nullable(String.class),
            nullable(String.class),
            any(),
            nullable(Integer.class),
            any());
    return names.getValue();
  }

  /** Builds the merged field for `siblingsSearch` with the given sub-selections. */
  private static MergedField mergedSiblingsField(final String... selectedFields) {
    final SelectionSet.Builder selectionSet = SelectionSet.newSelectionSet();
    for (String name : selectedFields) {
      selectionSet.selection(Field.newField(name).build());
    }
    return MergedField.newMergedField(
            Field.newField("siblingsSearch").selectionSet(selectionSet.build()).build())
        .build();
  }

  /**
   * A chunk's aggregations describe every urn in the chunk, so they cannot be attributed to one
   * dataset. Selecting facets must therefore fall back to the unbatched path, which aggregates over
   * this dataset's siblings alone.
   */
  @Test
  public void testSelectingFacetsFallsBackToDirectSearch() throws Exception {
    final DataFetchingEnvironment env = env(input(null));
    Mockito.when(env.getMergedField()).thenReturn(mergedSiblingsField("total", "facets"));

    new SiblingsSearchResolver(_entityClient, _viewService, flags(true)).get(env).get();

    verifyTookDirectPath();
  }
}
