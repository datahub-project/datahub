package com.linkedin.datahub.graphql.resolvers.container;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.ContainerEntitiesInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.loaders.ContainerEntityCountsBatchLoader;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import graphql.execution.MergedField;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.dataloader.DataLoader;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ContainerEntitiesResolverTest {

  private static final ContainerEntitiesInput TEST_INPUT =
      new ContainerEntitiesInput(null, 0, 20, Collections.emptyList());
  private static final String CONTAINER_URN = "urn:li:container:test-container";

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);

    final String childUrn = "urn:li:dataset:(test,test,test)";
    final String containerUrn = CONTAINER_URN;

    final Criterion filterCriterion =
        buildCriterion("container.keyword", Condition.EQUAL, containerUrn);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                Mockito.eq(ContainerEntitiesResolver.CONTAINABLE_ENTITY_NAMES),
                Mockito.eq("*"),
                Mockito.eq(
                    new Filter()
                        .setOr(
                            new ConjunctiveCriterionArray(
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(ImmutableList.of(filterCriterion)))))),
                Mockito.eq(0),
                Mockito.eq(20),
                Mockito.eq(Collections.emptyList())))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(
                            new SearchEntity().setEntity(Urn.createFromString(childUrn)))))
                .setMetadata(
                    new SearchResultMetadata().setAggregations(new AggregationMetadataArray())));

    ContainerEntitiesResolver resolver = new ContainerEntitiesResolver(mockClient);

    QueryContext mockContext = mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getMergedField())
        .thenReturn(mergedEntitiesField("total", "searchResults"));
    Mockito.when(mockEnv.getFragmentsByName()).thenReturn(Collections.emptyMap());

    Container parentContainer = new Container();
    parentContainer.setUrn(containerUrn);
    Mockito.when(mockEnv.getSource()).thenReturn(parentContainer);

    assertEquals((int) resolver.get(mockEnv).get().getStart(), 0);
    assertEquals((int) resolver.get(mockEnv).get().getCount(), 1);
    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getSearchResults().size(), 1);
    assertEquals(
        resolver.get(mockEnv).get().getSearchResults().get(0).getEntity().getUrn(), childUrn);
  }

  @Test
  public void testFiltersAreAppliedToSearch() throws Exception {
    // Facet filters from the input must be ANDed onto the container criterion rather than dropped.
    EntityClient mockClient = mock(EntityClient.class);
    stubEmptySearch(mockClient);

    final FacetFilterInput typeFilter = new FacetFilterInput();
    typeFilter.setField("_entityType");
    typeFilter.setValues(ImmutableList.of("DATASET"));

    QueryContext mockContext = mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(new ContainerEntitiesInput(null, 0, 20, ImmutableList.of(typeFilter)));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Container parentContainer = new Container();
    parentContainer.setUrn(CONTAINER_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentContainer);

    new ContainerEntitiesResolver(mockClient).get(mockEnv).get();

    final ArgumentCaptor<Filter> captor = ArgumentCaptor.forClass(Filter.class);
    Mockito.verify(mockClient)
        .searchAcrossEntities(
            any(), any(), any(), captor.capture(), anyInt(), nullable(Integer.class), any());

    final CriterionArray criteria = captor.getValue().getOr().get(0).getAnd();
    assertEquals(criteria.size(), 2);
    assertEquals(criteria.get(0).getField(), "container.keyword");
    assertTrue(criteria.get(0).getValues().contains(CONTAINER_URN));
    assertEquals(criteria.get(1).getField(), "_entityType");
    assertTrue(criteria.get(1).getValues().contains("DATASET"));
  }

  @Test
  public void testCountOnlySelectionServedFromBatchedLoader() throws Exception {
    // The search-result fragment shape: `entities(input: {}) { total }`. count falls back to 20,
    // but no hits are selected, so this must not issue a search.
    EntityClient mockClient = mock(EntityClient.class);
    DataFetchingEnvironment mockEnv =
        mockEnv(new ContainerEntitiesInput(null, null, null, null), mockCountLoader(42L), "total");

    ContainerEntitiesResolver resolver = new ContainerEntitiesResolver(mockClient);

    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 42);
    assertTrue(resolver.get(mockEnv).get().getSearchResults().isEmpty());
    verifySearchCount(mockClient, 0);
  }

  @Test
  public void testCountOnlySelectionWithNonZeroCountStillBatched() throws Exception {
    // The container profile shape: `entities(input: { start: 0, count: 1 }) { total }`. `total` is
    // independent of paging, so a non-zero count must not disqualify the fast path.
    EntityClient mockClient = mock(EntityClient.class);
    DataFetchingEnvironment mockEnv =
        mockEnv(new ContainerEntitiesInput(null, 0, 1, null), mockCountLoader(7L), "total");

    ContainerEntitiesResolver resolver = new ContainerEntitiesResolver(mockClient);

    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 7);
    verifySearchCount(mockClient, 0);
  }

  @Test
  public void testFragmentSpreadSelectingOnlyTotalIsBatched() throws Exception {
    // The production query reaches this field through a fragment, so the walk must resolve spreads
    // rather than treat them as an unrecognized selection.
    EntityClient mockClient = mock(EntityClient.class);
    DataFetchingEnvironment mockEnv =
        mockEnvWithSelections(
            mockCountLoader(11L),
            Collections.singletonList(new FragmentSpread("counts")),
            Collections.singletonMap(
                "counts",
                FragmentDefinition.newFragmentDefinition()
                    .name("counts")
                    .selectionSet(
                        SelectionSet.newSelectionSet()
                            .selection(Field.newField("total").build())
                            .build())
                    .build()));

    assertEquals((int) new ContainerEntitiesResolver(mockClient).get(mockEnv).get().getTotal(), 11);
    verifySearchCount(mockClient, 0);
  }

  @Test
  public void testHitSelectionFallsBackToDirectSearch() throws Exception {
    // Selecting searchResults means hits are actually read — the aggregation cannot serve it.
    EntityClient mockClient = mock(EntityClient.class);
    stubEmptySearch(mockClient);
    DataFetchingEnvironment mockEnv =
        mockEnv(TEST_INPUT, mockCountLoader(42L), "total", "searchResults");

    new ContainerEntitiesResolver(mockClient).get(mockEnv).get();

    verifySearchCount(mockClient, 1);
  }

  @Test
  public void testFiltersFallBackToDirectSearch() throws Exception {
    // The loader applies no facet filters, so a filtered request must take the direct path.
    EntityClient mockClient = mock(EntityClient.class);
    stubEmptySearch(mockClient);
    final FacetFilterInput filter = new FacetFilterInput();
    filter.setField("_entityType");
    filter.setValues(ImmutableList.of("DATASET"));
    DataFetchingEnvironment mockEnv =
        mockEnv(
            new ContainerEntitiesInput(null, 0, 20, ImmutableList.of(filter)),
            mockCountLoader(42L),
            "total");

    new ContainerEntitiesResolver(mockClient).get(mockEnv).get();

    verifySearchCount(mockClient, 1);
  }

  @Test
  public void testNonDefaultQueryFallsBackToDirectSearch() throws Exception {
    // The loader forces query "*", so a real query must take the direct path.
    EntityClient mockClient = mock(EntityClient.class);
    stubEmptySearch(mockClient);
    DataFetchingEnvironment mockEnv =
        mockEnv(new ContainerEntitiesInput("sales", 0, 20, null), mockCountLoader(42L), "total");

    new ContainerEntitiesResolver(mockClient).get(mockEnv).get();

    verifySearchCount(mockClient, 1);
  }

  @Test
  public void testMissingInputArgumentFallsBackToDefaults() throws Exception {
    // `entities` with no argument at all must use DEFAULT_ENTITIES_INPUT (query "*", count 20).
    EntityClient mockClient = mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = mockEnv(null, mockCountLoader(11L), "total");

    assertEquals((int) new ContainerEntitiesResolver(mockClient).get(mockEnv).get().getTotal(), 11);
    verifySearchCount(mockClient, 0);
  }

  @Test
  public void testNullLoaderResultYieldsZeroTotal() throws Exception {
    // A failed aggregation surfaces as a null count; the resolver must report 0, not NPE.
    EntityClient mockClient = mock(EntityClient.class);
    @SuppressWarnings("unchecked")
    final DataLoader<String, Long> loader = mock(DataLoader.class);
    Mockito.when(loader.load(Mockito.anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));
    DataFetchingEnvironment mockEnv =
        mockEnv(new ContainerEntitiesInput(null, 0, 0, null), loader, "total");

    assertEquals((int) new ContainerEntitiesResolver(mockClient).get(mockEnv).get().getTotal(), 0);
  }

  @Test
  public void testDirectSearchFailureIsWrapped() throws Exception {
    // The direct path must not leak the raw client exception.
    EntityClient mockClient = mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                any(),
                any(),
                nullable(Filter.class),
                anyInt(),
                nullable(Integer.class),
                any()))
        .thenThrow(new RuntimeException("search is down"));
    DataFetchingEnvironment mockEnv =
        mockEnv(TEST_INPUT, mockCountLoader(1L), "total", "searchResults");

    final ExecutionException e =
        expectThrows(
            ExecutionException.class,
            () -> new ContainerEntitiesResolver(mockClient).get(mockEnv).get());
    assertTrue(e.getCause().getMessage().contains(CONTAINER_URN));
  }

  private static DataLoader<String, Long> mockCountLoader(final long total) {
    @SuppressWarnings("unchecked")
    final DataLoader<String, Long> loader = mock(DataLoader.class);
    Mockito.when(loader.load(Mockito.anyString()))
        .thenReturn(CompletableFuture.completedFuture(total));
    return loader;
  }

  private static DataFetchingEnvironment mockEnv(
      final ContainerEntitiesInput input,
      final DataLoader<String, Long> loader,
      final String... selectedFields) {
    QueryContext mockContext = mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getMergedField()).thenReturn(mergedEntitiesField(selectedFields));
    Mockito.when(mockEnv.getFragmentsByName()).thenReturn(Collections.emptyMap());
    Mockito.when(mockEnv.<String, Long>getDataLoader(ContainerEntityCountsBatchLoader.LOADER_NAME))
        .thenReturn(loader);

    Container parentContainer = new Container();
    parentContainer.setUrn(CONTAINER_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(parentContainer);
    return mockEnv;
  }

  /**
   * Builds a real query AST for {@code entities { <selectedFields> }} so the resolver reads the
   * selection the same way it does at runtime, exercising the actual code path rather than a mocked
   * selection set.
   */
  private static MergedField mergedEntitiesField(final String... selectedFields) {
    final SelectionSet.Builder selectionSet = SelectionSet.newSelectionSet();
    for (String name : selectedFields) {
      selectionSet.selection(Field.newField(name).build());
    }
    return MergedField.newMergedField(
            Field.newField("entities").selectionSet(selectionSet.build()).build())
        .build();
  }

  /** Variant of {@link #mockEnv} taking arbitrary AST selections rather than plain field names. */
  private static DataFetchingEnvironment mockEnvWithSelections(
      final DataLoader<String, Long> loader,
      final List<? extends Selection<?>> selections,
      final Map<String, FragmentDefinition> fragments) {
    final SelectionSet.Builder selectionSet = SelectionSet.newSelectionSet();
    selections.forEach(selectionSet::selection);
    final MergedField mergedField =
        MergedField.newMergedField(
                Field.newField("entities").selectionSet(selectionSet.build()).build())
            .build();
    return mockEnvWithMergedField(loader, mergedField, fragments);
  }

  private static DataFetchingEnvironment mockEnvWithMergedField(
      final DataLoader<String, Long> loader,
      final MergedField mergedField,
      final Map<String, FragmentDefinition> fragments) {
    final DataFetchingEnvironment mockEnv =
        mockEnv(new ContainerEntitiesInput(null, null, null, null), loader);
    Mockito.when(mockEnv.getMergedField()).thenReturn(mergedField);
    Mockito.when(mockEnv.getFragmentsByName()).thenReturn(fragments);
    return mockEnv;
  }

  private static void stubEmptySearch(final EntityClient mockClient) throws Exception {
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                any(),
                any(),
                nullable(Filter.class),
                anyInt(),
                nullable(Integer.class),
                any()))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray())
                .setMetadata(
                    new SearchResultMetadata().setAggregations(new AggregationMetadataArray())));
  }

  private static void verifySearchCount(final EntityClient mockClient, final int times)
      throws Exception {
    Mockito.verify(mockClient, Mockito.times(times))
        .searchAcrossEntities(
            any(), any(), any(), nullable(Filter.class), anyInt(), nullable(Integer.class), any());
  }
}
