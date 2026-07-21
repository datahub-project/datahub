package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainEntitiesInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.loaders.DomainEntityCountsBatchLoader;
import com.linkedin.datahub.graphql.loaders.DomainEntityCountsBatchLoader.DomainCountKey;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
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
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.dataloader.DataLoader;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DomainEntitiesResolverTest {

  private static final DomainEntitiesInput TEST_INPUT =
      new DomainEntitiesInput(null, 0, 20, Collections.emptyList());

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    final String childUrn = "urn:li:dataset:(test,test,test)";
    final String domainUrn = "urn:li:domain:test-domain";

    final Criterion filterCriterion = buildCriterion("domains.keyword", Condition.EQUAL, domainUrn);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                Mockito.eq(
                    SEARCHABLE_ENTITY_TYPES.stream()
                        .map(EntityTypeMapper::getName)
                        .collect(Collectors.toList())),
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

    DomainEntitiesResolver resolver = new DomainEntitiesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Domain parentDomain = new Domain();
    parentDomain.setUrn(domainUrn);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDomain);

    // Data Assertions
    assertEquals((int) resolver.get(mockEnv).get().getStart(), 0);
    assertEquals((int) resolver.get(mockEnv).get().getCount(), 1);
    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 1);
    assertEquals(resolver.get(mockEnv).get().getSearchResults().size(), 1);
    assertEquals(
        resolver.get(mockEnv).get().getSearchResults().get(0).getEntity().getUrn(), childUrn);
  }

  @Test
  public void testCountOnlyServedFromBatchedLoader() throws Exception {
    final String domainUrn = "urn:li:domain:test-domain";
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final DomainEntitiesResolver resolver = new DomainEntitiesResolver(mockClient);

    // count == 0, no filters, no query → the count-only fast path with a no-constraint key.
    final DomainEntitiesInput input = new DomainEntitiesInput(null, 0, 0, Collections.emptyList());
    final DataFetchingEnvironment mockEnv = countOnlyEnv(input, new DomainCountKey(domainUrn), 10L);

    final var results = resolver.get(mockEnv).get();

    assertEquals((int) results.getTotal(), 10);
    assertEquals((int) results.getCount(), 0);
    assertTrue(results.getSearchResults().isEmpty());
    // The batched loader served it — no per-domain search was issued.
    Mockito.verifyNoInteractions(mockClient);
  }

  @Test
  public void testCountOnlyWithNegatedEntityTypeFilterFromLoader() throws Exception {
    final String domainUrn = "urn:li:domain:test-domain";
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final DomainEntitiesResolver resolver = new DomainEntitiesResolver(mockClient);

    // The "entities" alias shape: count 0, single negated _entityType filter.
    final FacetFilterInput entityTypeFilter = new FacetFilterInput();
    entityTypeFilter.setField("_entityType");
    entityTypeFilter.setNegated(true);
    entityTypeFilter.setValues(
        List.of(EntityType.DATA_PRODUCT.toString(), EntityType.DOMAIN.toString()));
    final DomainEntitiesInput input =
        new DomainEntitiesInput(null, 0, 0, List.of(entityTypeFilter));

    // The resolver must build a negated _entityType key with the raw GraphQL enum values.
    final DomainCountKey expectedKey =
        new DomainCountKey(
            domainUrn,
            List.of(EntityType.DATA_PRODUCT.toString(), EntityType.DOMAIN.toString()),
            true);
    final DataFetchingEnvironment mockEnv = countOnlyEnv(input, expectedKey, 16L);

    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 16);
    Mockito.verifyNoInteractions(mockClient);
  }

  @Test
  public void testCountOnlyWithNonEntityTypeFilterFallsBackToDirectSearch() throws Exception {
    final String domainUrn = "urn:li:domain:test-domain";
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), Mockito.anyInt(), any(), any()))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(42)
                .setEntities(new SearchEntityArray())
                .setMetadata(
                    new SearchResultMetadata().setAggregations(new AggregationMetadataArray())));
    final DomainEntitiesResolver resolver = new DomainEntitiesResolver(mockClient);

    // count 0 but a non-_entityType filter is not derivable from the breakdown → direct search.
    final FacetFilterInput platformFilter = new FacetFilterInput();
    platformFilter.setField("platform");
    platformFilter.setValues(List.of("urn:li:dataPlatform:snowflake"));
    final DomainEntitiesInput input = new DomainEntitiesInput(null, 0, 0, List.of(platformFilter));

    final QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    final Domain parentDomain = new Domain();
    parentDomain.setUrn(domainUrn);
    Mockito.when(mockEnv.getSource()).thenReturn(parentDomain);

    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 42);
    // The direct path ran; the batched loader was never consulted.
    Mockito.verify(mockClient, Mockito.times(1))
        .searchAcrossEntities(any(), any(), any(), any(), Mockito.anyInt(), any(), any());
    Mockito.verify(mockEnv, Mockito.never()).getDataLoader(any());
  }

  @Test
  public void testCountOnlyWithNullValuesEntityTypeFilterFallsBackToDirectSearch()
      throws Exception {
    // Regression: an _entityType filter that omits `values` (schema type [String!], nullable) must
    // not crash the fast path — it falls back to the direct search, which handles null values.
    final FacetFilterInput noValues = new FacetFilterInput();
    noValues.setField("_entityType");
    // values intentionally left null
    assertFallsBackToDirectSearch(new DomainEntitiesInput(null, 0, 0, List.of(noValues)));
  }

  /** Runs the resolver and asserts it took the direct search path, never the batched loader. */
  private static void assertFallsBackToDirectSearch(final DomainEntitiesInput input)
      throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), any(), any(), Mockito.anyInt(), any(), any()))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(7)
                .setEntities(new SearchEntityArray())
                .setMetadata(
                    new SearchResultMetadata().setAggregations(new AggregationMetadataArray())));
    final DomainEntitiesResolver resolver = new DomainEntitiesResolver(mockClient);

    final QueryContext mockContext = getMockAllowContext();
    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    final Domain parentDomain = new Domain();
    parentDomain.setUrn("urn:li:domain:test-domain");
    Mockito.when(mockEnv.getSource()).thenReturn(parentDomain);

    // Must not throw (regression guard for null values) and must use the direct path.
    assertEquals((int) resolver.get(mockEnv).get().getTotal(), 7);
    Mockito.verify(mockClient, Mockito.times(1))
        .searchAcrossEntities(any(), any(), any(), any(), Mockito.anyInt(), any(), any());
    Mockito.verify(mockEnv, Mockito.never()).getDataLoader(any());
  }

  /**
   * Builds a mock environment whose {@link DomainEntityCountsBatchLoader} loader resolves {@code
   * expectedKey} to {@code total}, for exercising the count-only fast path. If the resolver builds
   * a key other than {@code expectedKey}, {@code load} returns null and the test fails — so this
   * also asserts the resolver constructs the right key.
   */
  private static DataFetchingEnvironment countOnlyEnv(
      final DomainEntitiesInput input, final DomainCountKey expectedKey, final long total) {
    final QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    @SuppressWarnings("unchecked")
    final DataLoader<DomainCountKey, Long> mockLoader = Mockito.mock(DataLoader.class);
    Mockito.when(mockLoader.load(expectedKey)).thenReturn(CompletableFuture.completedFuture(total));

    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(
            mockEnv.<DomainCountKey, Long>getDataLoader(DomainEntityCountsBatchLoader.LOADER_NAME))
        .thenReturn(mockLoader);

    final Domain parentDomain = new Domain();
    parentDomain.setUrn(expectedKey.getDomainUrn());
    Mockito.when(mockEnv.getSource()).thenReturn(parentDomain);
    return mockEnv;
  }
}
