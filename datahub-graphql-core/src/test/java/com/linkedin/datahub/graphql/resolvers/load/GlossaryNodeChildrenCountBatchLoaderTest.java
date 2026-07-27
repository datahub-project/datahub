package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.utils.SearchUtil.AGGREGATION_SEPARATOR_CHAR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GlossaryNodeChildrenCount;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.r2.RemoteInvocationException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlossaryNodeChildrenCountBatchLoaderTest {

  private static final String NODE_A = "urn:li:glossaryNode:a";
  private static final String NODE_B = "urn:li:glossaryNode:b";
  private static final String NODE_C = "urn:li:glossaryNode:c";

  private EntityClient entityClient;
  private QueryContext context;
  private GlossaryNodeChildrenCountBatchLoader loader;

  @BeforeMethod
  public void setupTest() {
    entityClient = Mockito.mock(EntityClient.class);
    context = getMockAllowContext();
    loader = new GlossaryNodeChildrenCountBatchLoader(entityClient);
  }

  @Test
  public void testBatchLoadResolvesAllParentsWithASingleQuery() throws Exception {
    Mockito.when(
            entityClient.searchAcrossEntities(
                any(), any(), eq("*"), any(Filter.class), eq(0), eq(0), any(), any()))
        .thenReturn(
            searchResult(
                10L,
                Map.of(
                    nestedKey(NODE_A, "glossaryterm"), 5L,
                    nestedKey(NODE_A, "glossarynode"), 3L,
                    nestedKey(NODE_B, "glossaryterm"), 2L)));

    final List<GlossaryNodeChildrenCount> results =
        loader.batchLoad(List.of(NODE_A, NODE_B, NODE_C), context);

    assertEquals(results.size(), 3);
    assertCount(results.get(0), 5, 3);
    assertCount(results.get(1), 2, 0);
    // A parent with no children buckets is a genuine leaf, not a miss.
    assertCount(results.get(2), 0, 0);

    // The whole point of the loader: one ES round trip for the whole batch.
    Mockito.verify(entityClient, Mockito.times(1))
        .searchAcrossEntities(any(), any(), eq("*"), any(Filter.class), eq(0), eq(0), any(), any());
  }

  @Test
  public void testBatchLoadFiltersOnAllParentsAndRequestsTheNestedFacet() throws Exception {
    Mockito.when(
            entityClient.searchAcrossEntities(
                any(), any(), eq("*"), any(Filter.class), eq(0), eq(0), any(), any()))
        .thenReturn(searchResult(0L, Map.of()));

    loader.batchLoad(List.of(NODE_A, NODE_B), context);

    final ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    final ArgumentCaptor<List> facetCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(entityClient)
        .searchAcrossEntities(
            any(),
            any(),
            eq("*"),
            filterCaptor.capture(),
            eq(0),
            eq(0),
            any(),
            facetCaptor.capture());

    assertEquals(
        facetCaptor.getValue(), List.of("parentNode" + AGGREGATION_SEPARATOR_CHAR + "_entityType"));

    final Criterion criterion =
        filterCaptor.getValue().getOr().get(0).getAnd().stream()
            .filter(c -> "parentNode".equals(c.getField()))
            .findFirst()
            .orElseThrow();
    assertEquals(new HashSet<>(criterion.getValues()), Set.of(NODE_A, NODE_B));
  }

  @Test
  public void testBatchLoadChunksBatchesLargerThanTheAggregationBucketBudget() throws Exception {
    final int keyCount = GlossaryNodeChildrenCountBatchLoader.MAX_PARENTS_PER_QUERY + 1;
    final List<String> keys =
        IntStream.range(0, keyCount)
            .mapToObj(i -> "urn:li:glossaryNode:node-" + i)
            .collect(Collectors.toList());

    Mockito.when(
            entityClient.searchAcrossEntities(
                any(), any(), eq("*"), any(Filter.class), eq(0), eq(0), any(), any()))
        .thenAnswer(
            invocation -> {
              final Filter filter = invocation.getArgument(3);
              final List<String> parents = parentValues(filter);
              assertTrue(
                  parents.size() <= GlossaryNodeChildrenCountBatchLoader.MAX_PARENTS_PER_QUERY,
                  "chunk exceeded the aggregation bucket budget: " + parents.size());
              return searchResult(
                  parents.size(),
                  parents.stream()
                      .collect(Collectors.toMap(p -> nestedKey(p, "glossaryterm"), p -> 1L)));
            });

    final List<GlossaryNodeChildrenCount> results = loader.batchLoad(keys, context);

    assertEquals(results.size(), keyCount);
    results.forEach(count -> assertCount(count, 1, 0));
    Mockito.verify(entityClient, Mockito.times(2))
        .searchAcrossEntities(any(), any(), eq("*"), any(Filter.class), eq(0), eq(0), any(), any());
  }

  @Test
  public void testBatchLoadFallsBackToPerParentQueriesWhenBucketsAreTruncated() throws Exception {
    // Aggregated counts summing below the total hit count mean the parentNode terms
    // aggregation dropped buckets, so some parents would silently report zero children.
    Mockito.when(
            entityClient.searchAcrossEntities(
                any(), any(), eq("*"), any(Filter.class), eq(0), eq(0), any(), any()))
        .thenAnswer(
            invocation -> {
              final List<String> parents = parentValues(invocation.getArgument(3));
              if (parents.size() > 1) {
                return searchResult(100L, Map.of(nestedKey(NODE_A, "glossaryterm"), 1L));
              }
              final String parent = parents.get(0);
              return NODE_A.equals(parent)
                  ? searchResult(7L, Map.of(nestedKey(NODE_A, "glossaryterm"), 7L))
                  : searchResult(4L, Map.of(nestedKey(NODE_B, "glossarynode"), 4L));
            });

    final List<GlossaryNodeChildrenCount> results =
        loader.batchLoad(List.of(NODE_A, NODE_B), context);

    assertCount(results.get(0), 7, 0);
    assertCount(results.get(1), 0, 4);
    // One truncated batch query plus one query per parent in the retry.
    Mockito.verify(entityClient, Mockito.times(3))
        .searchAcrossEntities(any(), any(), eq("*"), any(Filter.class), eq(0), eq(0), any(), any());
  }

  @Test
  public void testBatchLoadPreservesTheSearchFailureCause() throws Exception {
    final RemoteInvocationException cause = new RemoteInvocationException("search unavailable");
    Mockito.when(
            entityClient.searchAcrossEntities(
                any(), any(), eq("*"), any(Filter.class), anyInt(), anyInt(), any(), any()))
        .thenThrow(cause);

    final RuntimeException thrown =
        expectThrows(
            RuntimeException.class, () -> loader.batchLoad(List.of(NODE_A, NODE_B), context));
    assertEquals(thrown.getCause(), cause);
  }

  private static void assertCount(
      final GlossaryNodeChildrenCount actual, final int termsCount, final int nodesCount) {
    assertEquals(actual.getTermsCount(), termsCount);
    assertEquals(actual.getNodesCount(), nodesCount);
  }

  private static String nestedKey(final String parentUrn, final String entityType) {
    return parentUrn + AGGREGATION_SEPARATOR_CHAR + entityType;
  }

  @SuppressWarnings("unchecked")
  private static List<String> parentValues(final Filter filter) {
    return filter.getOr().get(0).getAnd().stream()
        .filter(c -> "parentNode".equals(c.getField()))
        .findFirst()
        .map(c -> new ArrayList<>(c.getValues()))
        .orElseGet(ArrayList::new);
  }

  private static SearchResult searchResult(
      final long numEntities, final Map<String, Long> nestedFacetCounts) {
    final FilterValueArray filterValues = new FilterValueArray();
    nestedFacetCounts.forEach(
        (value, count) -> filterValues.add(new FilterValue().setValue(value).setFacetCount(count)));
    final AggregationMetadata aggregation =
        new AggregationMetadata()
            .setName("parentNode" + AGGREGATION_SEPARATOR_CHAR + "_entityType")
            .setFilterValues(filterValues);
    return new SearchResult()
        .setNumEntities((int) numEntities)
        .setMetadata(
            new SearchResultMetadata()
                .setAggregations(new AggregationMetadataArray(List.of(aggregation))));
  }
}
