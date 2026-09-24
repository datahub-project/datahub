package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.linkedin.data.template.LongMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.loaders.DomainEntityCountsBatchLoader.DomainCountKey;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainEntityCountsBatchLoaderTest {
  private static final String MARKETING = "urn:li:domain:marketing";
  private static final String FINANCE = "urn:li:domain:finance";
  private static final String OFF_CHUNK = "urn:li:domain:notrequested";
  private static final String DOMAINS_FACET = "domains";

  private EntityClient _entityClient;
  private QueryContext _context;

  @BeforeMethod
  public void setup() {
    _entityClient = Mockito.mock(EntityClient.class);
    _context = getMockAllowContext();
  }

  /** A search result carrying a {@code domains} facet keyed by domain urn. */
  private static SearchResult resultWithDomainCounts(Map<String, Long> countsByDomainUrn) {
    final AggregationMetadata agg =
        new AggregationMetadata()
            .setName(DOMAINS_FACET)
            .setAggregations(new LongMap(countsByDomainUrn))
            .setFilterValues(new FilterValueArray());
    return new SearchResult()
        .setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setFrom(0)
        .setPageSize(0)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray(agg)));
  }

  private void stubSearch(SearchResult result) throws Exception {
    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(),
                any(),
                any(),
                nullable(Filter.class),
                anyInt(),
                nullable(Integer.class),
                any(),
                any()))
        .thenReturn(result);
  }

  @Test
  public void testCountsDistributedByDomainUrn() throws Exception {
    stubSearch(resultWithDomainCounts(Map.of(MARKETING, 18L, FINANCE, 5L)));

    final List<Long> results =
        DomainEntityCountsBatchLoader.batchLoad(
            List.of(new DomainCountKey(MARKETING), new DomainCountKey(FINANCE)),
            _context,
            _entityClient);

    assertEquals(results, List.of(18L, 5L));
    // One constraint (no entity-type filter), one chunk → one aggregation.
    Mockito.verify(_entityClient, Mockito.times(1))
        .searchAcrossEntities(
            any(),
            any(),
            any(),
            nullable(Filter.class),
            anyInt(),
            nullable(Integer.class),
            any(),
            any());
  }

  @Test
  public void testEmptyConfiguredSearchScopeSkipsSearchAndReturnsZeros() throws Exception {
    // An empty getSearchEntityNames result means "search no entity types". It must not reach
    // searchAcrossEntities, which would read the empty list as "all non-empty indices" and count
    // far more than the configured scope allows.
    final SearchContext emptyScope =
        SearchContext.EMPTY.toBuilder().defaultSearchEntityNames(List.of()).build();
    final OperationContext opContext = Mockito.mock(OperationContext.class);
    Mockito.when(opContext.getSearchContext()).thenReturn(emptyScope);
    final QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getOperationContext()).thenReturn(opContext);

    final List<Long> results =
        DomainEntityCountsBatchLoader.batchLoad(
            List.of(new DomainCountKey(MARKETING), new DomainCountKey(FINANCE)),
            context,
            _entityClient);

    assertEquals(results, List.of(0L, 0L));
    Mockito.verifyNoInteractions(_entityClient);
  }

  @Test
  public void testDistinctConstraintsIssueSeparateSearchesAndAttributeIndependently()
      throws Exception {
    // Same domain, two different entity-type constraints (the "entities" and "dataProducts"
    // aliases) → two aggregations, one per constraint. Keys are grouped in insertion order, so the
    // first search answers the negated group and the second the positive group; returning distinct
    // per-invocation results proves each constraint's count is attributed to its own key (not
    // conflated by a grouping/equals bug).
    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(),
                any(),
                any(),
                nullable(Filter.class),
                anyInt(),
                nullable(Integer.class),
                any(),
                any()))
        .thenReturn(
            resultWithDomainCounts(Map.of(MARKETING, 16L)), // negated group ("entities")
            resultWithDomainCounts(Map.of(MARKETING, 3L))); // positive group ("dataProducts")

    final List<Long> results =
        DomainEntityCountsBatchLoader.batchLoad(
            List.of(
                new DomainCountKey(MARKETING, List.of("DATA_PRODUCT", "DOMAIN"), true),
                new DomainCountKey(MARKETING, List.of("DATA_PRODUCT"), false)),
            _context,
            _entityClient);

    assertEquals(results, List.of(16L, 3L));
    Mockito.verify(_entityClient, Mockito.times(2))
        .searchAcrossEntities(
            any(),
            any(),
            any(),
            nullable(Filter.class),
            anyInt(),
            nullable(Integer.class),
            any(),
            any());
  }

  @Test
  public void testResultsInKeyOrderWithAbsentDomainZeroed() throws Exception {
    stubSearch(resultWithDomainCounts(Map.of(MARKETING, 5L)));

    // finance absent from the facet (no visible assets) and requested first.
    final List<Long> results =
        DomainEntityCountsBatchLoader.batchLoad(
            List.of(new DomainCountKey(FINANCE), new DomainCountKey(MARKETING)),
            _context,
            _entityClient);

    assertEquals(results, List.of(0L, 5L));
  }

  @Test
  public void testOffChunkDomainsIgnored() throws Exception {
    // A multi-domain asset can surface a domain that was never requested; it must not appear as an
    // extra result nor perturb a requested domain's count.
    stubSearch(resultWithDomainCounts(Map.of(OFF_CHUNK, 99L, MARKETING, 5L)));

    final List<Long> results =
        DomainEntityCountsBatchLoader.batchLoad(
            List.of(new DomainCountKey(MARKETING)), _context, _entityClient);

    assertEquals(results, List.of(5L));
  }

  @Test
  public void testSearchFailurePropagatesInsteadOfYieldingZero() throws Exception {
    // A backend search failure must surface as an error, not a fabricated 0 that reads as an empty
    // domain and would silently mask a search outage behind wrong UI counts. The cause is preserved
    // so the underlying failure remains diagnosable.
    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(),
                any(),
                any(),
                nullable(Filter.class),
                anyInt(),
                nullable(Integer.class),
                any(),
                any()))
        .thenThrow(new RuntimeException("es down"));

    final RuntimeException thrown =
        expectThrows(
            RuntimeException.class,
            () ->
                DomainEntityCountsBatchLoader.batchLoad(
                    List.of(new DomainCountKey(MARKETING), new DomainCountKey(FINANCE)),
                    _context,
                    _entityClient));

    assertNotNull(thrown.getCause());
  }

  @Test
  public void testLargeFanOutIsChunkedAcrossMultipleSearches() throws Exception {
    stubSearch(resultWithDomainCounts(Map.of()));

    // 30 domains under one constraint exceeds the 25-per-aggregation chunk size → two searches.
    final List<DomainCountKey> keys = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      keys.add(new DomainCountKey("urn:li:domain:d" + i));
    }

    final List<Long> results =
        DomainEntityCountsBatchLoader.batchLoad(keys, _context, _entityClient);

    assertEquals(results.size(), 30);
    Mockito.verify(_entityClient, Mockito.times(2))
        .searchAcrossEntities(
            any(),
            any(),
            any(),
            nullable(Filter.class),
            anyInt(),
            nullable(Integer.class),
            any(),
            any());
  }
}
