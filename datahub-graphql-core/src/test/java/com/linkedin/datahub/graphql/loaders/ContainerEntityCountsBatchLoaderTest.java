package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.linkedin.data.template.LongMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.dataloader.DataLoader;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ContainerEntityCountsBatchLoaderTest {
  private static final String SALES = "urn:li:container:sales";
  private static final String FINANCE = "urn:li:container:finance";
  private static final String OFF_CHUNK = "urn:li:container:notrequested";
  private static final String CONTAINER_FACET = "container";

  private EntityClient _entityClient;
  private QueryContext _context;

  @BeforeMethod
  public void setup() {
    _entityClient = Mockito.mock(EntityClient.class);
    _context = getMockAllowContext();
  }

  /** A search result carrying a {@code container} facet keyed by container urn. */
  private static SearchResult resultWithContainerCounts(Map<String, Long> countsByContainerUrn) {
    final AggregationMetadata agg =
        new AggregationMetadata()
            .setName(CONTAINER_FACET)
            .setAggregations(new LongMap(countsByContainerUrn))
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

  private void verifySearchCount(int times) throws Exception {
    Mockito.verify(_entityClient, Mockito.times(times))
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
  public void testCountsDistributedByContainerUrn() throws Exception {
    stubSearch(resultWithContainerCounts(Map.of(SALES, 18L, FINANCE, 5L)));

    final List<Long> results =
        ContainerEntityCountsBatchLoader.batchLoad(
            List.of(SALES, FINANCE), _context, _entityClient);

    assertEquals(results, List.of(18L, 5L));
    // One chunk covers the whole page of containers, so one aggregation answers all of them.
    verifySearchCount(1);
  }

  @Test
  public void testResultsInKeyOrderWithAbsentContainerZeroed() throws Exception {
    // FINANCE has no matching assets, so the facet omits it entirely.
    stubSearch(resultWithContainerCounts(Map.of(SALES, 3L)));

    final List<Long> results =
        ContainerEntityCountsBatchLoader.batchLoad(
            List.of(FINANCE, SALES), _context, _entityClient);

    assertEquals(results, List.of(0L, 3L));
  }

  @Test
  public void testDuplicateKeysShareOneLookupAndBothResolve() throws Exception {
    // The same container can appear many times in one response (e.g. repeated across lineage
    // paths). It must be queried once but answered at every key position.
    stubSearch(resultWithContainerCounts(Map.of(SALES, 7L)));

    final List<Long> results =
        ContainerEntityCountsBatchLoader.batchLoad(
            List.of(SALES, SALES, SALES), _context, _entityClient);

    assertEquals(results, List.of(7L, 7L, 7L));
    verifySearchCount(1);
  }

  @Test
  public void testOffChunkContainersIgnored() throws Exception {
    stubSearch(resultWithContainerCounts(Map.of(SALES, 4L, OFF_CHUNK, 99L)));

    final List<Long> results =
        ContainerEntityCountsBatchLoader.batchLoad(List.of(SALES), _context, _entityClient);

    assertEquals(results, List.of(4L));
  }

  @Test
  public void testSearchFailurePropagatesInsteadOfYieldingZero() throws Exception {
    // A backend search failure must surface as an error, not a fabricated 0 that reads as an empty
    // container and would silently mask a search outage behind wrong UI counts. The cause is
    // preserved so the underlying failure remains diagnosable.
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
        .thenThrow(new RuntimeException("search is down"));

    final RuntimeException thrown =
        expectThrows(
            RuntimeException.class,
            () ->
                ContainerEntityCountsBatchLoader.batchLoad(
                    List.of(SALES, FINANCE), _context, _entityClient));

    assertNotNull(thrown.getCause());
  }

  @Test
  public void testLargeFanOutIsChunkedAcrossMultipleSearches() throws Exception {
    stubSearch(resultWithContainerCounts(Map.of()));

    final List<String> keys = new ArrayList<>();
    for (int i = 0; i < 60; i++) {
      keys.add("urn:li:container:c" + i);
    }

    final List<Long> results =
        ContainerEntityCountsBatchLoader.batchLoad(keys, _context, _entityClient);

    assertEquals(results.size(), 60);
    // 60 containers at 25 per aggregation → 3 searches: a constant factor, not 60.
    verifySearchCount(3);
  }

  @Test
  public void testCreatedDataLoaderResolvesThroughBatchFunction() throws Exception {
    // Exercises create() — the path GmsGraphQLEngine actually registers — rather than calling
    // batchLoad directly, so the DataLoader wiring and context provider are covered too.
    stubSearch(resultWithContainerCounts(Map.of(SALES, 9L, FINANCE, 2L)));

    final DataLoader<String, Long> loader =
        ContainerEntityCountsBatchLoader.create(_entityClient, _context);
    final CompletableFuture<Long> sales = loader.load(SALES);
    final CompletableFuture<Long> finance = loader.load(FINANCE);
    loader.dispatch();

    assertEquals(sales.get(30, TimeUnit.SECONDS), Long.valueOf(9L));
    assertEquals(finance.get(30, TimeUnit.SECONDS), Long.valueOf(2L));
    // Both keys coalesced into a single batch, hence a single aggregation.
    verifySearchCount(1);
  }

  @Test
  public void testNullSearchResultYieldsZeros() throws Exception {
    // Defensive: a null result from the search layer must not NPE.
    stubSearch(null);

    assertEquals(
        ContainerEntityCountsBatchLoader.batchLoad(
            List.of(SALES, FINANCE), _context, _entityClient),
        List.of(0L, 0L));
  }

  @Test
  public void testUnrelatedFacetIsIgnoredWhileContainerFacetIsCounted() throws Exception {
    // A same-named bucket on a different facet must not leak into container counts. Asserting
    // FINANCE's real count alongside proves the unrelated facet was skipped rather than the
    // whole aggregation block being discarded.
    final AggregationMetadata unrelated =
        new AggregationMetadata()
            .setName("platform")
            .setAggregations(new LongMap(Map.of(SALES, 77L)))
            .setFilterValues(new FilterValueArray());
    final AggregationMetadata containers =
        new AggregationMetadata()
            .setName(CONTAINER_FACET)
            .setAggregations(new LongMap(Map.of(FINANCE, 3L)))
            .setFilterValues(new FilterValueArray());
    stubSearch(
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(0)
            .setFrom(0)
            .setPageSize(0)
            .setMetadata(
                new SearchResultMetadata()
                    .setAggregations(new AggregationMetadataArray(unrelated, containers))));

    // SALES appears only under the platform facet, so it must stay 0.
    assertEquals(
        ContainerEntityCountsBatchLoader.batchLoad(
            List.of(SALES, FINANCE), _context, _entityClient),
        List.of(0L, 3L));
  }

  @Test
  public void testMalformedContainerFacetPropagates() throws Exception {
    // `aggregations` is a required PDL field, so omitting it makes the getter throw. Like a search
    // failure, that must surface rather than being reported as a count of zero.
    final AggregationMetadata empty =
        new AggregationMetadata().setName(CONTAINER_FACET).setFilterValues(new FilterValueArray());
    stubSearch(
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setNumEntities(0)
            .setFrom(0)
            .setPageSize(0)
            .setMetadata(
                new SearchResultMetadata().setAggregations(new AggregationMetadataArray(empty))));

    assertNotNull(
        expectThrows(
                RuntimeException.class,
                () ->
                    ContainerEntityCountsBatchLoader.batchLoad(
                        List.of(SALES), _context, _entityClient))
            .getCause());
  }

  @Test
  public void testMalformedUrnKeyIsSkippedWithoutFailingTheBatch() throws Exception {
    // A malformed key must be dropped from the filter and answered as zero, while its
    // well-formed neighbours still resolve.
    stubSearch(resultWithContainerCounts(Map.of(SALES, 4L)));

    assertEquals(
        ContainerEntityCountsBatchLoader.batchLoad(
            List.of("not-a-urn", SALES), _context, _entityClient),
        List.of(0L, 4L));
  }
}
