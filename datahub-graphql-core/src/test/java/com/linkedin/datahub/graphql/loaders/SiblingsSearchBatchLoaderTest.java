package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.SIBLINGS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SiblingsSearchBatchLoaderTest {

  private static final String DBT_ORDERS =
      "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders,PROD)";
  private static final String DBT_USERS =
      "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.users,PROD)";
  private static final String SNOW_ORDERS =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)";
  private static final String SNOW_USERS =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.users,PROD)";

  private static final String SIBLINGS_FACET = "siblings";
  private static final String TEST_ACTOR = "urn:li:corpuser:test";
  private static final String TEST_VIEW = "urn:li:dataHubView:test";

  private EntityClient _entityClient;
  private ViewService _viewService;
  private QueryContext _context;

  @BeforeMethod
  public void setup() {
    _entityClient = Mockito.mock(EntityClient.class);
    _viewService = Mockito.mock(ViewService.class);
    _context = getMockAllowContext();
  }

  private static SiblingsSearchBatchLoader.Key key(final String urn, final int count) {
    return new SiblingsSearchBatchLoader.Key(urn, List.of("dataset"), "*", null, null, null, count);
  }

  private static Filter filterOn(final String field, final String value) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            CriterionUtils.buildCriterion(field, Condition.EQUAL, value)))));
  }

  private void stubView(final List<String> entityTypes, final Filter filter) {
    final DataHubViewInfo view =
        new DataHubViewInfo()
            .setName("test view")
            .setType(DataHubViewType.PERSONAL)
            .setCreated(new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(TEST_ACTOR)))
            .setLastModified(new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(TEST_ACTOR)))
            .setDefinition(
                new DataHubViewDefinition()
                    .setEntityTypes(new StringArray(entityTypes))
                    .setFilter(filter));
    Mockito.when(_viewService.getViewInfo(any(), any())).thenReturn(view);
  }

  /** Captures the filter and entity names the loader actually sent to the search layer. */
  private SearchAcrossEntitiesCall captureSearch() throws Exception {
    final ArgumentCaptor<List> names = ArgumentCaptor.forClass(List.class);
    final ArgumentCaptor<Filter> filter = ArgumentCaptor.forClass(Filter.class);
    Mockito.verify(_entityClient, Mockito.atLeastOnce())
        .searchAcrossEntities(
            any(),
            names.capture(),
            any(),
            filter.capture(),
            anyInt(),
            nullable(Integer.class),
            any(),
            any());
    return new SearchAcrossEntitiesCall(names.getValue(), filter.getValue());
  }

  private record SearchAcrossEntitiesCall(List<String> entityNames, Filter filter) {}

  /** Search response carrying the hits plus a {@code siblings} facet keyed by the queried urn. */
  private static SearchResult searchResult(
      final List<String> hitUrns, final Map<String, Long> totalsBySiblingUrn) {
    final SearchEntityArray entities = new SearchEntityArray();
    for (String urn : hitUrns) {
      entities.add(new SearchEntity().setEntity(UrnUtils.getUrn(urn)));
    }
    final AggregationMetadata agg =
        new AggregationMetadata()
            .setName(SIBLINGS_FACET)
            .setAggregations(new LongMap(totalsBySiblingUrn))
            .setFilterValues(new FilterValueArray());
    return new SearchResult()
        .setEntities(entities)
        .setNumEntities(hitUrns.size())
        .setFrom(0)
        .setPageSize(hitUrns.size())
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray(agg)));
  }

  /** Wires each hit urn to the sibling urns its {@code siblings} aspect lists. */
  private void stubSiblingAspects(final Map<String, List<String>> siblingsByHitUrn)
      throws Exception {
    final Map<Urn, EntityResponse> responses = new LinkedHashMap<>();
    siblingsByHitUrn.forEach(
        (hitUrn, siblingUrns) -> {
          final UrnArray siblings = new UrnArray();
          siblingUrns.forEach(s -> siblings.add(UrnUtils.getUrn(s)));
          final EnvelopedAspect aspect =
              new EnvelopedAspect()
                  .setValue(new Aspect(new Siblings().setSiblings(siblings).data()));
          responses.put(
              UrnUtils.getUrn(hitUrn),
              new EntityResponse()
                  .setUrn(UrnUtils.getUrn(hitUrn))
                  .setAspects(new EnvelopedAspectMap(Map.of(SIBLINGS_ASPECT_NAME, aspect))));
        });

    Mockito.when(_entityClient.batchGetV2(any(), any(), any(), any(), any())).thenReturn(responses);
  }

  private void stubSearch(final SearchResult result) throws Exception {
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

  private void verifySearchCount(final int times) throws Exception {
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
  public void testHitsAttributedToTheirOwnRequestingUrn() throws Exception {
    stubSearch(
        searchResult(List.of(SNOW_ORDERS, SNOW_USERS), Map.of(DBT_ORDERS, 1L, DBT_USERS, 1L)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS), SNOW_USERS, List.of(DBT_USERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 1), key(DBT_USERS, 1)), _context, _entityClient, _viewService);

    assertEquals(results.get(0).getSearchResults().size(), 1);
    assertEquals(results.get(0).getSearchResults().get(0).getEntity().getUrn(), SNOW_ORDERS);
    assertEquals(results.get(1).getSearchResults().get(0).getEntity().getUrn(), SNOW_USERS);
    // Both datasets answered by a single search rather than one each.
    verifySearchCount(1);
  }

  @Test
  public void testPerKeyTotalReadFromAggregation() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS), Map.of(DBT_ORDERS, 3L)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 1)), _context, _entityClient, _viewService);

    // The window returned one hit, but the facet reports the true sibling count.
    assertEquals(results.get(0).getTotal(), 3);
  }

  /**
   * The DataLoader key carries the query shape, so the same urn asked two different ways must not
   * share an answer. Getting this wrong is silent — one caller would receive the other's results.
   */
  @Test
  public void testSameUrnWithDifferentInputDoesNotShareAResult() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS), Map.of(DBT_ORDERS, 1L)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    SiblingsSearchBatchLoader.batchLoad(
        List.of(key(DBT_ORDERS, 1), key(DBT_ORDERS, 5)), _context, _entityClient, _viewService);

    // Different `count` is a different question, so the two keys cannot share one search.
    verifySearchCount(2);
  }

  /**
   * Keys with a value-equal input must land in one search. If the key ever regains identity
   * semantics this stays green functionally but silently stops batching, so assert the call count.
   */
  @Test
  public void testValueEqualInputsShareASingleSearch() throws Exception {
    stubSearch(
        searchResult(List.of(SNOW_ORDERS, SNOW_USERS), Map.of(DBT_ORDERS, 1L, DBT_USERS, 1L)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS), SNOW_USERS, List.of(DBT_USERS)));

    SiblingsSearchBatchLoader.batchLoad(
        List.of(key(DBT_ORDERS, 1), key(DBT_USERS, 1)), _context, _entityClient, _viewService);

    verifySearchCount(1);
  }

  @Test
  public void testResultsReturnedInKeyOrderWithUnmatchedKeyEmpty() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS), Map.of(DBT_ORDERS, 1L)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_USERS, 1), key(DBT_ORDERS, 1)), _context, _entityClient, _viewService);

    // DBT_USERS has no siblings: zero total, no hits, and it must stay in position 0.
    assertEquals(results.get(0).getTotal(), 0);
    assertTrue(results.get(0).getSearchResults().isEmpty());
    // `count` is how many entities came back, not the requested page size — the unbatched path
    // derives it from the response, so an empty result must report 0 here too.
    assertEquals(results.get(0).getCount(), 0);
    assertEquals(results.get(1).getCount(), 1);
    assertEquals(results.get(1).getSearchResults().get(0).getEntity().getUrn(), SNOW_ORDERS);
  }

  /**
   * Hits are shared across the chunk, so a urn with many siblings can crowd out its neighbours. A
   * key whose facet total exceeds the hits it was attributed must be re-queried on its own rather
   * than returning short.
   */
  @Test
  public void testStarvedKeyIsRequeriedOnItsOwn() throws Exception {
    final Map<String, SearchResult> byCallCount = new HashMap<>();
    byCallCount.put(
        "batch", searchResult(List.of(SNOW_ORDERS), Map.of(DBT_ORDERS, 1L, DBT_USERS, 1L)));
    byCallCount.put("single", searchResult(List.of(SNOW_USERS), Map.of(DBT_USERS, 1L)));

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
        .thenReturn(byCallCount.get("batch"), byCallCount.get("single"));

    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 1), key(DBT_USERS, 1)), _context, _entityClient, _viewService);

    // DBT_USERS was starved out of the shared window, so it gets its own search.
    verifySearchCount(2);
    assertEquals(results.get(1).getSearchResults().size(), 1);
    assertEquals(results.get(1).getSearchResults().get(0).getEntity().getUrn(), SNOW_USERS);
  }

  private static SiblingsSearchBatchLoader.Key keyWith(
      final String urn,
      final List<String> entityNames,
      final Filter inputFilter,
      final SearchFlags searchFlags,
      final String viewUrn) {
    return new SiblingsSearchBatchLoader.Key(
        urn, entityNames, "*", inputFilter, searchFlags, viewUrn, 1);
  }

  /** A view narrows both the entity types searched and the filter applied. */
  @Test
  public void testViewNarrowsEntityTypesAndFilter() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS), Map.of(DBT_ORDERS, 1L)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));
    stubView(List.of("dataset"), filterOn("origin", "PROD"));

    SiblingsSearchBatchLoader.batchLoad(
        List.of(keyWith(DBT_ORDERS, List.of("dataset", "chart"), null, null, TEST_VIEW)),
        _context,
        _entityClient,
        _viewService);

    final SearchAcrossEntitiesCall call = captureSearch();
    // "chart" is not in the view, so the intersection drops it.
    assertEquals(call.entityNames(), List.of("dataset"));
    // The view's own predicate has to survive into the query alongside the siblings filter.
    final String rendered = call.filter().toString();
    assertTrue(rendered.contains("origin"), "view filter missing from query: " + rendered);
    assertTrue(rendered.contains(SIBLINGS_FACET), "siblings filter missing: " + rendered);
  }

  /** A dangling view urn resolves to null; the request must still run on its own types. */
  @Test
  public void testMissingViewFallsBackToRequestedTypes() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS), Map.of(DBT_ORDERS, 1L)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));
    Mockito.when(_viewService.getViewInfo(any(), any())).thenReturn(null);

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(keyWith(DBT_ORDERS, List.of("dataset"), null, null, TEST_VIEW)),
            _context,
            _entityClient,
            _viewService);

    assertEquals(captureSearch().entityNames(), List.of("dataset"));
    assertEquals(results.get(0).getTotal(), 1);
  }

  /** When a view intersects to no entity types there is nothing to search, so skip the query. */
  @Test
  public void testViewWithNoOverlappingEntityTypesSkipsTheSearch() throws Exception {
    stubView(List.of("chart"), filterOn("origin", "PROD"));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(keyWith(DBT_ORDERS, List.of("dataset"), null, null, TEST_VIEW)),
            _context,
            _entityClient,
            _viewService);

    assertEquals(results.get(0).getTotal(), 0);
    assertEquals(results.get(0).getCount(), 0);
    assertTrue(results.get(0).getSearchResults().isEmpty());
    Mockito.verify(_entityClient, Mockito.never())
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

  /** A caller-supplied orFilters predicate must be ANDed with the siblings filter, not dropped. */
  @Test
  public void testCallerFilterIsCombinedWithSiblingsFilter() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS), Map.of(DBT_ORDERS, 1L)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    SiblingsSearchBatchLoader.batchLoad(
        List.of(
            keyWith(DBT_ORDERS, List.of("dataset"), filterOn("platform", "snowflake"), null, null)),
        _context,
        _entityClient,
        _viewService);

    final String rendered = captureSearch().filter().toString();
    assertTrue(rendered.contains("platform"), "caller filter missing: " + rendered);
    assertTrue(rendered.contains(SIBLINGS_FACET), "siblings filter missing: " + rendered);
  }

  /** Caller search flags are honoured, and the key's own instance is never mutated. */
  @Test
  public void testCallerSearchFlagsArePreservedAndKeyIsNotMutated() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS), Map.of(DBT_ORDERS, 1L)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    final SearchFlags flags = new SearchFlags().setSkipCache(true);
    final SiblingsSearchBatchLoader.Key k =
        keyWith(DBT_ORDERS, List.of("dataset"), null, flags, null);
    final int hashBefore = k.hashCode();

    SiblingsSearchBatchLoader.batchLoad(List.of(k), _context, _entityClient, _viewService);

    // The loader raises maxAggValues on a copy; mutating the key's own flags would change its
    // hash and corrupt any map it is already sitting in.
    assertFalse(flags.hasMaxAggValues(), "loader mutated the key's SearchFlags");
    assertEquals(k.hashCode(), hashBefore);
  }

  /** A search failure must surface, not resolve to "this dataset has no siblings". */
  @Test
  public void testSearchFailurePropagates() throws Exception {
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
        .thenThrow(new RuntimeException("elasticsearch unavailable"));

    assertThrows(
        RuntimeException.class,
        () ->
            SiblingsSearchBatchLoader.batchLoad(
                List.of(key(DBT_ORDERS, 1)), _context, _entityClient, _viewService));
  }

  /** Facet buckets for urns outside the chunk belong to other datasets and must be ignored. */
  @Test
  public void testOffChunkFacetBucketsAreIgnored() throws Exception {
    stubSearch(
        searchResult(
            List.of(SNOW_ORDERS), Map.of(DBT_ORDERS, 1L, "urn:li:dataset:(x,other,PROD)", 9L)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 1)), _context, _entityClient, _viewService);

    assertEquals(results.get(0).getTotal(), 1);
  }

  /** A hit whose siblings aspect is absent cannot be attributed and must not be invented. */
  @Test
  public void testHitWithoutSiblingsAspectIsNotAttributed() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS), Map.of(DBT_ORDERS, 0L)));
    Mockito.when(_entityClient.batchGetV2(any(), any(), any(), any(), any()))
        .thenReturn(
            Map.of(
                UrnUtils.getUrn(SNOW_ORDERS),
                new EntityResponse()
                    .setUrn(UrnUtils.getUrn(SNOW_ORDERS))
                    .setAspects(new EnvelopedAspectMap(Map.of()))));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 1)), _context, _entityClient, _viewService);

    assertEquals(results.get(0).getTotal(), 0);
    assertTrue(results.get(0).getSearchResults().isEmpty());
  }
}
