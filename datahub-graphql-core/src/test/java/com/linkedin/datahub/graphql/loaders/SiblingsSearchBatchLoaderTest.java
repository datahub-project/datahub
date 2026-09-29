package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.SIBLINGS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
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
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
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
        .scrollAcrossEntities(
            any(),
            names.capture(),
            any(),
            filter.capture(),
            nullable(String.class),
            nullable(String.class),
            any(),
            nullable(Integer.class),
            any());
    return new SearchAcrossEntitiesCall(names.getValue(), filter.getValue());
  }

  private record SearchAcrossEntitiesCall(List<String> entityNames, Filter filter) {}

  /** A complete chunk page: every matching document was returned. */
  private static ScrollResult searchResult(final List<String> hitUrns) {
    return searchResult(hitUrns, hitUrns.size());
  }

  /** A chunk page reporting {@code matched} total matches, which may exceed the hits returned. */
  private static ScrollResult searchResult(final List<String> hitUrns, final int matched) {
    final SearchEntityArray entities = new SearchEntityArray();
    for (String urn : hitUrns) {
      // SearchRequestHandler stamps every hit with its own cursor; the loader reads it.
      entities.add(
          new SearchEntity()
              .setEntity(UrnUtils.getUrn(urn))
              .setExtraFields(new StringMap(Map.of("scrollId", "cursor-for-" + urn))));
    }
    return new ScrollResult()
        .setEntities(entities)
        .setNumEntities(matched)
        .setPageSize(hitUrns.size())
        .setMetadata(new SearchResultMetadata());
  }

  /** A chunk page whose hits carry no cursor, as if SearchRequestHandler had not stamped one. */
  private static ScrollResult searchResultWithoutCursors(final List<String> hitUrns) {
    final SearchEntityArray entities = new SearchEntityArray();
    for (String urn : hitUrns) {
      entities.add(new SearchEntity().setEntity(UrnUtils.getUrn(urn)));
    }
    return new ScrollResult()
        .setEntities(entities)
        .setNumEntities(hitUrns.size())
        .setPageSize(hitUrns.size())
        .setMetadata(new SearchResultMetadata());
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

  private void stubSearch(final ScrollResult result) throws Exception {
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
        .thenReturn(result);
  }

  private void verifySearchCount(final int times) throws Exception {
    Mockito.verify(_entityClient, Mockito.times(times))
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
  }

  @Test
  public void testHitsAttributedToTheirOwnRequestingUrn() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS, SNOW_USERS)));
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

  /** total counts every attributed hit; the requested count only bounds what is returned. */
  @Test
  public void testPerKeyTotalCountedFromAttributedHits() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS, SNOW_USERS, SNOW_ORDERS_B)));
    stubSiblingAspects(
        Map.of(
            SNOW_ORDERS, List.of(DBT_ORDERS),
            SNOW_USERS, List.of(DBT_ORDERS),
            SNOW_ORDERS_B, List.of(DBT_ORDERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 1)), _context, _entityClient, _viewService);

    assertEquals(results.get(0).getTotal(), 3);
    assertEquals(results.get(0).getCount(), 1);
    verifySearchCount(1);
  }

  /**
   * The DataLoader key carries the query shape, so the same urn asked two different ways must not
   * share an answer. Getting this wrong is silent — one caller would receive the other's results.
   */
  @Test
  public void testSameUrnWithDifferentInputDoesNotShareAResult() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
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
    stubSearch(searchResult(List.of(SNOW_ORDERS, SNOW_USERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS), SNOW_USERS, List.of(DBT_USERS)));

    SiblingsSearchBatchLoader.batchLoad(
        List.of(key(DBT_ORDERS, 1), key(DBT_USERS, 1)), _context, _entityClient, _viewService);

    verifySearchCount(1);
  }

  @Test
  public void testResultsReturnedInKeyOrderWithUnmatchedKeyEmpty() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
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
   * A short page means some key's siblings did not all fit, and the response cannot say which. When
   * the whole match set fits the ceiling, one resized retry answers the chunk — the per-key
   * fallback would cost a query per key instead.
   */
  @Test
  public void testTruncatedWindowRetriesTheChunkOnce() throws Exception {
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
        // 300 matched but only one hit returned, then the retry returns all of them.
        .thenReturn(searchResult(List.of(SNOW_ORDERS), 300))
        .thenReturn(searchResult(List.of(SNOW_ORDERS, SNOW_USERS), 2));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS), SNOW_USERS, List.of(DBT_USERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 1), key(DBT_USERS, 1)), _context, _entityClient, _viewService);

    // Two searches for the chunk, not one per key, and the retry's hits are what got attributed.
    verifySearchCount(2);
    assertEquals(results.get(0).getSearchResults().get(0).getEntity().getUrn(), SNOW_ORDERS);
    assertEquals(results.get(1).getSearchResults().get(0).getEntity().getUrn(), SNOW_USERS);
  }

  /** The retry is sized to the reported match count so the second page holds everything. */
  @Test
  public void testRetryRequestsTheFullMatchCount() throws Exception {
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
        .thenReturn(searchResult(List.of(SNOW_ORDERS), 300))
        .thenReturn(searchResult(List.of(SNOW_ORDERS), 1));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    SiblingsSearchBatchLoader.batchLoad(
        List.of(key(DBT_ORDERS, 1)), _context, _entityClient, _viewService);

    final ArgumentCaptor<Integer> sizes = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(_entityClient, Mockito.times(2))
        .scrollAcrossEntities(
            any(),
            any(),
            any(),
            nullable(Filter.class),
            nullable(String.class),
            nullable(String.class),
            any(),
            sizes.capture(),
            any());
    // First the floor for a single-urn chunk, then exactly the matched count.
    assertEquals(sizes.getAllValues(), List.of(100, 300));
  }

  /**
   * Beyond the ceiling a retry could not hold the match set either, so the chunk is redone per key
   * rather than reporting an undercount.
   */
  @Test
  public void testTruncationBeyondCeilingFallsBackToPerKeyQueries() throws Exception {
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
        // More matches than MAX_WINDOW can return, so no retry is attempted.
        .thenReturn(searchResult(List.of(SNOW_ORDERS), 501))
        .thenReturn(searchResult(List.of(SNOW_ORDERS)))
        .thenReturn(searchResult(List.of(SNOW_USERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS), SNOW_USERS, List.of(DBT_USERS)));

    SiblingsSearchBatchLoader.batchLoad(
        List.of(key(DBT_ORDERS, 1), key(DBT_USERS, 1)), _context, _entityClient, _viewService);

    // One chunk search that truncated, then one query per key.
    verifySearchCount(3);
  }

  /**
   * `count: 0` is a legitimate totals-only request. The empty page satisfies the requested size, so
   * it counts as full, but there is no last hit to take a cursor from.
   */
  @Test
  public void testZeroCountReturnsTotalWithoutHitsOrCursor() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS, SNOW_USERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS), SNOW_USERS, List.of(DBT_ORDERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 0)), _context, _entityClient, _viewService);

    assertEquals(results.get(0).getTotal(), 2);
    assertEquals(results.get(0).getCount(), 0);
    assertTrue(results.get(0).getSearchResults().isEmpty());
    // Matches what the unbatched path returns for count: 0.
    assertNull(results.get(0).getNextScrollId());
  }

  /**
   * The unbatched path treats a negative count as unbounded and returns every sibling, so the
   * batched path must too rather than letting Stream#limit reject it.
   */
  @Test
  public void testNegativeCountReturnsEverySiblingLikeTheUnbatchedPath() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS, SNOW_USERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS), SNOW_USERS, List.of(DBT_ORDERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, -1)), _context, _entityClient, _viewService);

    assertEquals(results.get(0).getTotal(), 2);
    assertEquals(results.get(0).getCount(), 2);
    assertNull(results.get(0).getNextScrollId());
  }

  /** A hit with no stamped cursor must not be reported as one, and must not fail the chunk. */
  @Test
  public void testFullPageWithoutStampedCursorReturnsNoCursor() throws Exception {
    stubSearch(searchResultWithoutCursors(List.of(SNOW_ORDERS, SNOW_USERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS), SNOW_USERS, List.of(DBT_ORDERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 2)), _context, _entityClient, _viewService);

    assertEquals(results.get(0).getTotal(), 2);
    assertEquals(results.get(0).getCount(), 2);
    assertNull(results.get(0).getNextScrollId());
  }

  /** A page size larger than the per-urn floor has to widen the window, or it cannot be filled. */
  @Test
  public void testLargePageWidensTheWindow() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    SiblingsSearchBatchLoader.batchLoad(
        List.of(key(DBT_ORDERS, 60)), _context, _entityClient, _viewService);

    final ArgumentCaptor<Integer> size = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            any(),
            any(),
            nullable(Filter.class),
            nullable(String.class),
            nullable(String.class),
            any(),
            size.capture(),
            any());
    // count 60 * headroom 3 beats both the per-urn floor and MIN_WINDOW.
    assertEquals(size.getValue().intValue(), 180);
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
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
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
    // Assert the values too, not just the field names: a combine that kept the field but dropped
    // its value would still satisfy a field-name check.
    final String rendered = call.filter().toString();
    assertTrue(rendered.contains("origin"), "view filter field missing: " + rendered);
    assertTrue(rendered.contains("PROD"), "view filter value missing: " + rendered);
    assertTrue(rendered.contains(SIBLINGS_FACET), "siblings filter field missing: " + rendered);
    assertTrue(rendered.contains(DBT_ORDERS), "siblings filter urn missing: " + rendered);
  }

  /** A dangling view urn resolves to null; the request must still run on its own types. */
  @Test
  public void testMissingViewFallsBackToRequestedTypes() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
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
  }

  /** A caller-supplied orFilters predicate must be ANDed with the siblings filter, not dropped. */
  @Test
  public void testCallerFilterIsCombinedWithSiblingsFilter() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    SiblingsSearchBatchLoader.batchLoad(
        List.of(
            keyWith(DBT_ORDERS, List.of("dataset"), filterOn("platform", "snowflake"), null, null)),
        _context,
        _entityClient,
        _viewService);

    final String rendered = captureSearch().filter().toString();
    assertTrue(rendered.contains("platform"), "caller filter field missing: " + rendered);
    assertTrue(rendered.contains("snowflake"), "caller filter value missing: " + rendered);
    assertTrue(rendered.contains(SIBLINGS_FACET), "siblings filter field missing: " + rendered);
    assertTrue(rendered.contains(DBT_ORDERS), "siblings filter urn missing: " + rendered);
  }

  /** Caller search flags are honoured, and the key's own instance is never mutated. */
  @Test
  public void testCallerSearchFlagsArePreservedAndKeyIsNotMutated() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
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
        .thenThrow(new RuntimeException("elasticsearch unavailable"));

    assertThrows(
        RuntimeException.class,
        () ->
            SiblingsSearchBatchLoader.batchLoad(
                List.of(key(DBT_ORDERS, 1)), _context, _entityClient, _viewService));
  }

  /** A hit may name siblings outside the chunk; only in-chunk urns may be credited. */
  @Test
  public void testSiblingsOutsideTheChunkAreNotAttributed() throws Exception {
    // The hit is a sibling of DBT_ORDERS (in chunk) and of a dataset outside the chunk.
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS, DBT_USERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 1)), _context, _entityClient, _viewService);

    assertEquals(results.get(0).getTotal(), 1);
  }

  /** A hit whose siblings aspect is absent cannot be attributed and must not be invented. */
  @Test
  public void testHitWithoutSiblingsAspectIsNotAttributed() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
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

  private static final String SNOW_ORDERS_B =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,analytics.orders,PROD)";
  private static final String SNOW_ORDERS_C =
      "urn:li:dataset:(urn:li:dataPlatform:redshift,analytics.orders,PROD)";

  /**
   * Regression: hits must come back in search-relevance order, not batchGetV2's map order.
   *
   * <p>batchGetV2 returns an unordered HashMap. Attributing hits by walking that map appends them
   * in hash order, so a dataset with several siblings resolves {@code searchResults[0]} to an
   * arbitrary one — and the lineage graph, schema tab and stats tab all read exactly that element.
   * The aspect stub here deliberately returns the responses reversed relative to the search order.
   */
  @Test
  public void testHitsKeepSearchOrderRegardlessOfAspectResponseOrder() throws Exception {
    final List<String> searchOrder = List.of(SNOW_ORDERS, SNOW_ORDERS_B, SNOW_ORDERS_C);
    stubSearch(searchResult(searchOrder));

    // Reversed on purpose: the loader must not inherit this ordering.
    final Map<Urn, EntityResponse> reversed = new LinkedHashMap<>();
    for (String hitUrn : List.of(SNOW_ORDERS_C, SNOW_ORDERS_B, SNOW_ORDERS)) {
      final UrnArray siblings = new UrnArray();
      siblings.add(UrnUtils.getUrn(DBT_ORDERS));
      reversed.put(
          UrnUtils.getUrn(hitUrn),
          new EntityResponse()
              .setUrn(UrnUtils.getUrn(hitUrn))
              .setAspects(
                  new EnvelopedAspectMap(
                      Map.of(
                          SIBLINGS_ASPECT_NAME,
                          new EnvelopedAspect()
                              .setValue(
                                  new Aspect(new Siblings().setSiblings(siblings).data()))))));
    }
    Mockito.when(_entityClient.batchGetV2(any(), any(), any(), any(), any())).thenReturn(reversed);

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 3)), _context, _entityClient, _viewService);

    final List<String> actual =
        results.get(0).getSearchResults().stream()
            .map(r -> r.getEntity().getUrn())
            .collect(java.util.stream.Collectors.toList());
    assertEquals(actual, searchOrder, "hits did not follow search-relevance order");
  }

  /**
   * No facet is requested: DataHub returns a filtered field's buckets at count 0, so they are
   * useless for per-key totals.
   */
  @Test
  public void testChunkSearchRequestsNoFacets() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    SiblingsSearchBatchLoader.batchLoad(
        List.of(key(DBT_ORDERS, 1)), _context, _entityClient, _viewService);

    final ArgumentCaptor<List> facets = ArgumentCaptor.forClass(List.class);
    Mockito.verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            any(),
            any(),
            nullable(Filter.class),
            nullable(String.class),
            nullable(String.class),
            any(),
            nullable(Integer.class),
            facets.capture());
    assertTrue(facets.getValue().isEmpty(), "chunk search must not request facets");
  }

  /**
   * A full page must carry the cursor of its last hit, so paging works the same as unbatched.
   *
   * <p>Each hit already carries a cursor built from its own sort values ([_score, urn]). Filter
   * context does not score, so that value is what the single-key query would have produced too.
   */
  @Test
  public void testFullPageReturnsTheLastHitsCursor() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS, SNOW_USERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS), SNOW_USERS, List.of(DBT_ORDERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 2)), _context, _entityClient, _viewService);

    // Two hits requested, two returned — a full page, so the last hit's cursor is the resume point.
    assertEquals(results.get(0).getCount(), 2);
    assertEquals(results.get(0).getNextScrollId(), "cursor-for-" + SNOW_USERS);
  }

  /** A short page means there is nothing after it, so no cursor — matching SearchRequestHandler. */
  @Test
  public void testShortPageReturnsNoCursor() throws Exception {
    stubSearch(searchResult(List.of(SNOW_ORDERS)));
    stubSiblingAspects(Map.of(SNOW_ORDERS, List.of(DBT_ORDERS)));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_ORDERS, 5)), _context, _entityClient, _viewService);

    assertEquals(results.get(0).getCount(), 1);
    assertNull(results.get(0).getNextScrollId(), "short page must not offer a resume point");
  }

  /** A key with no siblings has no page and no cursor. */
  @Test
  public void testEmptyResultHasNoCursor() throws Exception {
    stubSearch(searchResult(List.of()));

    final List<ScrollResults> results =
        SiblingsSearchBatchLoader.batchLoad(
            List.of(key(DBT_USERS, 1)), _context, _entityClient, _viewService);

    assertEquals(results.get(0).getCount(), 0);
    assertNull(results.get(0).getNextScrollId());
  }
}
