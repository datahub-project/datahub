package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.Constants.*;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_CONFIG;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESGraphQueryDAORelationshipGroupQueryTest {
  private final int GLOBAL_RESULT_LIMIT = 25;
  private final OperationContext operationContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private RestHighLevelClient mockClient;
  private ESGraphQueryDAO graphQueryDAO;

  @BeforeMethod
  public void setup() {
    // Initialize mocks
    mockClient = mock(RestHighLevelClient.class);

    // Create configuration with timeout and batch settings
    GraphQueryConfiguration graphConfig =
        new GraphQueryConfiguration()
            .setTimeoutSeconds(10)
            .setBatchSize(25)
            .setEnableMultiPathSearch(true)
            .setBoostViaNodes(true);

    SearchConfiguration.SearchLimitConfig limitConfig =
        new SearchConfiguration.SearchLimitConfig()
            .setResults(
                new SearchConfiguration.SearchResultsLimit()
                    .setMax(GLOBAL_RESULT_LIMIT)
                    .setStrict(false));

    ElasticSearchConfiguration testESConfig =
        TEST_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(graphConfig)
                    .limit(limitConfig)
                    .build())
            .build();

    // Create the DAO with mocks
    graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            testESConfig);
  }

  @Test
  public void testLineageWithNoResults() throws IOException {
    // Test data setup
    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");

    // Create edge info
    LineageRegistry.EdgeInfo edgeInfo =
        new LineageRegistry.EdgeInfo(
            "DownstreamOf", RelationshipDirection.OUTGOING, DATASET_ENTITY_NAME);

    // Set up mock search results with no hits
    SearchHit[] emptyHits = new SearchHit[0];
    SearchResponse emptyResponse = createMockSearchResponse(emptyHits, 0);

    // Configure client to return empty response
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(firstPageResponse)
        .thenReturn(secondPageResponse)
        .thenReturn(emptyResponse);

    // Configure LineageRegistry mock
    Map<String, Set<LineageRegistry.EdgeInfo>> edgeMap = new HashMap<>();
    edgeMap.put(DATASET_ENTITY_NAME, ImmutableSet.of(edgeInfo));

    // Create LineageGraphFilters with mock edge info
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>(edgeMap));

    // Execute
    ESGraphQueryDAO.LineageResponse response =
        graphQueryDAO.getLineage(operationContext, sourceUrn, lineageGraphFilters, 0, 100, 1);

    // Verify empty result
    Assert.assertEquals(response.getLineageRelationships().size(), 0);
    Assert.assertEquals(response.getTotal(), 0);
  }

  @Test
  public void testLineageWithInvalidViaEntity() throws IOException {
    // Test data setup
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");

    // Create edge info
    LineageRegistry.EdgeInfo edgeInfo =
        new LineageRegistry.EdgeInfo(
            "DownstreamOf", RelationshipDirection.OUTGOING, DATASET_ENTITY_NAME);

    // Configure mocked hit with an invalid via entity
    SearchHit[] hits = new SearchHit[1];

    // Hit with invalid via entity
    Map<String, Object> hitSource = new HashMap<>();
    Map<String, Object> hitSource_source = new HashMap<>();
    hitSource_source.put("urn", "urn:li:dataset:test-dataset-1");
    hitSource_source.put("entityType", DATASET_ENTITY_NAME);
    Map<String, Object> hitDestination = new HashMap<>();
    hitDestination.put("urn", "urn:li:dataset:test-dataset-2");
    hitDestination.put("entityType", DATASET_ENTITY_NAME);
    hitSource.put("source", hitSource_source);
    hitSource.put("destination", hitDestination);
    hitSource.put("relationshipType", "DownstreamOf");
    hitSource.put("via", "invalid-urn-format"); // Invalid URN format

    hits[0] = createMockSearchHit(hitSource, false);

    // Create empty hits for second page
    SearchHit[] emptyHits = new SearchHit[0];

    // Use doAnswer to provide more control over mocking
    doAnswer(
            invocation -> {
              SearchRequest request = invocation.getArgument(0);
              SearchSourceBuilder sourceBuilder = request.source();

              // First call returns hits with invalid via entity
              if (sourceBuilder.searchAfter() == null) {
                return createMockSearchResponse(hits, 1);
              }
              // Subsequent calls return empty hits
              return createMockSearchResponse(emptyHits, 0);
            })
        .when(mockClient)
        .search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    // Configure LineageRegistry mock
    Map<String, Set<LineageRegistry.EdgeInfo>> edgeMap = new HashMap<>();
    edgeMap.put(DATASET_ENTITY_NAME, ImmutableSet.of(edgeInfo));

    // Create LineageGraphFilters with mock edge info
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>(edgeMap));

    // Execute
    ESGraphQueryDAO.LineageResponse response =
        graphQueryDAO.getLineage(
            operationContext.withLineageFlags(
                f -> new LineageFlags().setEntitiesExploredPerHopLimit(10)),
            entityUrn,
            lineageGraphFilters,
            0,
            100,
            1);

    // Verify that the relationship is still processed despite invalid via entity
    Assert.assertEquals(response.getLineageRelationships().size(), 1);
    Assert.assertEquals(
        response.getLineageRelationships().get(0).getEntity().toString(),
        "urn:li:dataset:test-dataset-2");

    // Verify that search was called twice (for pagination)
    verify(mockClient, times(1)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testLineageWithMissingMetadata() throws IOException {
    // Test data setup
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");

    // Create edge info
    LineageRegistry.EdgeInfo edgeInfo =
        new LineageRegistry.EdgeInfo(
            "DownstreamOf", RelationshipDirection.OUTGOING, DATASET_ENTITY_NAME);

    // Configure mocked hit with minimal/missing metadata
    SearchHit[] hits = new SearchHit[1];

    // Hit with minimal/missing metadata
    Map<String, Object> hitSource = new HashMap<>();
    Map<String, Object> hitSource_source = new HashMap<>();
    hitSource_source.put("urn", "urn:li:dataset:test-dataset-1");
    Map<String, Object> hitDestination = new HashMap<>();
    hitDestination.put("urn", "urn:li:dataset:test-dataset-2");
    hitSource.put("source", hitSource_source);
    hitSource.put("destination", hitDestination);
    // Intentionally omitting some fields to test null handling
    hitSource.put("relationshipType", "DownstreamOf");

    hits[0] = createMockSearchHit(hitSource, false);

    // Create empty hits for second page
    SearchHit[] emptyHits = new SearchHit[0];

    // Use doAnswer to provide more control over mocking
    doAnswer(
            invocation -> {
              SearchRequest request = invocation.getArgument(0);
              SearchSourceBuilder sourceBuilder = request.source();

              // First call returns hits with minimal metadata
              if (sourceBuilder.searchAfter() == null) {
                return createMockSearchResponse(hits, 1);
              }

              // Subsequent calls return empty hits
              return createMockSearchResponse(emptyHits, 0);
            })
        .when(mockClient)
        .search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    // Configure LineageRegistry mock
    Map<String, Set<LineageRegistry.EdgeInfo>> edgeMap = new HashMap<>();
    edgeMap.put(DATASET_ENTITY_NAME, ImmutableSet.of(edgeInfo));

    // Create LineageGraphFilters with mock edge info
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>(edgeMap));

    // Execute with hop limit context
    ESGraphQueryDAO.LineageResponse result =
        graphQueryDAO.getLineage(
            operationContext.withLineageFlags(
                f -> new LineageFlags().setEntitiesExploredPerHopLimit(10)),
            entityUrn,
            lineageGraphFilters,
            0,
            100,
            1);

    // Verify search was called
    verify(mockClient, atLeast(1)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    // Verify that the relationship can be processed even with minimal metadata
    Assert.assertEquals(result.getLineageRelationships().size(), 1);
    Assert.assertEquals(
        result.getLineageRelationships().get(0).getEntity().toString(),
        "urn:li:dataset:test-dataset-2");

    // Additional verification of minimal metadata handling
    LineageRelationship relationship = result.getLineageRelationships().get(0);
    Assert.assertEquals(relationship.getType(), "DownstreamOf");
    Assert.assertNull(relationship.getCreatedOn());
    Assert.assertNull(relationship.getCreatedActor());
  }

  @Test
  public void testLineageWithMaxHopsLimit() throws IOException {
    // Test data setup
    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");

    // Create hits representing multiple hops
    SearchHit[] firstHopHits = new SearchHit[2];
    firstHopHits[0] =
        createMockSearchHit(
            createHitSourceMap(
                "urn:li:dataset:test-dataset-2", "urn:li:dataset:test-dataset-1", "DownstreamOf"),
            false);
    firstHopHits[1] =
        createMockSearchHit(
            createHitSourceMap(
                "urn:li:dataset:test-dataset-3", "urn:li:dataset:test-dataset-1", "DownstreamOf"),
            false);

    // Create hits for second hop
    SearchHit[] secondHopHits = new SearchHit[2];
    secondHopHits[0] =
        createMockSearchHit(
            createHitSourceMap(
                "urn:li:dataset:test-dataset-4", "urn:li:dataset:test-dataset-2", "DownstreamOf"),
            false);
    secondHopHits[1] =
        createMockSearchHit(
            createHitSourceMap(
                "urn:li:dataset:test-dataset-5", "urn:li:dataset:test-dataset-3", "DownstreamOf"),
            false);

    // Create empty hits for final page
    SearchHit[] emptyHits = new SearchHit[0];

    // Simplified mocking with explicit control
    AtomicInteger searchCallCount = new AtomicInteger(0);

    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            invocation -> {
              SearchRequest request = invocation.getArgument(0);
              SearchSourceBuilder sourceBuilder = request.source();
              int callCount = searchCallCount.incrementAndGet();

              switch (callCount) {
                case 1:
                  // First call - first hop hits
                  return createMockSearchResponseWithControlledHits(firstHopHits, 2);
                case 2:
                  // Second call - second hop hits
                  return createMockSearchResponseWithControlledHits(secondHopHits, 2);
                default:
                  // Subsequent calls - empty hits to end pagination
                  return createMockSearchResponseWithControlledHits(emptyHits, 2);
              }
            });

    // Create LineageGraphFilters with mock edge info
    LineageGraphFilters lineageGraphFilters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(),
            DATASET_ENTITY_NAME,
            LineageDirection.DOWNSTREAM);

    // Configure a custom operation context with extended timeout and hop limit
    OperationContext customContext =
        operationContext.withLineageFlags(
            lineageFlags -> lineageFlags.setEntitiesExploredPerHopLimit(10));

    // Execute with max 2 hops
    ESGraphQueryDAO.LineageResponse response =
        graphQueryDAO.getLineage(customContext, sourceUrn, lineageGraphFilters, 0, 100, 2);

    // Verify search was called multiple times
    verify(mockClient, atLeast(2)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    // Verify relationships from first two hops
    Assert.assertTrue(
        response.getLineageRelationships().size() > 0,
        "Should have relationships from multiple hops");

    // Verify hop depths
    for (LineageRelationship relationship : response.getLineageRelationships()) {
      Assert.assertTrue(
          relationship.getDegree() <= 2, "Relationship degree should not exceed max hops");
    }

    // Additional verification of relationships
    Set<String> expectedDestinations =
        Set.of(
            "urn:li:dataset:test-dataset-2",
            "urn:li:dataset:test-dataset-3",
            "urn:li:dataset:test-dataset-4",
            "urn:li:dataset:test-dataset-5");
    Set<String> actualDestinations =
        response.getLineageRelationships().stream()
            .map(r -> r.getEntity().toString())
            .collect(Collectors.toSet());

    Assert.assertTrue(
        actualDestinations.containsAll(expectedDestinations),
        "Should contain expected destination URNs");
  }

  // Existing method for creating mock search response
  private SearchResponse createMockSearchResponseWithControlledHits(
      SearchHit[] hits, long totalHitsCount) {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits searchHits = mock(SearchHits.class);

    // Create an actual TotalHits instance from Lucene
    TotalHits totalHits = new TotalHits(totalHitsCount, TotalHits.Relation.EQUAL_TO);

    // Explicitly stub the methods that might cause issues
    when(searchHits.getHits()).thenReturn(hits);
    when(searchHits.getTotalHits()).thenReturn(totalHits);

    // Configure the SearchResponse mock
    when(response.getHits()).thenReturn(searchHits);

    return response;
  }

  @Test
  public void testLineageRelationshipsGroupQueryPagination() throws IOException {
    // Test data setup
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");

    // Set up mocked search results
    SearchHit[] firstPageHits = createMockSearchHits(GLOBAL_RESULT_LIMIT, true);
    SearchHit[] secondPageHits = createMockSearchHits(2, false);
    SearchHit[] emptyHits = new SearchHit[0];

    // Create mock search responses
    SearchResponse firstPageResponse =
        createMockSearchResponse(firstPageHits, GLOBAL_RESULT_LIMIT + 2);
    SearchResponse secondPageResponse =
        createMockSearchResponse(secondPageHits, GLOBAL_RESULT_LIMIT + 2);
    SearchResponse emptyResponse = createMockSearchResponse(emptyHits, GLOBAL_RESULT_LIMIT + 2);

    // Configure client to return our mock responses in sequence for pagination
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(firstPageResponse)
        .thenReturn(secondPageResponse)
        .thenReturn(emptyResponse);

    // Create LineageGraphFilters with mock edge info
    LineageGraphFilters lineageGraphFilters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(),
            DATASET_ENTITY_NAME,
            LineageDirection.DOWNSTREAM);

    // Set up LineageFlags with entitiesExploredPerHopLimit
    OperationContext contextWithFlags =
        operationContext.withLineageFlags(
            f -> new LineageFlags().setEntitiesExploredPerHopLimit(GLOBAL_RESULT_LIMIT + 2));

    // Execute the test
    graphQueryDAO.getLineage(contextWithFlags, entityUrn, lineageGraphFilters, 0, 5, 2);

    // Verify search was called multiple times (pagination working)
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient, atLeast(2)).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    // Capture all search requests to verify pagination
    List<SearchRequest> requests = requestCaptor.getAllValues();

    // First request should have no search_after parameter
    SearchSourceBuilder firstSourceBuilder = requests.get(0).source();
    Assert.assertNull(firstSourceBuilder.searchAfter());

    // Second request should include search_after parameter
    SearchSourceBuilder secondSourceBuilder = requests.get(1).source();
    Assert.assertNotNull(secondSourceBuilder.searchAfter());

    // Verify the correct method was called to build the query
    Assert.assertTrue(requests.size() >= 2, "Expected at least 2 search requests for pagination");
  }

  @Test
  public void testExploreMultiplePathsProcessing() throws IOException {
    // Test data setup
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");

    // Create edge info
    LineageRegistry.EdgeInfo edgeInfo =
        new LineageRegistry.EdgeInfo(
            "DownstreamOf", RelationshipDirection.OUTGOING, DATASET_ENTITY_NAME);

    // Configure mocked hits with duplicate entities to test exploreMultiplePaths
    SearchHit[] hits = new SearchHit[3];

    // First hit (edge from test-dataset-1 to test-dataset-2)
    Map<String, Object> firstHitSource = new HashMap<>();
    Map<String, Object> firstHitSource_source = new HashMap<>();
    firstHitSource_source.put("urn", "urn:li:dataset:test-dataset-1");
    firstHitSource_source.put("entityType", DATASET_ENTITY_NAME);
    Map<String, Object> firstHitDestination = new HashMap<>();
    firstHitDestination.put("urn", "urn:li:dataset:test-dataset-2");
    firstHitDestination.put("entityType", DATASET_ENTITY_NAME);
    firstHitSource.put("source", firstHitSource_source);
    firstHitSource.put("destination", firstHitDestination);
    firstHitSource.put("relationshipType", "DownstreamOf");

    hits[0] = createMockSearchHit(firstHitSource, new Object[] {"score1", "id1"});

    // Second hit (another path to same destination from test-dataset-1)
    Map<String, Object> secondHitSource = new HashMap<>();
    Map<String, Object> secondHitSource_source = new HashMap<>();
    secondHitSource_source.put("urn", "urn:li:dataset:test-dataset-1");
    secondHitSource_source.put("entityType", DATASET_ENTITY_NAME);
    Map<String, Object> secondHitDestination = new HashMap<>();
    secondHitDestination.put("urn", "urn:li:dataset:test-dataset-2"); // Same destination
    secondHitDestination.put("entityType", DATASET_ENTITY_NAME);
    secondHitSource.put("source", secondHitSource_source);
    secondHitSource.put("destination", secondHitDestination);
    secondHitSource.put("relationshipType", "DownstreamOf");
    secondHitSource.put("via", "urn:li:dataJob:intermediate-job"); // Different via path

    hits[1] = createMockSearchHit(secondHitSource, new Object[] {"score2", "id2"});

    // Third hit (a different destination)
    Map<String, Object> thirdHitSource = new HashMap<>();
    Map<String, Object> thirdHitSource_source = new HashMap<>();
    thirdHitSource_source.put("urn", "urn:li:dataset:test-dataset-1");
    thirdHitSource_source.put("entityType", DATASET_ENTITY_NAME);
    Map<String, Object> thirdHitDestination = new HashMap<>();
    thirdHitDestination.put("urn", "urn:li:dataset:test-dataset-3");
    thirdHitDestination.put("entityType", DATASET_ENTITY_NAME);
    thirdHitSource.put("source", thirdHitSource_source);
    thirdHitSource.put("destination", thirdHitDestination);
    thirdHitSource.put("relationshipType", "DownstreamOf");

    hits[2] = createMockSearchHit(thirdHitSource, new Object[] {"score3", "id3"});

    // Create empty hits for second page
    SearchHit[] emptyHits = new SearchHit[0];

    // Configure client to properly handle search_after pagination
    doAnswer(
            new Answer<SearchResponse>() {
              @Override
              public SearchResponse answer(InvocationOnMock invocation) throws Throwable {
                SearchRequest request = invocation.getArgument(0);
                SearchSourceBuilder source = request.source();

                // Check if this is a request with search_after
                if (source.searchAfter() != null) {
                  // If we have a search_after, return empty results to end pagination
                  return createMockSearchResponse(emptyHits, 3);
                } else {
                  // First request, return our hits
                  return createMockSearchResponse(hits, 3);
                }
              }
            })
        .when(mockClient)
        .search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    // Configure LineageRegistry mock
    Map<String, Set<LineageRegistry.EdgeInfo>> edgeMap = new HashMap<>();
    edgeMap.put(DATASET_ENTITY_NAME, ImmutableSet.of(edgeInfo));

    // Create LineageGraphFilters with mock edge info
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>(edgeMap));

    // Create context with flags to force using relationshipsGroupQuery path
    OperationContext contextWithFlags =
        operationContext.withLineageFlags(
            f -> new LineageFlags().setEntitiesExploredPerHopLimit(10));

    // Test with exploreMultiplePaths = true
    GraphQueryConfiguration graphConfig =
        new GraphQueryConfiguration()
            .setTimeoutSeconds(10)
            .setBatchSize(25)
            .setEnableMultiPathSearch(true); // Enable multiple paths

    ElasticSearchConfiguration testESConfig =
        TEST_SEARCH_CONFIG.toBuilder()
            .search(TEST_SEARCH_CONFIG.getSearch().toBuilder().graph(graphConfig).build())
            .build();

    ESGraphQueryDAO daoWithMultiPath =
        new ESGraphQueryDAO(
            mockClient,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            testESConfig);

    // Call the public method directly with exploreMultiplePaths = true
    ESGraphQueryDAO.LineageResponse resultWithMultiPaths =
        daoWithMultiPath.getLineage(
            contextWithFlags, // Use context with flags
            entityUrn,
            lineageGraphFilters,
            0,
            100,
            1);

    // We should have relationships for both destinations plus a via entity
    Assert.assertEquals(resultWithMultiPaths.getLineageRelationships().size(), 3);

    // Test with exploreMultiplePaths = false
    GraphQueryConfiguration singlePathConfig =
        new GraphQueryConfiguration()
            .setTimeoutSeconds(10)
            .setBatchSize(25)
            .setEnableMultiPathSearch(false); // Disable multiple paths

    ElasticSearchConfiguration testSinglePathConfig =
        TEST_SEARCH_CONFIG.toBuilder()
            .search(TEST_SEARCH_CONFIG.getSearch().toBuilder().graph(singlePathConfig).build())
            .build();

    ESGraphQueryDAO daoWithSinglePath =
        new ESGraphQueryDAO(
            mockClient,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            testSinglePathConfig);

    // Reset the mock and reconfigure it
    reset(mockClient);

    // Configure client again for the second test
    doAnswer(
            new Answer<SearchResponse>() {
              @Override
              public SearchResponse answer(InvocationOnMock invocation) throws Throwable {
                SearchRequest request = invocation.getArgument(0);
                SearchSourceBuilder source = request.source();

                // Check if this is a request with search_after
                if (source.searchAfter() != null) {
                  // If we have a search_after, return empty results to end pagination
                  return createMockSearchResponse(emptyHits, 3);
                } else {
                  // First request, return our hits
                  return createMockSearchResponse(hits, 3);
                }
              }
            })
        .when(mockClient)
        .search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    // Call the public method directly with exploreMultiplePaths = false
    ESGraphQueryDAO.LineageResponse resultWithSinglePath =
        daoWithSinglePath.getLineage(
            contextWithFlags, // Use context with flags
            entityUrn,
            lineageGraphFilters,
            0,
            100,
            1);

    // With single path exploration, we should have fewer relationships
    // (Since we don't re-process already visited entities)
    Assert.assertTrue(
        resultWithSinglePath.getLineageRelationships().size()
            <= resultWithMultiPaths.getLineageRelationships().size());
  }

  @Test
  public void testViaEntityProcessing() throws IOException {
    // Test data setup
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");

    // Create edge info
    LineageRegistry.EdgeInfo edgeInfo =
        new LineageRegistry.EdgeInfo(
            "DownstreamOf", RelationshipDirection.OUTGOING, DATASET_ENTITY_NAME);

    // Configure mocked hit with a via entity
    SearchHit[] hits = new SearchHit[1];

    // Hit with via entity
    Map<String, Object> hitSource = new HashMap<>();
    Map<String, Object> hitSource_source = new HashMap<>();
    hitSource_source.put("urn", "urn:li:dataset:test-dataset-1");
    hitSource_source.put("entityType", DATASET_ENTITY_NAME);
    Map<String, Object> hitDestination = new HashMap<>();
    hitDestination.put("urn", "urn:li:dataset:test-dataset-2");
    hitDestination.put("entityType", DATASET_ENTITY_NAME);
    hitSource.put("source", hitSource_source);
    hitSource.put("destination", hitDestination);
    hitSource.put("relationshipType", "DownstreamOf");
    hitSource.put("via", "urn:li:dataJob:via-job");

    hits[0] = createMockSearchHit(hitSource, new Object[] {"score1", "id1"});

    // Create empty hits for second page
    SearchHit[] emptyHits = new SearchHit[0];

    // Configure client to properly handle search_after pagination
    doAnswer(
            new Answer<SearchResponse>() {
              @Override
              public SearchResponse answer(InvocationOnMock invocation) throws Throwable {
                SearchRequest request = invocation.getArgument(0);
                SearchSourceBuilder source = request.source();

                // Check if this is a request with search_after
                if (source.searchAfter() != null) {
                  // If we have a search_after, return empty results to end pagination
                  return createMockSearchResponse(emptyHits, 1);
                } else {
                  // First request, return our hits
                  return createMockSearchResponse(hits, 1);
                }
              }
            })
        .when(mockClient)
        .search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    // Configure LineageRegistry mock
    Map<String, Set<LineageRegistry.EdgeInfo>> edgeMap = new HashMap<>();
    edgeMap.put(DATASET_ENTITY_NAME, ImmutableSet.of(edgeInfo));

    // Create LineageGraphFilters with mock edge info
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>(edgeMap));

    // Create context with flags to force using relationshipsGroupQuery path
    OperationContext contextWithFlags =
        operationContext.withLineageFlags(
            f -> new LineageFlags().setEntitiesExploredPerHopLimit(10));

    // Call the public method directly
    ESGraphQueryDAO.LineageResponse result =
        graphQueryDAO.getLineage(
            contextWithFlags, // Use context with flags
            entityUrn,
            lineageGraphFilters,
            0,
            100,
            1);

    // We should have relationships for the destination and the via entity
    Assert.assertEquals(result.getLineageRelationships().size(), 2);

    // Verify we have a relationship for the via entity
    boolean foundViaEntity = false;
    for (LineageRelationship relationship : result.getLineageRelationships()) {
      if (relationship.getEntity().toString().equals("urn:li:dataJob:via-job")) {
        foundViaEntity = true;
        break;
      }
    }

    Assert.assertTrue(foundViaEntity, "Via entity relationship not found");
  }

  @Test
  public void testLineageQueryWithTimeWindowFilters() throws IOException {
    // Test scenario with time window filters
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");

    // Create hits
    SearchHit[] hits = createMockSearchHits(3, false);
    SearchHit[] emptyHits = new SearchHit[0];

    // Configure client to return our mock responses
    doAnswer(
            new Answer<SearchResponse>() {
              @Override
              public SearchResponse answer(InvocationOnMock invocation) throws Throwable {
                SearchRequest request = invocation.getArgument(0);
                SearchSourceBuilder source = request.source();

                // Check if this is a request with search_after
                if (source.searchAfter() != null) {
                  // If we have a search_after, return empty results to end pagination
                  return createMockSearchResponse(emptyHits, 3);
                } else {
                  // First request, return our hits
                  return createMockSearchResponse(hits, 3);
                }
              }
            })
        .when(mockClient)
        .search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    // Create LineageGraphFilters with mock edge info
    LineageGraphFilters lineageGraphFilters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(),
            DATASET_ENTITY_NAME,
            LineageDirection.DOWNSTREAM);

    // Create context with time window filters
    OperationContext contextWithTimeFilters =
        operationContext.withLineageFlags(
            f ->
                new LineageFlags()
                    .setEntitiesExploredPerHopLimit(10)
                    .setStartTimeMillis(System.currentTimeMillis() - 86400000L) // 24 hours ago
                    .setEndTimeMillis(System.currentTimeMillis()));

    // Call the public method directly
    ESGraphQueryDAO.LineageResponse result =
        graphQueryDAO.getLineage(contextWithTimeFilters, entityUrn, lineageGraphFilters, 0, 100, 1);

    // Verify we got results
    Assert.assertFalse(result.getLineageRelationships().isEmpty());
  }

  // Helper methods for creating mock objects

  private SearchHit createMockSearchHit(Map<String, Object> sourceMap, Object[] sortValues) {
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);
    when(hit.getSortValues()).thenReturn(sortValues);
    return hit;
  }

  private SearchHit[] createMockSearchHits(
      int count, Urn inputUrn, LineageDirection direction, boolean hasNext) {
    SearchHit[] hits = new SearchHit[count];
    for (int i = 0; i < count; i++) {
      Map<String, Object> sourceMap = new HashMap<>();
      Map<String, Object> source = new HashMap<>();
      source.put("urn", "urn:li:dataset:source-" + i);
      source.put("entityType", DATASET_ENTITY_NAME);
      Map<String, Object> destination = new HashMap<>();

      // Determine source and destination based on direction
      if (direction == LineageDirection.UPSTREAM) {
        // Outgoing edges: input URN is the source
        source.put("urn", inputUrn.toString());
        source.put("entityType", inputUrn.getEntityType());

        destination.put("urn", "urn:li:dataset:dest-" + i);
        destination.put("entityType", DATASET_ENTITY_NAME);
      } else {
        // Incoming edges: input URN is the destination
        source.put("urn", "urn:li:dataset:source-" + i);
        source.put("entityType", DATASET_ENTITY_NAME);

        destination.put("urn", inputUrn.toString());
        destination.put("entityType", inputUrn.getEntityType());
      }
      sourceMap.put("source", source);
      sourceMap.put("destination", destination);
      sourceMap.put("relationshipType", "DownstreamOf");

      // Create via field to match sort order
      sourceMap.put("via", i % 2 == 0 ? null : "urn:li:dataJob:via-job-" + i);

      // Add created and updated timestamps
      long baseTimestamp = System.currentTimeMillis();
      sourceMap.put("createdOn", baseTimestamp - (i * 1000L));
      sourceMap.put("updatedOn", baseTimestamp - (i * 500L));

      // Create sort values matching the order in createSearchAfterRequest
      Object[] sortValues =
          new Object[] {
            // First sort by via (ASC, missing last)
            sourceMap.get("via") != null ? sourceMap.get("via") : "_last",
            // Then by updatedOn (DESC, missing first)
            sourceMap.containsKey("updatedOn") ? sourceMap.get("updatedOn") : "_first",
            // Then by createdOn (DESC, missing first)
            sourceMap.containsKey("createdOn") ? sourceMap.get("createdOn") : "_first"
          };

      // Create SearchHit with sort values that match the search_after pagination
      hits[i] = createMockSearchHit(sourceMap, hasNext ? sortValues : null);
    }
    return hits;
  }

  private SearchHit[] createMockSearchHits(int count, boolean hasNext) {
    // Default to a sample input URN and downstream direction for backward compatibility
    Urn defaultUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");
    return createMockSearchHits(count, defaultUrn, LineageDirection.DOWNSTREAM, hasNext);
  }

  private SearchHit createMockSearchHit(Map<String, Object> sourceMap, boolean hasNext) {
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);

    if (hasNext) {
      // Create sort values matching the order in createSearchAfterRequest
      Object[] sortValues =
          new Object[] {
            // First sort by via (ASC, missing last)
            sourceMap.containsKey("via") && sourceMap.get("via") != null
                ? sourceMap.get("via")
                : "_last",

            // Then by updatedOn (DESC, missing first)
            sourceMap.containsKey("updatedOn") ? sourceMap.get("updatedOn") : "_first",

            // Then by createdOn (DESC, missing first)
            sourceMap.containsKey("createdOn") ? sourceMap.get("createdOn") : "_first"
          };

      when(hit.getSortValues()).thenReturn(sortValues);
    }
    return hit;
  }

  private SearchResponse createMockSearchResponse(SearchHit[] hits, long totalHitsCount) {
    SearchResponse response = mock(SearchResponse.class);

    // Create a real SearchHits mock with proper behavior
    SearchHits searchHits = mock(SearchHits.class);

    // Create an actual TotalHits instance from Lucene
    TotalHits totalHits = new TotalHits(totalHitsCount, TotalHits.Relation.EQUAL_TO);

    // Configure the SearchHits mock with realistic behavior
    when(searchHits.getHits()).thenReturn(hits);
    when(searchHits.getTotalHits()).thenReturn(totalHits);

    // Configure the SearchResponse mock
    when(response.getHits()).thenReturn(searchHits);

    return response;
  }

  private Map<String, Object> createHitSourceMap(
      String sourceUrn, String destUrn, String relationshipType) {
    Map<String, Object> hitSource = new HashMap<>();
    Map<String, Object> hitSource_source = new HashMap<>();
    hitSource_source.put("urn", sourceUrn);
    hitSource_source.put("entityType", DATASET_ENTITY_NAME);
    Map<String, Object> hitDestination = new HashMap<>();
    hitDestination.put("urn", destUrn);
    hitDestination.put("entityType", DATASET_ENTITY_NAME);
    hitSource.put("source", hitSource_source);
    hitSource.put("destination", hitDestination);
    hitSource.put("relationshipType", relationshipType);
    return hitSource;
  }
}
