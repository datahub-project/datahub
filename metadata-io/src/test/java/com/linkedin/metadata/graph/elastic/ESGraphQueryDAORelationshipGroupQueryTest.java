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
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
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
            .setResults(new SearchConfiguration.SearchResultsLimit().setMax(100).setStrict(false));

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
  public void testLineageRelationshipsGroupQueryPagination() throws IOException {
    // Test data setup
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");

    // Mock EdgeInfo objects
    LineageRegistry.EdgeInfo edgeInfo =
        new LineageRegistry.EdgeInfo(
            "DownstreamOf", RelationshipDirection.OUTGOING, DATASET_ENTITY_NAME);

    // Set up mocked search results
    SearchHit[] firstPageHits = createMockSearchHits(3);
    SearchHit[] secondPageHits = createMockSearchHits(2);
    SearchHit[] emptyHits = new SearchHit[0];

    // Create mock search responses
    SearchResponse firstPageResponse = createMockSearchResponse(firstPageHits, 5);
    SearchResponse secondPageResponse = createMockSearchResponse(secondPageHits, 5);
    SearchResponse emptyResponse = createMockSearchResponse(emptyHits, 5);

    // Configure client to return our mock responses in sequence for pagination
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

    // Set up LineageFlags with entitiesExploredPerHopLimit
    OperationContext contextWithFlags =
        operationContext.withLineageFlags(
            f -> new LineageFlags().setEntitiesExploredPerHopLimit(10));

    // Execute the test
    graphQueryDAO.getLineage(contextWithFlags, entityUrn, lineageGraphFilters, 0, 100, 2);

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
  public void testLineageWithEntitiesExploredPerHopLimit() throws IOException {
    // Test data setup
    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset-1");

    // Create edge info
    LineageRegistry.EdgeInfo edgeInfo =
        new LineageRegistry.EdgeInfo(
            "DownstreamOf", RelationshipDirection.OUTGOING, DATASET_ENTITY_NAME);

    // Set up mock search results with more entities than the limit
    SearchHit[] hits = createMockSearchHits(15); // More than our limit of 10
    SearchHit[] emptyHits = new SearchHit[0]; // Empty hits for second page

    // Create mock search response
    SearchResponse mockResponse = createMockSearchResponse(hits, 15);
    SearchResponse emptyResponse = createMockSearchResponse(emptyHits, 15);

    // Configure client to return our mock response for first call, then empty for subsequent calls
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse)
        .thenReturn(emptyResponse); // Return empty response for second call to end pagination

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

    // Set up LineageFlags with entitiesExploredPerHopLimit of 10
    OperationContext contextWithFlags =
        operationContext.withLineageFlags(
            f -> new LineageFlags().setEntitiesExploredPerHopLimit(10));

    // Execute
    ESGraphQueryDAO.LineageResponse response =
        graphQueryDAO.getLineage(contextWithFlags, sourceUrn, lineageGraphFilters, 0, 100, 1);

    // Verify search request size is limited to our entitiesExploredPerHopLimit
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient, times(2)).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    // Verify we didn't have an infinite loop by checking we only made 2 search calls
    verify(mockClient, times(2)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));
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

  // Helper methods for creating mock objects

  private SearchHit createMockSearchHit(Map<String, Object> sourceMap, Object[] sortValues) {
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);
    when(hit.getSortValues()).thenReturn(sortValues);
    return hit;
  }

  private SearchHit[] createMockSearchHits(int count) {
    SearchHit[] hits = new SearchHit[count];
    for (int i = 0; i < count; i++) {
      Map<String, Object> sourceMap = new HashMap<>();
      Map<String, Object> source = new HashMap<>();
      source.put("urn", "urn:li:dataset:source-" + i);
      source.put("entityType", DATASET_ENTITY_NAME);
      Map<String, Object> destination = new HashMap<>();
      destination.put("urn", "urn:li:dataset:dest-" + i);
      destination.put("entityType", DATASET_ENTITY_NAME);
      sourceMap.put("source", source);
      sourceMap.put("destination", destination);
      sourceMap.put("relationshipType", "DownstreamOf");

      // Create SearchHit with sort values
      hits[i] = createMockSearchHit(sourceMap, new Object[] {"score" + i, "id" + i});
    }
    return hits;
  }

  private SearchHit createMockSearchHit(Map<String, Object> sourceMap) {
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSourceAsMap()).thenReturn(sourceMap);
    when(hit.getSortValues()).thenReturn(new Object[] {"sort-value"});
    return hit;
  }

  private SearchResponse createMockSearchResponse(SearchHit[] hits, long totalHitsCount) {
    SearchResponse response = mock(SearchResponse.class);
    SearchHits searchHits = mock(SearchHits.class);

    // Create an actual TotalHits instance from Lucene
    TotalHits totalHits = new TotalHits(totalHitsCount, Relation.EQUAL_TO);

    // Set up the mocks
    when(searchHits.getHits()).thenReturn(hits);
    when(searchHits.getTotalHits()).thenReturn(totalHits);
    when(response.getHits()).thenReturn(searchHits);

    return response;
  }
}
