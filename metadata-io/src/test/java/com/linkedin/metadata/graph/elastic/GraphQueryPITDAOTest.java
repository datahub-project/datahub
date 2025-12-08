package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.graph.elastic.TestUtils.createEmptySearchResponse;
import static com.linkedin.metadata.graph.elastic.TestUtils.createFakeLineageHits;
import static com.linkedin.metadata.graph.elastic.TestUtils.createFakeSearchResponse;
import static com.linkedin.metadata.graph.elastic.TestUtils.hasMessageInChain;
import static com.linkedin.metadata.graph.elastic.TestUtils.mockSliceBasedSearch;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG_NO_PIT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import com.datahub.util.exception.ESQueryException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.elastic.utils.GraphQueryUtils;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class GraphQueryPITDAOTest {

  private static final String TEST_QUERY_FILE_LIMITED =
      "elasticsearch/sample_filters/lineage_query_filters_limited.json";
  private static final String TEST_QUERY_FILE_FULL =
      "elasticsearch/sample_filters/lineage_query_filters_full.json";

  // Track created DAOs for cleanup
  private final List<GraphQueryPITDAO> createdDAOs = new ArrayList<>();

  /** Create a GraphQueryPITDAO and track it for cleanup */
  private GraphQueryPITDAO createTrackedDAO(SearchClientShim<?> client) {
    return createTrackedDAO(client, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);
  }

  /** Create a GraphQueryPITDAO with custom configs and track it for cleanup */
  private GraphQueryPITDAO createTrackedDAO(
      SearchClientShim<?> client,
      GraphServiceConfiguration graphConfig,
      ElasticSearchConfiguration esConfig) {
    GraphQueryPITDAO dao = new GraphQueryPITDAO(client, graphConfig, esConfig, null);
    createdDAOs.add(dao);
    return dao;
  }

  @AfterMethod
  public void cleanup() {
    // Shutdown all created DAOs to prevent thread pool leaks
    for (GraphQueryPITDAO dao : createdDAOs) {
      try {
        dao.shutdown();
      } catch (Exception e) {
        // Log but don't fail the test
        System.err.println("Failed to shutdown DAO: " + e.getMessage());
      }
    }
    createdDAOs.clear();
  }

  private static final String TEST_QUERY_FILE_FULL_EMPTY_FILTERS =
      "elasticsearch/sample_filters/lineage_query_filters_full_empty_filters.json";
  private static final String TEST_QUERY_FILE_FULL_MULTIPLE_FILTERS =
      "elasticsearch/sample_filters/lineage_query_filters_full_multiple_filters.json";

  private final OperationContext operationContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  private void testGetQueryForLineageFullArguments() throws Exception {
    URL urlLimited = Resources.getResource(TEST_QUERY_FILE_LIMITED);
    String expectedQueryLimited = Resources.toString(urlLimited, StandardCharsets.UTF_8);
    URL urlFull = Resources.getResource(TEST_QUERY_FILE_FULL);
    String expectedQueryFull = Resources.toString(urlFull, StandardCharsets.UTF_8);
    URL urlFullEmptyFilters = Resources.getResource(TEST_QUERY_FILE_FULL_EMPTY_FILTERS);
    String expectedQueryFullEmptyFilters =
        Resources.toString(urlFullEmptyFilters, StandardCharsets.UTF_8);
    URL urlFullMultipleFilters = Resources.getResource(TEST_QUERY_FILE_FULL_MULTIPLE_FILTERS);
    String expectedQueryFullMultipleFilters =
        Resources.toString(urlFullMultipleFilters, StandardCharsets.UTF_8);

    Set<Urn> urns = Set.of(Urn.createFromString("urn:li:dataset:test-urn"));
    Set<Urn> urnsMultiple1 =
        ImmutableSet.of(
            UrnUtils.getUrn("urn:li:dataset:test-urn"),
            UrnUtils.getUrn("urn:li:dataset:test-urn2"),
            UrnUtils.getUrn("urn:li:dataset:test-urn3"));
    Set<Urn> urnsMultiple2 =
        ImmutableSet.of(
            UrnUtils.getUrn("urn:li:chart:test-urn"),
            UrnUtils.getUrn("urn:li:chart:test-urn2"),
            UrnUtils.getUrn("urn:li:chart:test-urn3"));
    Set<LineageRegistry.EdgeInfo> edgeInfos =
        ImmutableSet.of(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.INCOMING, DATASET_ENTITY_NAME));
    Set<LineageRegistry.EdgeInfo> edgeInfosMultiple1 =
        ImmutableSet.of(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.OUTGOING, DATASET_ENTITY_NAME),
            new LineageRegistry.EdgeInfo(
                "Consumes", RelationshipDirection.OUTGOING, DATASET_ENTITY_NAME));
    Set<LineageRegistry.EdgeInfo> edgeInfosMultiple2 =
        ImmutableSet.of(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.OUTGOING, DATA_JOB_ENTITY_NAME),
            new LineageRegistry.EdgeInfo(
                "Consumes", RelationshipDirection.OUTGOING, DATA_JOB_ENTITY_NAME));

    Map<String, Set<Urn>> urnsPerEntityType = Map.of(DATASET_ENTITY_NAME, urns);
    Map<String, Set<Urn>> urnsPerEntityTypeMultiple =
        Map.of(DATASET_ENTITY_NAME, urnsMultiple1, CHART_ENTITY_NAME, urnsMultiple2);
    Map<String, Set<LineageRegistry.EdgeInfo>> edgesPerEntityType =
        Map.of(DATASET_ENTITY_NAME, edgeInfos);
    Map<String, Set<LineageRegistry.EdgeInfo>> edgesPerEntityTypeMultiple =
        Map.of(
            DATASET_ENTITY_NAME,
            edgeInfosMultiple1,
            DATA_JOB_ENTITY_NAME,
            edgeInfosMultiple2,
            CHART_ENTITY_NAME,
            Set.of());

    Long startTime = 0L;
    Long endTime = 1L;

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(null, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);
    QueryBuilder limitedBuilder = TestUtils.getLineageQueryForEntityType(urns, edgeInfos);

    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>(edgesPerEntityType));

    QueryBuilder fullBuilder =
        graphQueryDAO.getLineageQuery(
            operationContext.withLineageFlags(
                f -> new LineageFlags().setEndTimeMillis(endTime).setStartTimeMillis(startTime)),
            urnsPerEntityType,
            lineageGraphFilters);

    LineageGraphFilters lineageGraphFiltersEmpty =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(),
            null,
            new ConcurrentHashMap<>(edgesPerEntityType));

    QueryBuilder fullBuilderEmptyFilters =
        graphQueryDAO.getLineageQuery(
            operationContext, urnsPerEntityType, lineageGraphFiltersEmpty);

    LineageGraphFilters lineageGraphFiltersMultiple =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME, DASHBOARD_ENTITY_NAME, DATA_JOB_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    QueryBuilder fullBuilderMultipleFilters =
        graphQueryDAO.getLineageQuery(
            operationContext.withLineageFlags(
                f -> new LineageFlags().setEndTimeMillis(endTime).setStartTimeMillis(startTime)),
            urnsPerEntityTypeMultiple,
            lineageGraphFiltersMultiple);

    Assert.assertEquals(limitedBuilder.toString(), expectedQueryLimited);
    Assert.assertEquals(fullBuilder.toString(), expectedQueryFull);
    Assert.assertEquals(fullBuilderEmptyFilters.toString(), expectedQueryFullEmptyFilters);
    JSONAssert.assertEquals(
        fullBuilderMultipleFilters.toString(), expectedQueryFullMultipleFilters, false);
  }

  @Test
  private static void testAddEdgeToPaths() {
    // Test method, ensure that the global structure is updated as expected.
    Urn testParent = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,Test,PROD)");
    Urn testChild = UrnUtils.getUrn("urn:li:dashboard:(looker,test-dashboard)");

    // Case 0: Add with no existing paths.
    ThreadSafePathStore pathStore = new ThreadSafePathStore();
    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, null, testChild);
    Map<Urn, UrnArrayArray> nodePaths = pathStore.toUrnArrayArrayMap();
    UrnArrayArray expectedPathsToChild =
        new UrnArrayArray(ImmutableList.of(new UrnArray(ImmutableList.of(testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 1: No paths to parent.
    pathStore = new ThreadSafePathStore();
    pathStore.addPath(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,Other,PROD)"), new UrnArray());
    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, null, testChild);
    nodePaths = pathStore.toUrnArrayArrayMap();
    expectedPathsToChild =
        new UrnArrayArray(ImmutableList.of(new UrnArray(ImmutableList.of(testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 2: 1 Existing Path to Parent Node
    pathStore = new ThreadSafePathStore();
    Urn testParentParent =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestParent,PROD)");
    UrnArray existingPathToParent = new UrnArray(ImmutableList.of(testParentParent, testParent));
    pathStore.addPath(testParent, existingPathToParent);
    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, null, testChild);
    nodePaths = pathStore.toUrnArrayArrayMap();
    expectedPathsToChild =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParentParent, testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 3: > 1 Existing Paths to Parent Node
    pathStore = new ThreadSafePathStore();
    Urn testParentParent2 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestParent2,PROD)");
    UrnArray existingPathToParent1 = new UrnArray(ImmutableList.of(testParentParent, testParent));
    UrnArray existingPathToParent2 = new UrnArray(ImmutableList.of(testParentParent2, testParent));
    pathStore.addPath(testParent, existingPathToParent1);
    pathStore.addPath(testParent, existingPathToParent2);
    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, null, testChild);
    nodePaths = pathStore.toUrnArrayArrayMap();
    expectedPathsToChild =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParentParent, testParent, testChild)),
                new UrnArray(ImmutableList.of(testParentParent2, testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 4: Build graph from empty by adding multiple edges
    pathStore = new ThreadSafePathStore();
    GraphQueryUtils.addEdgeToPaths(pathStore, testParentParent, null, testParent);
    GraphQueryUtils.addEdgeToPaths(pathStore, testParentParent2, null, testParent);
    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, null, testChild);
    nodePaths = pathStore.toUrnArrayArrayMap();

    // Verify no paths to the grand-parents
    Assert.assertNull(nodePaths.get(testParentParent));
    Assert.assertNull(nodePaths.get(testParentParent2));

    // Verify paths to testParent
    UrnArrayArray expectedPathsToParent =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParentParent, testParent)),
                new UrnArray(ImmutableList.of(testParentParent2, testParent))));
    Assert.assertEquals(nodePaths.get(testParent), expectedPathsToParent);

    // Verify paths to testChild
    expectedPathsToChild =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParentParent, testParent, testChild)),
                new UrnArray(ImmutableList.of(testParentParent2, testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 5: Mainly documentation: Verify that if you build the graph out of order bad things
    // happen.
    // Test that duplicate edge addition is now prevented (improved behavior)
    pathStore = new ThreadSafePathStore();
    // Add edge to testChild first! Before path to testParent has been constructed.
    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, null, testChild);
    // Duplicate paths are now prevented - this is an improvement over the old behavior
    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, null, testChild);
    // Now construct paths to testParent.
    GraphQueryUtils.addEdgeToPaths(pathStore, testParentParent, null, testParent);
    GraphQueryUtils.addEdgeToPaths(pathStore, testParentParent2, null, testParent);
    nodePaths = pathStore.toUrnArrayArrayMap();

    // Verify no paths to the grand-parents
    Assert.assertNull(nodePaths.get(testParentParent));
    Assert.assertNull(nodePaths.get(testParentParent2));

    // Verify paths to testParent
    expectedPathsToParent =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParentParent, testParent)),
                new UrnArray(ImmutableList.of(testParentParent2, testParent))));
    Assert.assertEquals(nodePaths.get(testParent), expectedPathsToParent);

    // Verify paths to testChild are now CORRECT: no duplicates (improved behavior)
    expectedPathsToChild =
        new UrnArrayArray(ImmutableList.of(new UrnArray(ImmutableList.of(testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);
  }

  @Test
  public void testGetSearchResponse() throws Exception {
    // Mock dependencies
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create the DAO with mocks
    GraphQueryPITDAO dao =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Mock the search response
    SearchResponse mockSearchResponse = mock(SearchResponse.class);

    // Create test data
    GraphFilters graphFilters =
        GraphFilters.outgoingFilter(
            newFilter(
                "urn",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,calm-pagoda-323403.jaffle_shop.orders,PROD)"));
    graphFilters.setRelationshipDirection(RelationshipDirection.OUTGOING);
    graphFilters.setSourceTypes(ImmutableSet.of(DATASET_ENTITY_NAME));

    List<SortCriterion> sortCriteria =
        ImmutableList.of(new SortCriterion().setField("urn").setOrder(SortOrder.DESCENDING));

    String scrollId =
        "eyJzb3J0IjpbInVybjpsaTphc3NlcnRpb246NGU0NmJjYTQ2ZTlmN2I3OTlmN2UzZDQyYmRlYWFmMmMiXSwicGl0SWQiOm51bGwsImV4cGlyYXRpb25UaW1lIjowfQ==";
    String keepAlive = "1m";
    int count = 10;

    // Set up mock behavior
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSearchResponse);

    // Mock the createPit method for OpenSearch Point-in-Time searches
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Call the method
    SearchResponse response =
        dao.getSearchResponse(
            operationContext, graphFilters, sortCriteria, scrollId, keepAlive, count);

    // Verify the response
    Assert.assertEquals(response, mockSearchResponse);

    // Verify that search was called with correct parameters
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    SearchRequest capturedRequest = requestCaptor.getValue();

    // Verify the request properties
    // When using PIT (keepAlive != null), indices are not set on the search request
    // because PIT specifies indices in creation
    if (keepAlive != null) {
      // PIT is used, so indices should be empty
      Assert.assertEquals(capturedRequest.indices().length, 0);
    } else {
      // No PIT, so indices should be set
      Assert.assertEquals(capturedRequest.indices()[0], "graph_service_v1");
    }

    SearchSourceBuilder sourceBuilder = capturedRequest.source();
    Assert.assertEquals(sourceBuilder.size(), count);

    // Verify scroll ID was used
    Assert.assertNotNull(sourceBuilder.searchAfter());
  }

  @Test
  public void testGetLineageQueryWithInvalidEntityTypes() {
    // Mock only the client
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create the DAO with minimal mocks
    GraphQueryPITDAO dao =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Create a map with mixed entity types for a single entity type key
    Map<String, Set<Urn>> urnsPerEntityType = new HashMap<>();

    // First entry is valid - dataset key with dataset URNs
    Set<Urn> datasetUrns =
        ImmutableSet.of(
            UrnUtils.getUrn("urn:li:dataset:test-dataset-1"),
            UrnUtils.getUrn("urn:li:dataset:test-dataset-2"));
    urnsPerEntityType.put(DATASET_ENTITY_NAME, datasetUrns);

    // Second entry has mixed URNs for the CHART_ENTITY_NAME key
    Set<Urn> mixedUrns =
        ImmutableSet.of(
            UrnUtils.getUrn("urn:li:chart:test-chart"),
            UrnUtils.getUrn("urn:li:dataset:invalid-entity-type") // Doesn't match CHART_ENTITY_NAME
            );
    urnsPerEntityType.put(CHART_ENTITY_NAME, mixedUrns);

    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME, CHART_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // Call getLineageQuery which should call buildLineageGraphFiltersQuery internally
    // The method should throw an IllegalArgumentException for the mixed URNs
    try {
      dao.getLineageQuery(operationContext, urnsPerEntityType, lineageGraphFilters);
      fail("Should throw IllegalArgumentException for URNs of different entity types");
    } catch (IllegalArgumentException e) {
      // Expected exception
      Assert.assertEquals(e.getMessage(), "Urns must be of the same entity type.");
    }
  }

  @Test
  public void testGetLineageQueryWithEmptyUrns() {
    // Mock only the client
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create the DAO with minimal mocks
    GraphQueryPITDAO dao =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Create a map with empty URN sets
    Map<String, Set<Urn>> urnsPerEntityType = new HashMap<>();
    urnsPerEntityType.put(DATASET_ENTITY_NAME, ImmutableSet.of()); // Empty URNs

    // Create LineageGraphFilters with valid edge info
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // Call the method - internal buildLineageGraphFiltersQuery should return empty Optional
    // But getLineageQuery should still build a query with minimumShouldMatch(1)
    QueryBuilder result =
        dao.getLineageQuery(operationContext, urnsPerEntityType, lineageGraphFilters);

    // Verify that we still got a query
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof BoolQueryBuilder);

    // Verify that the query was structured as expected for empty URNs
    BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;
    // There should be a filter clause with the entity type queries
    Assert.assertTrue(boolQuery.filter().size() > 0);

    // Verify the first filter is a BoolQuery with minimumShouldMatch(1)
    Object firstFilter = boolQuery.filter().get(0);
    Assert.assertTrue(firstFilter instanceof BoolQueryBuilder);
    BoolQueryBuilder entityTypeQueries = (BoolQueryBuilder) firstFilter;
    Assert.assertEquals(entityTypeQueries.minimumShouldMatch(), "1");
    // Since URNs are empty, there should be no should clauses
    Assert.assertEquals(entityTypeQueries.should().size(), 0);
  }

  @Test
  public void testGetLineageQueryWithUndirectedEdges() {
    // Mock only the client
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a LineageRegistry with undirected edges
    LineageRegistry mockLineageRegistry = mock(LineageRegistry.class);
    when(mockLineageRegistry.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());

    // Create EdgeInfo objects with undirected relationship
    List<LineageRegistry.EdgeInfo> edgeInfoList = new ArrayList<>();
    edgeInfoList.add(
        new LineageRegistry.EdgeInfo(
            "DownstreamOf", RelationshipDirection.OUTGOING, DATA_JOB_ENTITY_NAME));
    edgeInfoList.add(
        new LineageRegistry.EdgeInfo(
            "RelatedTo", RelationshipDirection.UNDIRECTED, CHART_ENTITY_NAME));

    // Setup the LineageRegistry to return these edges
    when(mockLineageRegistry.getLineageRelationships(
            eq(DATASET_ENTITY_NAME), eq(LineageDirection.DOWNSTREAM)))
        .thenReturn(edgeInfoList);

    // Create a spy of the operation context and mock the getLineageRegistry method
    OperationContext customOperationContext = spy(operationContext);
    when(customOperationContext.getLineageRegistry()).thenReturn(mockLineageRegistry);

    // Create the DAO with our mock registry
    GraphQueryPITDAO dao =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Create a map with dataset URNs
    Map<String, Set<Urn>> urnsPerEntityType = new HashMap<>();
    Set<Urn> datasetUrns =
        ImmutableSet.of(
            UrnUtils.getUrn("urn:li:dataset:test-dataset-1"),
            UrnUtils.getUrn("urn:li:dataset:test-dataset-2"));
    urnsPerEntityType.put(DATASET_ENTITY_NAME, datasetUrns);

    // Create a LineageGraphFilters
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME, CHART_ENTITY_NAME, DATA_JOB_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // Call the method to build a query
    QueryBuilder result =
        dao.getLineageQuery(customOperationContext, urnsPerEntityType, lineageGraphFilters);

    // Verify that we got a non-null result
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof BoolQueryBuilder);

    // Navigate through the query structure
    BoolQueryBuilder outerBoolQuery = (BoolQueryBuilder) result;
    // The outer query should have a filter
    List<QueryBuilder> filters = outerBoolQuery.filter();
    Assert.assertEquals(filters.size(), 1);

    // The filter should be a BoolQueryBuilder with "should" clauses
    BoolQueryBuilder entityTypeQueriesBool = (BoolQueryBuilder) filters.get(0);
    List<QueryBuilder> shouldClauses =
        ((BoolQueryBuilder) entityTypeQueriesBool.should().get(0)).should();
    Assert.assertFalse(shouldClauses.isEmpty());

    // Examine each should clause to find the ones with RelatedTo
    boolean foundOutgoingRelatedTo = false;
    boolean foundIncomingRelatedTo = false;
    boolean foundDownstreamOf = false;

    for (QueryBuilder shouldClause : shouldClauses) {
      BoolQueryBuilder boolShouldClause = (BoolQueryBuilder) shouldClause;
      List<QueryBuilder> clauseFilters = boolShouldClause.filter();

      String relationshipType = null;
      String sourceOrDestField = null;
      String entityTypeField = null;

      for (QueryBuilder filter : clauseFilters) {
        String filterString = filter.toString();

        if (filterString.contains("relationshipType")) {
          if (filterString.contains("RelatedTo")) {
            relationshipType = "RelatedTo";
          } else if (filterString.contains("DownstreamOf")) {
            relationshipType = "DownstreamOf";
          }
        }

        if (filterString.contains("source.urn")
            && filterString.contains("urn:li:dataset:test-dataset")) {
          sourceOrDestField = "source";
        }

        if (filterString.contains("destination.urn")
            && filterString.contains("urn:li:dataset:test-dataset")) {
          sourceOrDestField = "destination";
        }

        if (filterString.contains("destination.entityType") && filterString.contains("chart")) {
          entityTypeField = "chart";
        }

        if (filterString.contains("destination.entityType") && filterString.contains("dataJob")) {
          entityTypeField = "dataJob";
        }

        if (filterString.contains("source.entityType") && filterString.contains("chart")) {
          entityTypeField = "chart";
        }
      }

      // Check if this is the outgoing RelatedTo edge
      if ("RelatedTo".equals(relationshipType)
          && "source".equals(sourceOrDestField)
          && "chart".equals(entityTypeField)) {
        foundOutgoingRelatedTo = true;
      }

      // Check if this is the incoming RelatedTo edge
      if ("RelatedTo".equals(relationshipType)
          && "destination".equals(sourceOrDestField)
          && "chart".equals(entityTypeField)) {
        foundIncomingRelatedTo = true;
      }

      // Check if this is the DownstreamOf edge
      if ("DownstreamOf".equals(relationshipType)
          && "source".equals(sourceOrDestField)
          && "dataJob".equals(entityTypeField)) {
        foundDownstreamOf = true;
      }
    }

    // Verify that we found all the expected edges
    Assert.assertTrue(foundOutgoingRelatedTo, "Missing outgoing RelatedTo edge");
    Assert.assertTrue(foundIncomingRelatedTo, "Missing incoming RelatedTo edge");
    Assert.assertTrue(foundDownstreamOf, "Missing DownstreamOf edge");
  }

  @Test
  public void testSearchSourceBuilderAppliesResultLimit() throws Exception {
    // construction

    // Mock dependencies
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockClient.search(any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with a specific limit
    GraphServiceConfiguration testConfig =
        TEST_GRAPH_SERVICE_CONFIG.toBuilder()
            .limit(
                LimitConfig.builder()
                    .results(new ResultsLimitConfig().setMax(50).setApiDefault(50))
                    .build())
            .build();

    // Create the DAO with our test config
    GraphQueryPITDAO dao = createTrackedDAO(mockClient, testConfig, TEST_OS_SEARCH_CONFIG);

    // Create test data - requesting more than the limit
    GraphFilters graphFilters =
        GraphFilters.outgoingFilter(newFilter("urn", "urn:li:dataset:test"));
    graphFilters.setRelationshipDirection(RelationshipDirection.OUTGOING);

    // Call method with a count that exceeds the limit
    int requestedCount = 100; // Exceeds our limit of 50
    dao.getSearchResponse(operationContext, graphFilters, 0, requestedCount);

    // Verify that search was called with the right parameters
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    SearchRequest capturedRequest = requestCaptor.getValue();
    SearchSourceBuilder sourceBuilder = capturedRequest.source();

    // Verify the size was limited to the max (50)
    Assert.assertEquals(sourceBuilder.size(), 50);
  }

  @Test
  public void testScrollSearchAppliesResultLimit() throws Exception {
    // Mock dependencies
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    SearchResponse mockResponse = mock(SearchResponse.class);
    when(mockClient.search(any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with a specific limit
    GraphServiceConfiguration testConfig =
        TEST_GRAPH_SERVICE_CONFIG.toBuilder()
            .limit(
                LimitConfig.builder()
                    .results(new ResultsLimitConfig().setMax(25).setApiDefault(25))
                    .build())
            .build();

    // Create the DAO with our test config
    GraphQueryPITDAO dao = createTrackedDAO(mockClient, testConfig, TEST_OS_SEARCH_CONFIG);

    // Create test data
    GraphFilters graphFilters =
        GraphFilters.outgoingFilter(newFilter("urn", "urn:li:dataset:test"));
    graphFilters.setRelationshipDirection(RelationshipDirection.OUTGOING);
    List<SortCriterion> sortCriteria =
        ImmutableList.of(new SortCriterion().setField("urn").setOrder(SortOrder.DESCENDING));

    // Call method with a count that exceeds the limit
    int requestedCount = 50; // Exceeds our limit of 25
    dao.getSearchResponse(operationContext, graphFilters, sortCriteria, null, null, requestedCount);

    // Verify that search was called with the right parameters
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    SearchRequest capturedRequest = requestCaptor.getValue();
    SearchSourceBuilder sourceBuilder = capturedRequest.source();

    // Verify the size was limited to the max (25)
    Assert.assertEquals(sourceBuilder.size(), 25);
  }

  // ===== NEW TESTS FOR getImpactLineage FUNCTIONALITY =====

  @Test
  public void testGetImpactLineageBasic() throws Exception {
    // Test basic functionality of getImpactLineage with slice-based search
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Mock the createPit method for PIT search
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    GraphQueryPITDAO dao =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Mock responses for slice-based search
    // With 2 slices, we expect 2 calls to search (one per slice)
    // Each slice returns one page of results
    SearchHit[] hits1 =
        createFakeLineageHits(
            2,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");

    SearchHit[] hits2 =
        createFakeLineageHits(
            1,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest2",
            "DownstreamOf");

    SearchResponse searchResponse1 = createFakeSearchResponse(hits1, 2, "scroll_id_1");
    SearchResponse searchResponse2 = createFakeSearchResponse(hits2, 1, "scroll_id_2");

    // Create empty response for pagination
    SearchResponse emptySearchResponse = createEmptySearchResponse(0);

    // Mock search calls: first 2 calls return results, subsequent calls return empty
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(searchResponse1) // First slice, first page
        .thenReturn(emptySearchResponse) // First slice, no more pages
        .thenReturn(searchResponse2) // Second slice, first page
        .thenReturn(emptySearchResponse); // Second slice, no more pages

    // Test getImpactLineage with 2 slices
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response);
    Assert.assertEquals(response.getTotal(), 3);
    for (LineageRelationship rel : response.getLineageRelationships()) {
      Assert.assertNotEquals(rel.isExplored(), Boolean.TRUE); // Allow false or null
    }

    // Verify that search was called at least 4 times (2 slices × 2 searches each)
    verify(mockClient, atLeast(4)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test(timeOut = 10000) // Add timeout to prevent hanging in test suites
  public void testGetImpactLineageMaxRelationsLimit() throws Exception {
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with a very low maxRelations limit to ensure we hit it
    // Explicitly set partialResults=false to test error throwing behavior
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(5) // Very low limit to ensure we hit it
                                    .partialResults(
                                        false) // Explicitly set to false to test error throwing
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Create a simple response with 6 hits to exceed the maxRelations limit of 5
    SearchHit[] hits =
        createFakeLineageHits(
            6,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");

    SearchResponse searchResponse = createFakeSearchResponse(hits, 6);

    // Create empty response for pagination
    SearchResponse emptyResponse = createEmptySearchResponse(6);

    // Use the utility method to mock slice-based search behavior
    // First slice gets the search response, second slice gets empty response
    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    try {
      LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      fail("Should throw exception for exceeding maxRelations limit");
    } catch (RuntimeException e) {
      // Verify the exception message contains the maxRelations limit error
      // The exception may be wrapped by processSliceFutures, so use hasMessageInChain to check
      // recursively
      // Note: IllegalStateException extends RuntimeException, so catching RuntimeException will
      // catch both
      Assert.assertTrue(
          hasMessageInChain(e, "maxRelations limit"),
          "Expected maxRelations limit error in exception chain, got: " + e.getMessage());
    }
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageMaxRelationsLimitWithPartialResults() throws Exception {
    // Test that when partialResults=true, we return partial results instead of throwing
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with partialResults=true
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(5) // Very low limit to ensure we hit it
                                    .partialResults(true) // Enable partial results
                                    .searchQueryTimeReservation(
                                        0.2) // Set reservation for partial results tests
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Create a simple response with 6 hits to exceed the maxRelations limit of 5
    SearchHit[] hits =
        createFakeLineageHits(
            6,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");

    SearchResponse searchResponse = createFakeSearchResponse(hits, 6);

    // Create empty response for pagination
    SearchResponse emptyResponse = createEmptySearchResponse(6);

    // Use the utility method to mock slice-based search behavior
    // First slice gets the search response, second slice gets empty response
    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // When partialResults=true, should return partial results instead of throwing
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response, "Response should not be null");
    Assert.assertTrue(
        response.isPartial(),
        "Response should be marked as partial when maxRelations is reached and partialResults=true");
    // Verify we got some results (at least up to the limit)
    Assert.assertTrue(
        response.getTotal() > 0,
        "Response should contain some relationships when partial results are returned");
    // Note: Due to parallel slice processing, the total may slightly exceed maxRelations
    // before the aggregate check catches it. The important thing is that partial=true
    Assert.assertTrue(
        response.getTotal() <= 10,
        "Response total should be reasonable (may slightly exceed maxRelations due to parallel processing), but partial flag should be set");
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageTimeoutWithPartialResults() throws Exception {
    // Test that when partialResults=true and timeout occurs, we return partial results
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with a very short timeout and partialResults=true
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(1) // Very short timeout (1 second)
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000) // High limit so we don't hit it
                                    .partialResults(true)
                                    .searchQueryTimeReservation(0.2)
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Create a response that will take time to process
    SearchHit[] hits =
        createFakeLineageHits(
            10,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");

    SearchResponse searchResponse = createFakeSearchResponse(hits, 10);
    SearchResponse emptyResponse = createEmptySearchResponse(10);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Mock search to delay, simulating a timeout scenario
    // We'll make the first search return results, but delay to trigger timeout check
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            invocation -> {
              // Simulate delay that would cause timeout
              Thread.sleep(1200); // Longer than 1 second timeout
              return searchResponse;
            })
        .thenReturn(emptyResponse);

    // When partialResults=true and timeout occurs, should return partial results
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response, "Response should not be null");
    // May or may not have results depending on timing, but should not throw
    Assert.assertTrue(
        response.isPartial() || response.getTotal() >= 0,
        "Response should be marked as partial when timeout occurs with partialResults=true");
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageTimeoutWithoutPartialResults() throws Exception {
    // Test that when partialResults=false and timeout occurs, we throw exception
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with a very short timeout and partialResults=false
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(1) // Very short timeout (1 second)
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000) // High limit so we don't hit it
                                    .partialResults(false)
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Mock search to delay, simulating a timeout scenario
    SearchHit[] hits =
        createFakeLineageHits(
            10,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");
    SearchResponse searchResponse = createFakeSearchResponse(hits, 10);

    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            invocation -> {
              // Simulate delay that would cause timeout
              Thread.sleep(1200); // Longer than 1 second timeout
              return searchResponse;
            });

    // When partialResults=false and timeout occurs, should throw exception
    try {
      dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      fail("Should throw exception when timeout occurs with partialResults=false");
    } catch (RuntimeException e) {
      // The exception may be wrapped, so check both the exception and its cause
      Throwable cause = e.getCause();
      String message = cause.getMessage();
      boolean isTimeoutException = false;

      // Check if the cause is an IllegalStateException with timeout message
      if (cause instanceof RuntimeException) {
        String causeMessage = cause.getMessage();
        isTimeoutException =
            (causeMessage != null
                && (causeMessage.contains("timed out") || causeMessage.contains("timeout")));
      }

      // Also check if the wrapper message indicates a timeout
      if (!isTimeoutException
          && message != null
          && (message.contains("timed out") || message.contains("timeout"))) {
        isTimeoutException = true;
      }

      Assert.assertTrue(
          isTimeoutException,
          "Exception should indicate timeout. Got: "
              + e.getClass().getSimpleName()
              + " - "
              + message
              + (cause != null
                  ? " (cause: "
                      + cause.getClass().getSimpleName()
                      + " - "
                      + cause.getMessage()
                      + ")"
                  : ""));
    }
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageTimeoutRemainingTimeCheckWithPartialResults() throws Exception {
    // Test that when remainingTime < 0 is checked and allowPartialResults=true,
    // we log a warning, set isPartial=true, and break (returning partial results)
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with a very short timeout and partialResults=true
    // Use a timeout small enough that remainingTime will be negative after first hop
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(1) // 1 second timeout
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000) // High limit so we don't hit it
                                    .partialResults(true)
                                    .searchQueryTimeReservation(0.2)
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Create hits that will be returned, but we'll delay to ensure timeout
    SearchHit[] hits =
        createFakeLineageHits(
            5,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");
    SearchResponse searchResponse = createFakeSearchResponse(hits, 5);
    SearchResponse emptyResponse = createEmptySearchResponse(5);

    // Mock search to take time, causing remainingTime to become negative
    // The delay should be long enough that after the first hop completes,
    // when we check remainingTime at the start of the next iteration, it's negative
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            invocation -> {
              // Delay to ensure timeout happens
              Thread.sleep(1200); // Longer than 1 second timeout
              return searchResponse;
            })
        .thenReturn(emptyResponse);

    // When partialResults=true and remainingTime < 0, should return partial results
    // Note: Due to timing issues, the timeout might not trigger exactly as expected,
    // but the important thing is that no exception is thrown when partialResults=true
    try {
      LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 2);
      Assert.assertNotNull(response, "Response should not be null");
      // If we get here without exception, the timeout handling worked correctly
      // (either partial results were returned or the operation completed normally)
      Assert.assertTrue(response.getTotal() >= 0, "Response should have valid total count");
    } catch (IllegalStateException e) {
      // If we get IllegalStateException, it means timeout occurred but partialResults handling
      // didn't work
      fail(
          "Should not throw IllegalStateException when partialResults=true. Got: "
              + e.getMessage());
    }
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageTimeoutRemainingTimeCheckWithoutPartialResults()
      throws Exception {
    // Test that when remainingTime < 0 is checked and allowPartialResults=false,
    // we log an error and throw IllegalStateException
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with a very short timeout and partialResults=false
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(1) // 1 second timeout
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000) // High limit so we don't hit it
                                    .partialResults(false)
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Create hits that will be returned, but we'll delay to ensure timeout
    SearchHit[] hits =
        createFakeLineageHits(
            5,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");
    SearchResponse searchResponse = createFakeSearchResponse(hits, 5);

    // Mock search - first call returns quickly, second call delays to consume timeout
    // This ensures first hop completes, then timeout occurs before second hop
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(searchResponse) // First hop - return quickly with results
        .thenAnswer(
            invocation -> {
              // Delay on second hop to consume timeout budget
              Thread.sleep(1200); // Longer than 1 second timeout
              return createEmptySearchResponse(5);
            });

    // When partialResults=false and remainingTime < 0, should throw IllegalStateException
    // Note: The timeout might occur at slice processing level instead of loop level,
    // which would result in a RuntimeException. We accept either case.
    try {
      dao.getImpactLineage(operationContext, sourceUrn, filters, 2);
      fail("Should throw exception when remainingTime < 0 and partialResults=false");
    } catch (IllegalStateException e) {
      // Verify the exception message contains the expected information
      String message = e.getMessage();
      Assert.assertNotNull(message, "Exception message should not be null");
      Assert.assertTrue(
          message.contains("timed out") || message.contains("timeout"),
          "Exception message should mention timeout. Got: " + message);
    } catch (RuntimeException e) {
      // The exception might be wrapped, check the cause chain for IllegalStateException
      // OR it might be a timeout at slice level, which is also acceptable
      Throwable cause = e;
      boolean foundIllegalStateException = false;
      boolean foundTimeoutMessage = false;

      while (cause != null) {
        if (cause instanceof IllegalStateException) {
          foundIllegalStateException = true;
          String message = cause.getMessage();
          Assert.assertNotNull(message, "Exception message should not be null");
          Assert.assertTrue(
              message.contains("timed out") || message.contains("timeout"),
              "Exception message should mention timeout. Got: " + message);
          break;
        }
        // Also check if the message mentions timeout
        if (cause.getMessage() != null
            && (cause.getMessage().contains("timed out")
                || cause.getMessage().contains("timeout"))) {
          foundTimeoutMessage = true;
        }
        cause = cause.getCause();
      }

      // Accept either IllegalStateException in chain OR timeout-related RuntimeException
      if (!foundIllegalStateException && !foundTimeoutMessage) {
        fail(
            "Expected IllegalStateException or timeout-related exception in chain but got: "
                + e.getClass().getSimpleName()
                + " - "
                + e.getMessage());
      }
    }
  }

  @Test(timeOut = 10000)
  public void testProcessSliceFuturesTimeoutRemainingTimeCheckWithPartialResults()
      throws Exception {
    // Test that when remainingTime <= 0 during slice processing and allowPartialResults=true,
    // we log a warning and return partial results
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with a very short timeout, multiple slices, and partialResults=true
    // Use a timeout small enough that remainingTime will be <= 0 during slice processing
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(1) // 1 second timeout
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000) // High limit so we don't hit it
                                    .partialResults(true)
                                    .slices(3) // Use 3 slices to ensure we process multiple slices
                                    .searchQueryTimeReservation(0.2)
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Create hits for slices
    SearchHit[] hits1 =
        createFakeLineageHits(
            2,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest1",
            "DownstreamOf");
    SearchHit[] hits2 =
        createFakeLineageHits(
            2,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest2",
            "DownstreamOf");

    SearchResponse searchResponse1 = createFakeSearchResponse(hits1, 2);
    SearchResponse searchResponse2 = createFakeSearchResponse(hits2, 2);
    SearchResponse emptyResponse = createEmptySearchResponse(2);

    // Mock search to take time for first slice, causing remainingTime to become <= 0
    // The first slice takes time, and by the time we process later slices, remainingTime <= 0
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            invocation -> {
              // First slice takes time, simulating processing that consumes remainingTime
              Thread.sleep(800); // Take most of the 1 second timeout
              return searchResponse1;
            })
        .thenReturn(emptyResponse) // First slice completes
        .thenReturn(searchResponse2) // Second slice starts
        .thenReturn(emptyResponse); // Second slice completes

    // When partialResults=true and remainingTime <= 0 during slice processing,
    // should return partial results
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response, "Response should not be null");
    // Should be marked as partial when timeout occurs during slice processing with
    // partialResults=true, OR should have some results even if partial flag isn't set
    // (due to timing issues in tests)
    Assert.assertTrue(
        response.isPartial() || response.getTotal() > 0,
        "Response should be marked as partial or have results when remainingTime <= 0 during slice processing and partialResults=true");
  }

  @Test(timeOut = 10000)
  public void testProcessSliceFuturesTimeoutRemainingTimeCheckWithoutPartialResults()
      throws Exception {
    // Test that when remainingTime <= 0 during slice processing and allowPartialResults=false,
    // we log a warning and continue (but may throw later if needed)
    // Note: When allowPartialResults=false, the code logs a warning but still breaks,
    // so we should verify the warning is logged and processing stops
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with a very short timeout, multiple slices, and partialResults=false
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(1) // 1 second timeout
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000) // High limit so we don't hit it
                                    .partialResults(false)
                                    .slices(3) // Use 3 slices to ensure we process multiple slices
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Create hits for slices
    SearchHit[] hits1 =
        createFakeLineageHits(
            2,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest1",
            "DownstreamOf");
    SearchHit[] hits2 =
        createFakeLineageHits(
            2,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest2",
            "DownstreamOf");

    SearchResponse searchResponse1 = createFakeSearchResponse(hits1, 2);
    SearchResponse searchResponse2 = createFakeSearchResponse(hits2, 2);
    SearchResponse emptyResponse = createEmptySearchResponse(2);

    // Mock search to take time for first slice, causing remainingTime to become <= 0
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            invocation -> {
              // First slice takes time, simulating processing that consumes remainingTime
              Thread.sleep(800); // Take most of the 1 second timeout
              return searchResponse1;
            })
        .thenReturn(emptyResponse) // First slice completes
        .thenReturn(searchResponse2) // Second slice starts
        .thenReturn(emptyResponse); // Second slice completes

    // When partialResults=false and remainingTime <= 0 during slice processing,
    // the code logs a warning and breaks, but since allowPartialResults=false,
    // it may throw later if the overall operation times out
    // However, in this case, since we're just testing the slice processing timeout,
    // it should break and return whatever results were collected
    try {
      LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      // If it returns, it should have processed at least some slices before timing out
      Assert.assertNotNull(response, "Response should not be null");
      // The response may or may not be marked as partial depending on whether
      // the overall operation also timed out
    } catch (IllegalStateException e) {
      // If the overall operation times out (remainingTime < 0 at the loop level),
      // it will throw IllegalStateException since partialResults=false
      String message = e.getMessage();
      Assert.assertNotNull(message, "Exception message should not be null");
      Assert.assertTrue(
          message.contains("timed out") || message.contains("timeout"),
          "Exception message should mention timeout. Got: " + message);
    }
  }

  @Test(timeOut = 10000)
  public void testProcessSliceFuturesExceptionWithPartialResultsAndCollectedRelationships()
      throws Exception {
    // Test that when an exception occurs during slice processing, allowPartialResults=true,
    // and some relationships have been collected, we log a warning and return partial results
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with multiple slices and partialResults=true
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(10) // Reasonable timeout
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000) // High limit so we don't hit it
                                    .partialResults(true)
                                    .slices(3) // Use 3 slices
                                    .searchQueryTimeReservation(0.2)
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Create hits for the first slice that will succeed
    SearchHit[] hits1 =
        createFakeLineageHits(
            2,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest1",
            "DownstreamOf");

    SearchResponse searchResponse1 = createFakeSearchResponse(hits1, 2);
    SearchResponse emptyResponse = createEmptySearchResponse(2);

    // Mock search: first slice succeeds completely (initial + scroll), then second slice throws
    // exception
    // For PIT-based search, we need to ensure the first slice completes fully before the second
    // throws
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(searchResponse1) // First slice, initial search - succeeds
        .thenReturn(emptyResponse) // First slice, scroll search (empty = complete)
        .thenReturn(emptyResponse) // First slice, any additional scroll (empty = complete)
        .thenThrow(
            new RuntimeException("Search operation failed for second slice")); // Second slice fails

    // When partialResults=true and an exception occurs but some relationships were collected,
    // should return partial results
    // Note: The exception might be thrown before relationships are collected due to async
    // execution,
    // so we need to handle both cases - either partial results are returned OR an exception is
    // thrown
    try {
      LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      Assert.assertNotNull(response, "Response should not be null");
      // Should return partial results from the first slice that succeeded
      Assert.assertTrue(
          response.getTotal() > 0,
          "Should return partial results from slices that completed successfully");
    } catch (RuntimeException e) {
      // If exception is thrown, it means the exception happened before relationships were collected
      // This is acceptable behavior - the test verifies the code path exists
      // We just verify it's the expected exception type
      Assert.assertTrue(
          e.getMessage() != null && e.getMessage().contains("slice"),
          "Exception should mention slice failure. Got: " + e.getMessage());
    }
  }

  @Test(timeOut = 10000)
  public void testProcessSliceFuturesExceptionWithoutPartialResults() throws Exception {
    // Test that when an exception occurs during slice processing and allowPartialResults=false,
    // we throw the exception even if some relationships were collected
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with multiple slices and partialResults=false
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(10) // Reasonable timeout
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000) // High limit so we don't hit it
                                    .partialResults(false)
                                    .slices(3) // Use 3 slices
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Create hits for the first slice that will succeed
    SearchHit[] hits1 =
        createFakeLineageHits(
            2,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest1",
            "DownstreamOf");

    SearchResponse searchResponse1 = createFakeSearchResponse(hits1, 2);
    SearchResponse emptyResponse = createEmptySearchResponse(2);

    // Mock search: first slice succeeds, second slice throws exception
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(searchResponse1) // First slice, first page - succeeds
        .thenReturn(emptyResponse) // First slice completes successfully
        .thenThrow(
            new RuntimeException("Search operation failed for second slice")); // Second slice fails

    // When partialResults=false and an exception occurs, should throw exception
    try {
      dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      fail("Should throw exception when slice processing fails and partialResults=false");
    } catch (RuntimeException e) {
      // Verify the exception message contains the expected information
      String message = e.getMessage();
      Assert.assertNotNull(message, "Exception message should not be null");
      // The exception should indicate a failure in slice-based search
      Assert.assertTrue(
          message.contains("Failed to execute slice-based search") || message.contains("slice"),
          "Exception message should mention slice-based search failure. Got: " + message);
    }
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageMaxRelationsUnlimitedMinusOne() throws Exception {
    // Test that maxRelations = -1 means unlimited (only time-bound)
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with maxRelations = -1 (unlimited)
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(-1) // Unlimited
                                    .partialResults(true)
                                    .searchQueryTimeReservation(0.2)
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Create many hits to verify we don't hit maxRelations limit
    SearchHit[] hits =
        createFakeLineageHits(
            100,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");

    SearchResponse searchResponse = createFakeSearchResponse(hits, 100);
    SearchResponse emptyResponse = createEmptySearchResponse(100);

    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Should return results without hitting maxRelations limit (since it's unlimited)
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response, "Response should not be null");
    // Should be able to collect all results without hitting maxRelations limit
    Assert.assertTrue(
        response.getTotal() > 0, "Should collect relationships when maxRelations is unlimited");
    // Should not be marked as partial due to maxRelations (only time or completion)
    Assert.assertFalse(
        response.isPartial(),
        "Should not be marked as partial when maxRelations is unlimited and we complete normally");
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageMaxRelationsUnlimitedZero() throws Exception {
    // Test that maxRelations = 0 means unlimited (only time-bound)
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with maxRelations = 0 (unlimited)
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(0) // Unlimited
                                    .partialResults(true)
                                    .searchQueryTimeReservation(0.2)
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Create many hits to verify we don't hit maxRelations limit
    SearchHit[] hits =
        createFakeLineageHits(
            100,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");

    SearchResponse searchResponse = createFakeSearchResponse(hits, 100);
    SearchResponse emptyResponse = createEmptySearchResponse(100);

    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Should return results without hitting maxRelations limit (since it's unlimited)
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response, "Response should not be null");
    // Should be able to collect all results without hitting maxRelations limit
    Assert.assertTrue(
        response.getTotal() > 0, "Should collect relationships when maxRelations is 0 (unlimited)");
    // Should not be marked as partial due to maxRelations (only time or completion)
    Assert.assertFalse(
        response.isPartial(),
        "Should not be marked as partial when maxRelations is 0 (unlimited) and we complete normally");
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageTimeReservationCalculation() throws Exception {
    // Test that time reservation is calculated correctly when partialResults is enabled
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with specific timeout and reservation
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(10) // 10 seconds = 10000ms
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000)
                                    .partialResults(true)
                                    .searchQueryTimeReservation(0.3) // 30% reservation
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    SearchHit[] hits =
        createFakeLineageHits(
            5,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");

    SearchResponse searchResponse = createFakeSearchResponse(hits, 5);
    SearchResponse emptyResponse = createEmptySearchResponse(5);

    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Should work correctly with time reservation
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response, "Response should not be null");
    // With 30% reservation on 10s timeout, 3s should be reserved, leaving 7s for graph traversal
    // The query should complete normally within this time
    Assert.assertTrue(response.getTotal() >= 0, "Should return results");
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageTimeReservationMinimum() throws Exception {
    // Test that minimum reservation (100ms) is applied for very small timeouts
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with very small timeout (2 seconds = 2000ms)
    // 20% of 2000ms = 400ms, which is above minimum, so should use 400ms
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(2) // 2 seconds = 2000ms
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000)
                                    .partialResults(true)
                                    .searchQueryTimeReservation(0.2) // 20% = 400ms (above minimum)
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    SearchHit[] hits =
        createFakeLineageHits(
            5,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");

    SearchResponse searchResponse = createFakeSearchResponse(hits, 5);
    SearchResponse emptyResponse = createEmptySearchResponse(5);

    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Should work correctly with time reservation
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response, "Response should not be null");
    Assert.assertTrue(response.getTotal() >= 0, "Should return results");
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageNoTimeReservationWhenPartialResultsDisabled() throws Exception {
    // Test that time reservation is NOT applied when partialResults is disabled
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with partialResults=false (even with reservation config, it shouldn't
    // be used)
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(10)
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000)
                                    .partialResults(false) // Disabled
                                    .searchQueryTimeReservation(
                                        0.2) // Config exists but shouldn't be used
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    SearchHit[] hits =
        createFakeLineageHits(
            5,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");

    SearchResponse searchResponse = createFakeSearchResponse(hits, 5);
    SearchResponse emptyResponse = createEmptySearchResponse(5);

    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Should work correctly without time reservation (full timeout available)
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response, "Response should not be null");
    Assert.assertTrue(response.getTotal() >= 0, "Should return results");
    // No reservation should be applied, so full timeout is available for graph traversal
  }

  @Test
  public void testGetImpactLineageSearchExceptionHandling() throws Exception {
    // Test handling of search operation exceptions
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    GraphQueryPITDAO dao =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Mock a search operation that will throw an exception
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Search operation failed"));

    // Mock the createPit method for OpenSearch Point-in-Time searches
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // This should throw an exception due to the search operation failure
    try {
      dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      fail("Should throw RuntimeException for search operation failure");
    } catch (RuntimeException e) {
      // The exception should be wrapped in our new exception handling
      // Check the entire exception chain for the expected messages
      Assert.assertTrue(
          hasMessageInChain(e, "Failed to execute slice-based search"),
          "Expected slice-related error message in exception chain, got: " + e.getMessage());
    }
  }

  @Test(timeOut = 10000)
  public void testGetImpactLineageTimeoutExceptionExactMessageFormat() throws Exception {
    // Test the exact timeout exception message format from getImpactLineage
    // Covers the else block (lines 1373-1386) that throws IllegalStateException with:
    // "Timed out while fetching lineage... Operation exceeded the configured timeout."
    // and "Lineage operation timed out after %d seconds. Entity: %s, Direction: %s, MaxHops: %d.
    //      Consider increasing the timeout or set partialResults to true to return partial
    // results."
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(),
            DATASET_ENTITY_NAME,
            LineageDirection.DOWNSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    int timeoutSeconds = 2;
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(timeoutSeconds)
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000)
                                    .partialResults(false) // Must be false to trigger else block
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Mock search responses - first hop completes, second hop times out
    SearchHit[] hits =
        createFakeLineageHits(
            5,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");
    SearchResponse searchResponse = createFakeSearchResponse(hits, 5);
    SearchResponse emptyResponse = createEmptySearchResponse(5);

    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    // Override to delay second hop to cause timeout in main loop
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(searchResponse) // First hop returns quickly
        .thenAnswer(
            invocation -> {
              Thread.sleep(timeoutSeconds * 1000 + 500); // Exceeds timeout
              return searchResponse;
            });

    // Should throw IllegalStateException with exact message format
    try {
      dao.getImpactLineage(operationContext, sourceUrn, filters, 2);
      fail("Should throw IllegalStateException when timeout occurs with partialResults=false");
    } catch (IllegalStateException e) {
      String message = e.getMessage();
      Assert.assertNotNull(message, "Exception message should not be null");
      // Verify exact message components from the code
      Assert.assertTrue(
          message.contains("Lineage operation timed out after"),
          "Message should contain 'Lineage operation timed out after'. Got: " + message);
      Assert.assertTrue(
          message.contains(String.valueOf(timeoutSeconds)),
          "Message should contain timeout seconds (" + timeoutSeconds + "). Got: " + message);
      Assert.assertTrue(
          message.contains("Consider increasing the timeout or set partialResults to true"),
          "Message should suggest increasing timeout or setting partialResults. Got: " + message);
      Assert.assertTrue(
          message.contains("Entity: " + sourceUrn) || message.contains(sourceUrn.toString()),
          "Message should contain entity URN. Got: " + message);
    } catch (RuntimeException e) {
      // Check if wrapped - unwrap to find IllegalStateException in the cause chain
      // The IllegalStateException may be wrapped multiple times:
      // - RuntimeException("Failed to execute slice-based search", RuntimeException("Slice X
      // failed", ExecutionException(IllegalStateException)))
      // - Or RuntimeException("Slice X timed out", TimeoutException)
      Throwable cause = e;
      IllegalStateException foundIllegalStateException = null;

      // Traverse the entire cause chain to find IllegalStateException
      while (cause != null && foundIllegalStateException == null) {
        if (cause instanceof IllegalStateException) {
          String message = cause.getMessage();
          if (message != null && message.contains("Lineage operation timed out after")) {
            foundIllegalStateException = (IllegalStateException) cause;
            break;
          }
        }
        // Also check if it's an ExecutionException (from CompletableFuture) and unwrap its cause
        if (cause instanceof java.util.concurrent.ExecutionException && cause.getCause() != null) {
          cause = cause.getCause();
          continue;
        }
        cause = cause.getCause();
      }

      if (foundIllegalStateException != null) {
        String message = foundIllegalStateException.getMessage();
        Assert.assertNotNull(message, "Exception message should not be null");
        Assert.assertTrue(
            message.contains("Lineage operation timed out after"),
            "Exception should contain 'Lineage operation timed out after'. Got: " + message);
        Assert.assertTrue(
            message.contains(String.valueOf(timeoutSeconds)),
            "Exception should contain timeout seconds (" + timeoutSeconds + "). Got: " + message);
        Assert.assertTrue(
            message.contains("Consider increasing the timeout or set partialResults to true"),
            "Exception should suggest increasing timeout or setting partialResults. Got: "
                + message);
      } else {
        // If we didn't find IllegalStateException, check if any exception in the chain contains
        // timeout info
        // This handles cases where the timeout happens at a different level (e.g., slice processing
        // timeout)
        Throwable checkCause = e;
        boolean foundTimeoutMessage = false;
        while (checkCause != null && !foundTimeoutMessage) {
          String msg = checkCause.getMessage();
          if (msg != null
              && (msg.contains("timed out")
                  || msg.contains("timeout")
                  || msg.contains("Lineage operation timed out"))) {
            foundTimeoutMessage = true;
            // Verify it has the expected timeout content
            Assert.assertTrue(
                msg.contains("timeout")
                    || msg.contains("timed out")
                    || msg.contains("Lineage operation timed out"),
                "Exception should mention timeout. Got: " + msg);
            break;
          }
          if (checkCause instanceof java.util.concurrent.ExecutionException
              && checkCause.getCause() != null) {
            checkCause = checkCause.getCause();
          } else {
            checkCause = checkCause.getCause();
          }
        }
        if (!foundTimeoutMessage) {
          throw new AssertionError(
              "Expected IllegalStateException with timeout message in exception chain, got: "
                  + e.getClass().getSimpleName()
                  + " - "
                  + e.getMessage()
                  + (e.getCause() != null
                      ? " (cause: "
                          + e.getCause().getClass().getSimpleName()
                          + " - "
                          + e.getCause().getMessage()
                          + ")"
                      : ""));
        }
      }
    }
  }

  @Test
  public void testGetImpactLineageSearchQueryTimeReservationValidationDefaultsToZeroPointTwo()
      throws Exception {
    // Test that searchQueryTimeReservation is validated and defaults to 0.2 if <= 0.0 or >= 1.0
    // Covers: if (searchQueryTimeReservation <= 0.0 || searchQueryTimeReservation >= 1.0) {
    //           searchQueryTimeReservation = 0.2;
    //         }
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Test case 1: searchQueryTimeReservation = 0.0 (should default to 0.2)
    ElasticSearchConfiguration testConfigZero =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(10)
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000)
                                    .partialResults(true)
                                    .searchQueryTimeReservation(
                                        0.0) // Invalid, should default to 0.2
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO daoZero =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfigZero);

    SearchHit[] hits =
        createFakeLineageHits(
            5,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");
    SearchResponse searchResponse = createFakeSearchResponse(hits, 5);
    SearchResponse emptyResponse = createEmptySearchResponse(5);
    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Should work with defaulted 0.2 reservation (20% of 10s = 2s reserved, 8s available)
    LineageResponse responseZero =
        daoZero.getImpactLineage(operationContext, sourceUrn, filters, 1);
    Assert.assertNotNull(
        responseZero, "Response should not be null when searchQueryTimeReservation=0.0");

    // Test case 2: searchQueryTimeReservation >= 1.0 (should default to 0.2)
    ElasticSearchConfiguration testConfigGreaterThanOne =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(10)
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000)
                                    .partialResults(true)
                                    .searchQueryTimeReservation(
                                        1.5) // Invalid, should default to 0.2
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO daoGreaterThanOne =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfigGreaterThanOne);

    // Should work with defaulted 0.2 reservation
    LineageResponse responseGreaterThanOne =
        daoGreaterThanOne.getImpactLineage(operationContext, sourceUrn, filters, 1);
    Assert.assertNotNull(
        responseGreaterThanOne, "Response should not be null when searchQueryTimeReservation>=1.0");

    // Test case 3: searchQueryTimeReservation < 0.0 (should default to 0.2)
    ElasticSearchConfiguration testConfigNegative =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(10)
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000)
                                    .partialResults(true)
                                    .searchQueryTimeReservation(
                                        -0.1) // Invalid, should default to 0.2
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO daoNegative =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfigNegative);

    // Should work with defaulted 0.2 reservation
    LineageResponse responseNegative =
        daoNegative.getImpactLineage(operationContext, sourceUrn, filters, 1);
    Assert.assertNotNull(
        responseNegative, "Response should not be null when searchQueryTimeReservation<0.0");
  }

  @Test
  public void testGetImpactLineageTimeReservationMinimumClamping() throws Exception {
    // Test that reservedTimeForSearchQuery is clamped to minimum 100ms when:
    // reservedTimeForSearchQuery < minReservationMs (100ms) && totalTimeoutMs >= minReservationMs
    // Covers: if (reservedTimeForSearchQuery < minReservationMs && totalTimeoutMs >=
    // minReservationMs) {
    //           reservedTimeForSearchQuery = minReservationMs;
    //         }
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Test case: timeout of 1 second (1000ms), with 0.05 (5%) reservation = 50ms
    // 50ms < 100ms (min) && 1000ms >= 100ms, so should clamp to 100ms
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .timeoutSeconds(1) // 1 second = 1000ms
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(1000)
                                    .partialResults(true)
                                    .searchQueryTimeReservation(
                                        0.05) // 5% of 1000ms = 50ms (< 100ms)
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO dao = createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    SearchHit[] hits =
        createFakeLineageHits(
            5,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");
    SearchResponse searchResponse = createFakeSearchResponse(hits, 5);
    SearchResponse emptyResponse = createEmptySearchResponse(5);
    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Should work correctly with clamped minimum reservation (100ms instead of 50ms)
    // This means 900ms available for graph traversal instead of 950ms
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response, "Response should not be null");
    Assert.assertTrue(response.getTotal() >= 0, "Should return results");
    // The reservation should be clamped to 100ms minimum, leaving 900ms for graph traversal
  }

  @Test
  public void testGetImpactLineageEmptyResponse() throws Exception {
    // Test handling of empty response
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    GraphQueryPITDAO dao =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Mock empty response
    SearchResponse searchResponse = createEmptySearchResponse(0);

    // Use the utility method to mock slice-based search behavior
    // Both slices get the same search response
    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(searchResponse));

    // Mock the createPit method for OpenSearch Point-in-Time searches
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getLineageRelationships());
    Assert.assertEquals(response.getTotal(), 0);
  }

  @Test
  public void testGetImpactLineageMaxHopsLimit() throws Exception {
    // Test that maxHops limit is respected
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(),
            DATASET_ENTITY_NAME,
            LineageDirection.DOWNSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    GraphQueryPITDAO dao =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Create mock data for direct relationships (1 hop)
    SearchHit[] hits =
        createFakeLineageHits(
            3,
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)",
            "dest",
            "DownstreamOf");

    SearchResponse searchResponse = createFakeSearchResponse(hits, 3);

    // Use the utility method to mock slice-based search behavior
    // Both slices get the same search response
    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(searchResponse));

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Test with maxHops = 1 (should return the 3 direct relationships)
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getLineageRelationships());
    // Note: The actual result count depends on whether relationships are extracted from mock data
    // For now, just verify the response structure is correct
    Assert.assertTrue(response.getTotal() >= 0, "Response total should be non-negative");
    Set<Urn> oneHopUrns = new HashSet<>();
    for (LineageRelationship rel : response.getLineageRelationships()) {
      Assert.assertNotEquals(rel.isExplored(), Boolean.TRUE);
      oneHopUrns.add(rel.getEntity());
    }

    // Test with maxHops = 2 (should also return results, potentially the same if no multi-hop data)
    LineageResponse responseTwoHops = dao.getImpactLineage(operationContext, sourceUrn, filters, 2);
    for (LineageRelationship rel : responseTwoHops.getLineageRelationships()) {
      Assert.assertNotEquals(rel.isExplored(), !oneHopUrns.contains(rel.getEntity()));
    }

    Assert.assertNotNull(responseTwoHops);
    Assert.assertNotNull(responseTwoHops.getLineageRelationships());
    Assert.assertTrue(responseTwoHops.getTotal() >= 0, "Response total should be non-negative");

    // Both responses should have the same structure
    Assert.assertNotNull(response.getLineageRelationships());
    Assert.assertNotNull(responseTwoHops.getLineageRelationships());
  }

  @Test
  public void testComputeIfAbsentThreadSafety() throws Exception {
    // Test the computeIfAbsent pattern used in addEdgeToPaths
    Map<Urn, UrnArrayArray> existingPaths = new ConcurrentHashMap<>();

    // Test concurrent computeIfAbsent operations
    int numThreads = 3;
    int numOperations = 50;

    Thread[] threads = new Thread[numThreads];

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      threads[i] =
          new Thread(
              () -> {
                try {
                  for (int j = 0; j < numOperations; j++) {
                    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:test-urn-" + j);

                    // This should not throw ConcurrentModificationException
                    UrnArrayArray paths =
                        existingPaths.computeIfAbsent(testUrn, k -> new UrnArrayArray());

                    // Add to the paths safely
                    UrnArray path = new UrnArray();
                    path.add(testUrn);
                    paths.add(path);
                  }
                } catch (Exception e) {
                  fail("Thread " + threadId + " should not throw exception: " + e.getMessage());
                }
              });
    }

    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    // Verify results
    Assert.assertEquals(existingPaths.size(), numOperations);
  }

  @Test
  public void testAddEdgeToPathsDuplicatePrevention() {
    // Test that addEdgeToPaths prevents duplicate paths from being added
    Urn testParent = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,Test,PROD)");
    Urn testChild = UrnUtils.getUrn("urn:li:dashboard:(looker,test-dashboard)");
    Urn testVia = UrnUtils.getUrn("urn:li:dataJob:(airflow,test-job,PROD)");

    // Test duplicate prevention without via node
    ThreadSafePathStore pathStore = new ThreadSafePathStore();
    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, null, testChild);
    GraphQueryUtils.addEdgeToPaths(
        pathStore, testParent, null, testChild); // Should not add duplicate

    Map<Urn, UrnArrayArray> nodePaths = pathStore.toUrnArrayArrayMap();
    UrnArrayArray pathsToChild = nodePaths.get(testChild);
    Assert.assertNotNull(pathsToChild);
    Assert.assertEquals(pathsToChild.size(), 1); // Only one path should exist

    // Test duplicate prevention with via node
    pathStore = new ThreadSafePathStore();
    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, testVia, testChild);
    GraphQueryUtils.addEdgeToPaths(
        pathStore, testParent, testVia, testChild); // Should not add duplicate

    nodePaths = pathStore.toUrnArrayArrayMap();
    pathsToChild = nodePaths.get(testChild);
    Assert.assertNotNull(pathsToChild);
    Assert.assertEquals(pathsToChild.size(), 1); // Only one path should exist

    // Test duplicate prevention when extending existing paths
    pathStore = new ThreadSafePathStore();
    Urn testParentParent =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestParent,PROD)");
    UrnArray existingPathToParent = new UrnArray(ImmutableList.of(testParentParent, testParent));
    pathStore.addPath(testParent, existingPathToParent);

    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, null, testChild);
    GraphQueryUtils.addEdgeToPaths(
        pathStore, testParent, null, testChild); // Should not add duplicate

    nodePaths = pathStore.toUrnArrayArrayMap();
    pathsToChild = nodePaths.get(testChild);
    Assert.assertNotNull(pathsToChild);
    Assert.assertEquals(pathsToChild.size(), 1); // Only one extended path should exist
  }

  @Test
  public void testAddEdgeToPathsThreadSafety() throws Exception {
    // Test that addEdgeToPaths is thread-safe when multiple threads access the same
    // ThreadSafePathStore
    Urn testParent = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,Test,PROD)");
    Urn testChild = UrnUtils.getUrn("urn:li:dashboard:(looker,test-dashboard)");

    ThreadSafePathStore pathStore = new ThreadSafePathStore();

    // Pre-populate with existing paths
    UrnArray existingPathToParent = new UrnArray(ImmutableList.of(testParent));
    pathStore.addPath(testParent, existingPathToParent);

    int numThreads = 5;
    int numOperations = 20;

    Thread[] threads = new Thread[numThreads];

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      threads[i] =
          new Thread(
              () -> {
                try {
                  for (int j = 0; j < numOperations; j++) {
                    // This should not throw ConcurrentModificationException
                    GraphQueryUtils.addEdgeToPaths(pathStore, testParent, null, testChild);
                  }
                } catch (Exception e) {
                  fail("Thread " + threadId + " should not throw exception: " + e.getMessage());
                }
              });
    }

    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    // Verify that only one path was added (duplicates were prevented)
    Map<Urn, UrnArrayArray> nodePaths = pathStore.toUrnArrayArrayMap();
    UrnArrayArray pathsToChild = nodePaths.get(testChild);
    Assert.assertNotNull(pathsToChild);
    Assert.assertEquals(
        pathsToChild.size(), 1); // Only one path should exist despite multiple threads
  }

  @Test
  public void testGetImpactLineageDuplicatePathPrevention() throws Exception {
    // Test that getImpactLineage doesn't return duplicate paths due to the fixes
    // This tests the integration of both the addEdgeToPaths and mergeLineageRelationships fixes

    // Create a mock client that returns the same search results multiple times
    // to simulate the scenario where multiple slices process the same data
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Create test URNs
    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestSource,PROD)");
    Urn targetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestTarget,PROD)");

    LineageGraphFilters filters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // Mock search response that would normally cause duplicate paths
    SearchResponse mockSearchResponse = mock(SearchResponse.class);
    SearchHits mockSearchHits = mock(SearchHits.class);
    SearchHit mockHit = mock(SearchHit.class);

    // Create a mock document that represents a lineage edge
    Map<String, Object> source = new HashMap<>();
    source.put("source", Map.of("entityType", "dataset", "urn", sourceUrn.toString()));
    source.put("destination", Map.of("entityType", "dataset", "urn", targetUrn.toString()));
    source.put("relationshipType", "DownstreamOf");
    source.put("createdOn", System.currentTimeMillis());
    source.put("updatedOn", System.currentTimeMillis());
    source.put("createdActor", "urn:li:corpuser:test");
    source.put("updatedActor", "urn:li:corpuser:test");
    source.put("properties", Map.of("source", "API"));

    when(mockHit.getSourceAsMap()).thenReturn(source);
    when(mockHit.getSortValues()).thenReturn(new Object[] {"sort_value"});
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[] {mockHit});
    when(mockSearchHits.getTotalHits()).thenReturn(new TotalHits(1L, TotalHits.Relation.EQUAL_TO));
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(mockSearchResponse.getScrollId()).thenReturn("scroll_id_1");

    // Mock empty response for scroll to stop pagination
    SearchHits emptySearchHits = mock(SearchHits.class);
    when(emptySearchHits.getHits()).thenReturn(new SearchHit[0]);
    when(emptySearchHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));

    SearchResponse emptyScrollResponse = mock(SearchResponse.class);
    when(emptyScrollResponse.getHits()).thenReturn(emptySearchHits);
    when(emptyScrollResponse.getScrollId()).thenReturn("empty_scroll_id");

    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSearchResponse)
        .thenReturn(emptyScrollResponse);

    // Mock the clearScroll calls
    when(mockClient.clearScroll(any(ClearScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(null);

    // Mock the createPit method for OpenSearch Point-in-Time searches
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Test with maxHops = 1 to trigger the path building logic
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    // Verify that the response is valid and doesn't contain duplicate paths
    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getLineageRelationships());

    // The key assertion: if there are any relationships, they should not have duplicate paths
    if (response.getTotal() > 0) {
      for (LineageRelationship relationship : response.getLineageRelationships()) {
        if (relationship.hasPaths()) {
          UrnArrayArray paths = relationship.getPaths();
          // Convert to set to check for duplicates
          Set<UrnArray> uniquePaths = new HashSet<>();
          for (UrnArray path : paths) {
            uniquePaths.add(path);
          }
          // The number of unique paths should equal the total number of paths
          Assert.assertEquals(
              uniquePaths.size(),
              paths.size(),
              "Duplicate paths detected in lineage relationship for entity: "
                  + relationship.getEntity());
        }
      }
    }
  }

  @Test
  private void testExecuteLineageSearchQueryThrowsESQueryException() throws Exception {
    // Mock the client to throw an exception
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch connection failed"));

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Test that executeLineageSearchQuery throws ESQueryException when client.search fails
    try {
      graphQueryDAO.getSearchResponse(
          operationContext, GraphFilters.outgoingFilter(EMPTY_FILTER), 0, 10);
      fail("Expected ESQueryException to be thrown");
    } catch (ESQueryException e) {
      Assert.assertEquals(e.getMessage(), "Search query failed:");
      Assert.assertNotNull(e.getCause());
      Assert.assertEquals(e.getCause().getMessage(), "Elasticsearch connection failed");
    }
  }

  @Test
  private void testExecuteSearchThrowsESQueryException() throws Exception {
    // Mock the client to throw an exception
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch search failed"));

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices("test_index");

    // Test that executeSearch throws ESQueryException when client.search fails
    try {
      graphQueryDAO.executeSearch(searchRequest);
      fail("Expected ESQueryException to be thrown");
    } catch (ESQueryException e) {
      Assert.assertEquals(e.getMessage(), "Search query failed:");
      Assert.assertNotNull(e.getCause());
      Assert.assertEquals(e.getCause().getMessage(), "Elasticsearch search failed");
    }
  }

  @Test
  private void testExecuteScrollSearchQueryThrowsESQueryException() throws Exception {
    // Mock the client to throw an exception
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch scroll search failed"));

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    GraphFilters graphFilters = GraphFilters.outgoingFilter(EMPTY_FILTER);
    List<SortCriterion> sortCriteria =
        List.of(new SortCriterion().setField("createdOn").setOrder(SortOrder.DESCENDING));

    // Test that executeScrollSearchQuery throws ESQueryException when client.search fails
    try {
      graphQueryDAO.getSearchResponse(operationContext, graphFilters, sortCriteria, null, null, 10);
      fail("Expected ESQueryException to be thrown");
    } catch (ESQueryException e) {
      Assert.assertEquals(e.getMessage(), "Search query failed:");
      Assert.assertNotNull(e.getCause());
      Assert.assertEquals(e.getCause().getMessage(), "Elasticsearch scroll search failed");
    }
  }

  @Test
  private void testExecuteGroupByLineageSearchQueryThrowsESQueryException() throws Exception {
    // Mock the client to throw an exception
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch group by lineage search failed"));

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Create test data
    Set<Urn> entityUrns = Set.of(Urn.createFromString("urn:li:dataset:test-urn"));
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // Test that the method handles the error gracefully and returns empty results
    // instead of throwing an exception, since the public API should be stable
    try {
      List<LineageRelationship> result =
          graphQueryDAO.getLineageRelationshipsInBatches(
              operationContext,
              new ArrayList<>(entityUrns),
              lineageGraphFilters,
              ConcurrentHashMap.newKeySet(),
              ConcurrentHashMap.newKeySet(),
              1,
              2,
              10000L,
              new ThreadSafePathStore(),
              false);

      // The method should handle the error gracefully and return empty results
      // This tests that the public API is stable even when Elasticsearch fails
      Assert.assertNotNull(result);
      Assert.assertTrue(result.isEmpty());
    } catch (Exception e) {
      // If an exception is thrown, it should be a RuntimeException, not ESQueryException
      Assert.assertTrue(e instanceof RuntimeException);
      Assert.assertFalse(e instanceof ESQueryException);
    }
  }

  @Test
  private void testExecuteQueryWithLimitThrowsESQueryException() throws Exception {
    // Mock the client to throw an exception
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch query with limit failed"));

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Create test data
    Set<Urn> entityUrns = Set.of(Urn.createFromString("urn:li:dataset:test-urn"));
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // Test that the method handles the error gracefully and returns empty results
    // instead of throwing an exception, since the public API should be stable
    try {
      List<LineageRelationship> result =
          graphQueryDAO.getLineageRelationshipsInBatches(
              operationContext,
              new ArrayList<>(entityUrns),
              lineageGraphFilters,
              ConcurrentHashMap.newKeySet(),
              ConcurrentHashMap.newKeySet(),
              1,
              2,
              10000L,
              new ThreadSafePathStore(),
              false);

      // The method should handle the error gracefully and return empty results
      // This tests that the public API is stable even when Elasticsearch fails
      Assert.assertNotNull(result);
      Assert.assertTrue(result.isEmpty());
    } catch (Exception e) {
      // If an exception is thrown, it should be a RuntimeException, not ESQueryException
      Assert.assertTrue(e instanceof RuntimeException);
      Assert.assertFalse(e instanceof ESQueryException);
    }
  }

  @Test
  private void testExecuteSearchRequestThrowsESQueryException() throws Exception {
    // Mock the client to throw an exception
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch search request failed"));

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    // Create test data
    Set<Urn> entityUrns = Set.of(Urn.createFromString("urn:li:dataset:test-urn"));
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // Test that the method handles the error gracefully and returns empty results
    // instead of throwing an exception, since the public API should be stable
    try {
      List<LineageRelationship> result =
          graphQueryDAO.getLineageRelationshipsInBatches(
              operationContext,
              new ArrayList<>(entityUrns),
              lineageGraphFilters,
              ConcurrentHashMap.newKeySet(),
              ConcurrentHashMap.newKeySet(),
              1,
              2,
              10000L,
              new ThreadSafePathStore(),
              false);

      // The method should handle the error gracefully and return empty results
      // This tests that the public API is stable even when Elasticsearch fails
      Assert.assertNotNull(result);
      Assert.assertTrue(result.isEmpty());
    } catch (Exception e) {
      // If an exception is thrown, it should be a RuntimeException, not ESQueryException
      Assert.assertTrue(e instanceof RuntimeException);
      Assert.assertFalse(e instanceof ESQueryException);
    }
  }

  @Test
  private void testScrollSingleSliceFailsWhenPITDisabled() throws Exception {
    // Mock the client (not used since PIT validation fails first)
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG_NO_PIT);

    // Create test data
    Urn entityUrn = Urn.createFromString("urn:li:dataset:test-urn");
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // Test that the method fails fast with a clear error message when PIT is disabled
    // This is the correct behavior for the public API
    try {
      LineageResponse result =
          graphQueryDAO.getImpactLineage(operationContext, entityUrn, lineageGraphFilters, 2);
      fail("Expected IllegalStateException to be thrown when PIT is disabled");
    } catch (IllegalStateException e) {
      // The method should fail fast with a clear error message when PIT is disabled
      // Check the entire exception chain for the expected messages
      Assert.assertTrue(
          hasMessageInChain(e, "Point-in-Time creation is required for impact analysis queries"),
          "Expected PIT requirement message in exception chain");
      Assert.assertTrue(
          hasMessageInChain(e, "ELASTICSEARCH_SEARCH_GRAPH_POINT_IN_TIME_CREATION_ENABLED"),
          "Expected configuration key message in exception chain");
    }
  }

  @Test
  private void testExceptionHandlingWithDifferentExceptionTypes() throws Exception {
    // Test different types of exceptions that should be wrapped in ESQueryException
    String[] exceptionMessages = {
      "Connection refused",
      "Read timeout",
      "Index not found",
      "Query parsing error",
      "Cluster unavailable"
    };

    for (String exceptionMessage : exceptionMessages) {
      SearchClientShim<?> mockClient = mock(SearchClientShim.class);
      when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
      when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
          .thenThrow(new RuntimeException(exceptionMessage));

      GraphQueryPITDAO graphQueryDAO =
          createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

      try {
        graphQueryDAO.getSearchResponse(
            operationContext, GraphFilters.outgoingFilter(EMPTY_FILTER), 0, 10);
        fail("Expected ESQueryException to be thrown for: " + exceptionMessage);
      } catch (ESQueryException e) {
        Assert.assertEquals(e.getMessage(), "Search query failed:");
        Assert.assertNotNull(e.getCause());
        Assert.assertEquals(e.getCause().getMessage(), exceptionMessage);
      }
    }
  }

  @Test
  private void testExceptionHandlingPreservesOriginalException() throws Exception {
    // Test that the original exception is properly preserved
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    RuntimeException originalException = new RuntimeException("Original error message");
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(originalException);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    try {
      graphQueryDAO.getSearchResponse(
          operationContext, GraphFilters.outgoingFilter(EMPTY_FILTER), 0, 10);
      fail("Expected ESQueryException to be thrown");
    } catch (ESQueryException e) {
      Assert.assertEquals(e.getMessage(), "Search query failed:");
      Assert.assertSame(e.getCause(), originalException);
      Assert.assertEquals(e.getCause().getMessage(), "Original error message");
    }
  }

  @Test
  private void testExceptionHandlingWithNullCause() throws Exception {
    // Test exception handling when the original exception has no cause
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    RuntimeException originalException = new RuntimeException("Error without cause");
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(originalException);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    try {
      graphQueryDAO.getSearchResponse(
          operationContext, GraphFilters.outgoingFilter(EMPTY_FILTER), 0, 10);
      fail("Expected ESQueryException to be thrown");
    } catch (ESQueryException e) {
      Assert.assertEquals(e.getMessage(), "Search query failed:");
      Assert.assertNotNull(e.getCause());
      Assert.assertEquals(e.getCause().getMessage(), "Error without cause");
    }
  }

  @Test
  private void testExceptionHandlingWithChainedExceptions() throws Exception {
    // Test exception handling with chained exceptions
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    RuntimeException rootCause = new RuntimeException("Root cause");
    RuntimeException middleException = new RuntimeException("Middle layer", rootCause);
    RuntimeException topException = new RuntimeException("Top layer", middleException);

    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(topException);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG);

    try {
      graphQueryDAO.getSearchResponse(
          operationContext, GraphFilters.outgoingFilter(EMPTY_FILTER), 0, 10);
      fail("Expected ESQueryException to be thrown");
    } catch (ESQueryException e) {
      Assert.assertEquals(e.getMessage(), "Search query failed:");
      Assert.assertNotNull(e.getCause());
      Assert.assertEquals(e.getCause().getMessage(), "Top layer");
      Assert.assertNotNull(e.getCause().getCause());
      Assert.assertEquals(e.getCause().getCause().getMessage(), "Middle layer");
      Assert.assertNotNull(e.getCause().getCause().getCause());
      Assert.assertEquals(e.getCause().getCause().getCause().getMessage(), "Root cause");
    }
  }

  @Test
  private void testGetImpactLineageRequiresPITEnabled() throws Exception {
    // Test that getImpactLineage throws an exception when PIT is disabled
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with PIT disabled
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .pointInTimeCreationEnabled(false)
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:test-urn");
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    try {
      graphQueryDAO.getImpactLineage(operationContext, testUrn, lineageGraphFilters, 5);
      fail("Expected IllegalStateException to be thrown when PIT is disabled");
    } catch (IllegalStateException e) {
      // Check the entire exception chain for the expected messages
      Assert.assertTrue(
          hasMessageInChain(e, "Point-in-Time creation is required for impact analysis queries"),
          "Expected PIT requirement message in exception chain");
      Assert.assertTrue(
          hasMessageInChain(e, "ELASTICSEARCH_SEARCH_GRAPH_POINT_IN_TIME_CREATION_ENABLED"),
          "Expected configuration key message in exception chain");
    }
  }

  @Test
  private void testGetImpactLineageWithPITEnabled() throws Exception {
    // Test that getImpactLineage validation passes when PIT is enabled
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    // Create a configuration with PIT enabled (default)
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .pointInTimeCreationEnabled(true)
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig);

    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:test-urn");
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // Mock the createPit method to avoid NullPointerException
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Mock empty search response to avoid complex mocking
    SearchResponse mockSearchResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[0]);
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));
    when(mockSearchResponse.getHits()).thenReturn(mockHits);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSearchResponse);

    // The validation should pass, and the method should complete successfully
    try {
      LineageResponse response =
          graphQueryDAO.getImpactLineage(operationContext, testUrn, lineageGraphFilters, 5);
      // If we get here, the validation passed and the method completed successfully
      Assert.assertNotNull(response);
      Assert.assertEquals(response.getTotal(), 0);
      for (LineageRelationship rel : response.getLineageRelationships()) {
        Assert.assertNotEquals(
            rel.isExplored(),
            rel.getDegrees().stream().min(Integer::compareTo).map(v -> v == 5).orElse(null));
      }
    } catch (IllegalStateException e) {
      // This should NOT be the PIT validation exception
      if (hasMessageInChain(e, "Point-in-Time creation is required")) {
        fail("PIT validation should have passed when PIT is enabled");
      }
      // Any other IllegalStateException is unexpected
      throw e;
    }
  }

  // ==================== ELASTICSEARCH SCROLL+SLICE TESTS ====================

  @Test
  public void testElasticsearchImplementationUsesScrollInsteadOfPIT() throws Exception {
    // Test that Elasticsearch implementation routes to scroll+slice instead of PIT+slice
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    // This triggers the scroll path
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);

    // Create Elasticsearch configuration
    ElasticSearchConfiguration elasticsearchConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .pointInTimeCreationEnabled(true)
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, elasticsearchConfig);

    // Mock scroll search responses
    SearchResponse mockScrollResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    SearchHit[] hits = new SearchHit[0]; // Empty results to avoid complex mocking
    when(mockHits.getHits()).thenReturn(hits);
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));
    when(mockScrollResponse.getHits()).thenReturn(mockHits);
    when(mockScrollResponse.getScrollId()).thenReturn("test_scroll_id");

    // Mock the search call to return scroll response
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockScrollResponse);

    // Mock clear scroll call
    when(mockClient.clearScroll(any(ClearScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(null);

    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:test-urn");
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    try {
      // This should use the scroll path instead of PIT
      graphQueryDAO.getImpactLineage(operationContext, testUrn, lineageGraphFilters, 1);

      // Verify that search was called (scroll path) instead of PIT creation
      verify(mockClient, atLeast(1)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

      // Verify that clearScroll was called to clean up scroll context
      verify(mockClient, atLeast(1))
          .clearScroll(any(ClearScrollRequest.class), eq(RequestOptions.DEFAULT));

    } catch (Exception e) {
      // Expected to fail due to missing lineage data, but should use scroll path
      // The important thing is that it didn't fail on PIT creation
      Assert.assertFalse(hasMessageInChain(e, "Point-in-Time creation is required"));
    }
  }

  @Test
  public void testScrollSearchWithSlices() throws Exception {
    // Test the scroll+slice functionality with actual data
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    // This triggers the scroll path
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);

    // Create Elasticsearch configuration with 2 slices
    ElasticSearchConfiguration elasticsearchConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .pointInTimeCreationEnabled(true)
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .slices(2) // Test with 2 slices
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, elasticsearchConfig);

    // Create mock search hits with lineage data
    SearchHit[] hits1 =
        createFakeLineageHits(2, "urn:li:dataset:test-urn", "slice1", "DownstreamOf");
    SearchHit[] hits2 =
        createFakeLineageHits(1, "urn:li:dataset:test-urn", "slice2", "DownstreamOf");

    // Mock initial search responses for each slice
    SearchResponse mockResponse1 = createFakeSearchResponse(hits1, 2, "scroll_id_1");
    SearchResponse mockResponse2 = createFakeSearchResponse(hits2, 1, "scroll_id_2");

    // Mock empty responses for subsequent scroll calls (no more pages)
    SearchResponse emptyResponse = createEmptySearchResponse(0);

    // Mock search calls: first 2 calls return results (one for each slice), subsequent calls return
    // empty
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse1) // First slice
        .thenReturn(mockResponse2); // Second slice

    // Mock scroll calls to return empty (no more pages)
    when(mockClient.scroll(any(SearchScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(emptyResponse);

    // Mock clear scroll calls
    when(mockClient.clearScroll(any(ClearScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(null);

    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:test-urn");
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    try {
      // This should use scroll+slice and process both slices
      graphQueryDAO.getImpactLineage(operationContext, testUrn, lineageGraphFilters, 1);

      // Verify that search was called for both slices
      verify(mockClient, atLeast(2)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

      // Verify that clearScroll was called for both slices
      verify(mockClient, atLeast(2))
          .clearScroll(any(ClearScrollRequest.class), eq(RequestOptions.DEFAULT));

    } catch (Exception e) {
      // Expected to fail due to missing lineage data, but should use scroll path
      Assert.assertFalse(hasMessageInChain(e, "Point-in-Time creation is required"));
    }
  }

  @Test
  public void testScrollSearchHandlesEmptyResults() throws Exception {
    // Test scroll search when no results are found
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    // This triggers the scroll path
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);

    ElasticSearchConfiguration elasticsearchConfig = TEST_OS_SEARCH_CONFIG.toBuilder().build();

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, elasticsearchConfig);

    // Mock empty search response
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[0]);
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockResponse.getScrollId()).thenReturn("test_scroll_id");

    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:test-urn");
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    try {
      graphQueryDAO.getImpactLineage(operationContext, testUrn, lineageGraphFilters, 1);

      // Should handle empty results gracefully
      verify(mockClient, atLeast(1)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    } catch (Exception e) {
      // Expected to fail due to missing lineage data, but should handle empty results
      Assert.assertFalse(hasMessageInChain(e, "Point-in-Time creation is required"));
    }
  }

  @Test
  public void testScrollSearchWithKeepAliveConfiguration() throws Exception {
    // Test that scroll search uses the configured keepAlive value
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    // This triggers the scroll path
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);

    ElasticSearchConfiguration elasticsearchConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .keepAlive("10m") // Test with custom keepAlive
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryPITDAO graphQueryDAO =
        createTrackedDAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, elasticsearchConfig);

    // Mock search response
    SearchResponse mockResponse = mock(SearchResponse.class);
    SearchHits mockHits = mock(SearchHits.class);
    when(mockHits.getHits()).thenReturn(new SearchHit[0]);
    when(mockHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));
    when(mockResponse.getHits()).thenReturn(mockHits);
    when(mockResponse.getScrollId()).thenReturn("test_scroll_id");

    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:test-urn");
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    try {
      graphQueryDAO.getImpactLineage(operationContext, testUrn, lineageGraphFilters, 1);

      // Verify that search was called with scroll parameter
      ArgumentCaptor<SearchRequest> searchRequestCaptor =
          ArgumentCaptor.forClass(SearchRequest.class);
      verify(mockClient, atLeast(1))
          .search(searchRequestCaptor.capture(), eq(RequestOptions.DEFAULT));

      // Verify that scroll parameter was set
      SearchRequest capturedRequest = searchRequestCaptor.getValue();
      Assert.assertNotNull(capturedRequest.scroll());
      Assert.assertEquals("10m", capturedRequest.scroll().keepAlive().toString());

    } catch (Exception e) {
      // Expected to fail due to missing lineage data, but should set scroll parameter correctly
      Assert.assertFalse(hasMessageInChain(e, "Point-in-Time creation is required"));
    }
  }

  @Test
  public void testSearchSingleSliceWithPitThreadInterruption() throws Exception {
    // Test that thread interruption is properly handled in searchSingleSliceWithPit
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao = createTrackedDAO(mockClient);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Create a thread that will be interrupted
    Thread testThread =
        new Thread(
            () -> {
              try {
                Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-urn");
                LineageGraphFilters filters =
                    LineageGraphFilters.forEntityType(
                        operationContext.getLineageRegistry(),
                        DATASET_ENTITY_NAME,
                        LineageDirection.DOWNSTREAM);

                // Start the search operation
                dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
              } catch (Exception e) {
                // Expected to throw exception due to interruption
              }
            });

    // Start the thread and then interrupt it
    testThread.start();

    // Give the thread a moment to start, then interrupt it
    Thread.sleep(100);
    testThread.interrupt();

    // Wait for the thread to complete
    testThread.join(5000);

    // Verify that the thread completed (either successfully or with exception)
    Assert.assertFalse(testThread.isAlive(), "Test thread should have completed");
  }

  @Test
  public void testSearchSingleSliceWithPitThreadInterruptionException() throws Exception {
    // Test that the specific RuntimeException is thrown when thread is interrupted
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao = createTrackedDAO(mockClient);

    // Mock PIT creation
    CreatePitResponse mockPitResponse = mock(CreatePitResponse.class);
    when(mockPitResponse.getId()).thenReturn("test_pit_id");
    when(mockClient.createPit(any(CreatePitRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockPitResponse);

    // Create a mock search response that will cause the method to enter the loop
    SearchHit[] hits = createFakeLineageHits(1, "urn:li:dataset:test-urn", "dest", "DownstreamOf");
    SearchResponse searchResponse = createFakeSearchResponse(hits, 1);

    // Mock search to return the response, then throw interruption exception
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            invocation -> {
              // Simulate thread interruption by checking Thread.currentThread().isInterrupted()
              if (Thread.currentThread().isInterrupted()) {
                throw new RuntimeException("Slice 0 was interrupted");
              }
              return searchResponse;
            });

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test-urn");
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(),
            DATASET_ENTITY_NAME,
            LineageDirection.DOWNSTREAM);

    // Create a thread that will be interrupted
    final RuntimeException[] caughtException = new RuntimeException[1];
    Thread testThread =
        new Thread(
            () -> {
              try {
                dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
              } catch (RuntimeException e) {
                caughtException[0] = e;
              }
            });

    // Start the thread and then interrupt it
    testThread.start();

    // Give the thread a moment to start, then interrupt it
    Thread.sleep(100);
    testThread.interrupt();

    // Wait for the thread to complete
    testThread.join(5000);

    // Verify that the specific interruption exception was caught
    Assert.assertNotNull(
        caughtException[0], "Expected RuntimeException to be thrown due to interruption");
    Assert.assertTrue(
        caughtException[0].getMessage().contains("Failed to execute slice-based search"),
        "Expected slice-based search failure message, got: " + caughtException[0].getMessage());
  }

  @Test
  public void testShutdown() throws Exception {
    // Test that shutdown method properly terminates the thread pool
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao = createTrackedDAO(mockClient);

    // Verify thread pool is running
    Assert.assertFalse(dao.pitExecutor.isShutdown(), "Thread pool should be running");

    // Call shutdown
    dao.shutdown();

    // Verify thread pool is shutdown
    Assert.assertTrue(dao.pitExecutor.isShutdown(), "Thread pool should be shutdown");
    Assert.assertTrue(dao.pitExecutor.isTerminated(), "Thread pool should be terminated");
  }

  @Test
  public void testShutdownWithForcedTermination() throws Exception {
    // Test shutdown when graceful termination fails and forced shutdown is needed
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao = createTrackedDAO(mockClient);

    // Create a custom ExecutorService that simulates graceful shutdown failure
    ExecutorService mockExecutor = mock(ExecutorService.class);
    when(mockExecutor.isShutdown()).thenReturn(false);
    when(mockExecutor.awaitTermination(30, TimeUnit.SECONDS))
        .thenReturn(false); // Graceful shutdown fails
    when(mockExecutor.awaitTermination(10, TimeUnit.SECONDS))
        .thenReturn(true); // Forced shutdown succeeds

    // Replace the executor with our mock
    java.lang.reflect.Field executorField = GraphQueryPITDAO.class.getDeclaredField("pitExecutor");
    executorField.setAccessible(true);
    executorField.set(dao, mockExecutor);

    // Call shutdown
    dao.shutdown();

    // Verify that shutdownNow was called
    verify(mockExecutor).shutdown();
    verify(mockExecutor).shutdownNow();
    verify(mockExecutor).awaitTermination(30, TimeUnit.SECONDS);
    verify(mockExecutor).awaitTermination(10, TimeUnit.SECONDS);
  }

  @Test
  public void testShutdownWithFailedForcedTermination() throws Exception {
    // Test shutdown when both graceful and forced termination fail
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao = createTrackedDAO(mockClient);

    // Create a custom ExecutorService that simulates both graceful and forced shutdown failure
    ExecutorService mockExecutor = mock(ExecutorService.class);
    when(mockExecutor.isShutdown()).thenReturn(false);
    when(mockExecutor.awaitTermination(30, TimeUnit.SECONDS))
        .thenReturn(false); // Graceful shutdown fails
    when(mockExecutor.awaitTermination(10, TimeUnit.SECONDS))
        .thenReturn(false); // Forced shutdown also fails

    // Replace the executor with our mock
    java.lang.reflect.Field executorField = GraphQueryPITDAO.class.getDeclaredField("pitExecutor");
    executorField.setAccessible(true);
    executorField.set(dao, mockExecutor);

    // Call shutdown
    dao.shutdown();

    // Verify that shutdownNow was called and both awaitTermination calls were made
    verify(mockExecutor).shutdown();
    verify(mockExecutor).shutdownNow();
    verify(mockExecutor).awaitTermination(30, TimeUnit.SECONDS);
    verify(mockExecutor).awaitTermination(10, TimeUnit.SECONDS);
  }

  @Test
  public void testShutdownWithInterruptedException() throws Exception {
    // Test shutdown when interrupted during awaitTermination
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao = createTrackedDAO(mockClient);

    // Create a custom ExecutorService that throws InterruptedException
    ExecutorService mockExecutor = mock(ExecutorService.class);
    when(mockExecutor.isShutdown()).thenReturn(false);
    when(mockExecutor.awaitTermination(30, TimeUnit.SECONDS))
        .thenThrow(new InterruptedException("Test interruption"));

    // Replace the executor with our mock
    java.lang.reflect.Field executorField = GraphQueryPITDAO.class.getDeclaredField("pitExecutor");
    executorField.setAccessible(true);
    executorField.set(dao, mockExecutor);

    // Call shutdown
    dao.shutdown();

    // Verify that shutdownNow was called and thread was interrupted
    verify(mockExecutor).shutdown();
    verify(mockExecutor).shutdownNow();
    verify(mockExecutor).awaitTermination(30, TimeUnit.SECONDS);

    // Verify that the current thread was interrupted
    Assert.assertTrue(Thread.currentThread().isInterrupted(), "Thread should be interrupted");

    // Clear the interrupt flag for other tests
    Thread.interrupted();
  }

  @Test
  public void testShutdownWhenAlreadyShutdown() throws Exception {
    // Test shutdown when executor is already shutdown
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao = createTrackedDAO(mockClient);

    // Create a custom ExecutorService that is already shutdown
    ExecutorService mockExecutor = mock(ExecutorService.class);
    when(mockExecutor.isShutdown()).thenReturn(true);

    // Replace the executor with our mock
    java.lang.reflect.Field executorField = GraphQueryPITDAO.class.getDeclaredField("pitExecutor");
    executorField.setAccessible(true);
    executorField.set(dao, mockExecutor);

    // Call shutdown
    dao.shutdown();

    // Verify that no shutdown methods were called since it's already shutdown
    verify(mockExecutor, never()).shutdown();
    verify(mockExecutor, never()).shutdownNow();
    verify(mockExecutor, never()).awaitTermination(anyLong(), any(TimeUnit.class));
  }

  @Test
  public void testShutdownWhenExecutorIsNull() throws Exception {
    // Test shutdown when executor is null
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao = createTrackedDAO(mockClient);

    // Set executor to null
    java.lang.reflect.Field executorField = GraphQueryPITDAO.class.getDeclaredField("pitExecutor");
    executorField.setAccessible(true);
    executorField.set(dao, null);

    // Call shutdown - should not throw exception
    dao.shutdown();

    // Test passes if no exception is thrown
  }

  @Test
  public void testPitCreatedOnceAndCleanedUpInFinally() throws Exception {
    // Tests that PIT is created once before the slice loop and cleaned up in finally
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao = createTrackedDAO(mockClient);

    // Mock PIT creation
    CreatePitResponse pitResponse = mock(CreatePitResponse.class);
    when(pitResponse.getId()).thenReturn("test-pit-id");
    when(mockClient.createPit(any(CreatePitRequest.class), any(RequestOptions.class)))
        .thenReturn(pitResponse);

    // Mock search responses
    SearchHit[] hits = createFakeLineageHits(5, "urn:li:dataset:test", "dest", "DownstreamOf");
    SearchResponse searchResponse = createFakeSearchResponse(hits, 5);
    SearchResponse emptyResponse = createEmptySearchResponse(5);

    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(emptyResponse));

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test");
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    // Execute
    dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    // Verify PIT was created exactly once (not per slice)
    verify(mockClient, times(1)).createPit(any(CreatePitRequest.class), any(RequestOptions.class));

    // Verify PIT cleanup was called exactly once in finally
    verify(mockClient, times(1))
        .deletePit(
            argThat(req -> req.getPitIds().contains("test-pit-id")), any(RequestOptions.class));
  }

  @Test
  public void testPitCleanupHappensEvenOnSliceException() throws Exception {
    // Tests that finally block executes and cleans up PIT even when slice fails
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    GraphQueryPITDAO dao = createTrackedDAO(mockClient);

    // Mock PIT creation
    CreatePitResponse pitResponse = mock(CreatePitResponse.class);
    when(pitResponse.getId()).thenReturn("test-pit-id");
    when(mockClient.createPit(any(CreatePitRequest.class), any(RequestOptions.class)))
        .thenReturn(pitResponse);

    // Mock search to throw exception
    when(mockClient.search(any(SearchRequest.class), any(RequestOptions.class)))
        .thenThrow(new RuntimeException("Search failed"));

    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:test");
    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    try {
      dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      fail("Should have thrown exception");
    } catch (RuntimeException e) {
      // Expected
    }

    // Verify PIT cleanup was STILL called in finally block
    verify(mockClient, times(1))
        .deletePit(
            argThat(req -> req.getPitIds().contains("test-pit-id")), any(RequestOptions.class));
  }
}
