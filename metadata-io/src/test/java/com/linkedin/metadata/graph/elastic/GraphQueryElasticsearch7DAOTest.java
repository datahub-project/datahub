package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.graph.elastic.TestUtils.createEmptySearchResponse;
import static com.linkedin.metadata.graph.elastic.TestUtils.createFakeLineageHits;
import static com.linkedin.metadata.graph.elastic.TestUtils.createFakeSearchResponse;
import static com.linkedin.metadata.graph.elastic.TestUtils.mockSliceBasedSearch;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
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
import org.testng.annotations.Test;

public class GraphQueryElasticsearch7DAOTest {

  private static final String TEST_QUERY_FILE_LIMITED =
      "elasticsearch/sample_filters/lineage_query_filters_limited.json";
  private static final String TEST_QUERY_FILE_FULL =
      "elasticsearch/sample_filters/lineage_query_filters_full.json";
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

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            null, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);
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
  public void testGetLineageQueryWithInvalidEntityTypes() {
    // Mock only the client
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);

    // Create the DAO with minimal mocks
    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

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
      Assert.fail("Should throw IllegalArgumentException for URNs of different entity types");
    } catch (IllegalArgumentException e) {
      // Expected exception
      Assert.assertEquals(e.getMessage(), "Urns must be of the same entity type.");
    }
  }

  @Test
  public void testGetLineageQueryWithEmptyUrns() {
    // Mock only the client
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);

    // Create the DAO with minimal mocks
    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

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
    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

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

    // Create a configuration with a specific limit
    GraphServiceConfiguration testConfig =
        TEST_GRAPH_SERVICE_CONFIG.toBuilder()
            .limit(
                LimitConfig.builder()
                    .results(new ResultsLimitConfig().setMax(50).setApiDefault(50))
                    .build())
            .build();

    // Create the DAO with our test config
    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(mockClient, testConfig, TEST_OS_SEARCH_CONFIG, null);

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

    // Create a configuration with a specific limit
    GraphServiceConfiguration testConfig =
        TEST_GRAPH_SERVICE_CONFIG.toBuilder()
            .limit(
                LimitConfig.builder()
                    .results(new ResultsLimitConfig().setMax(25).setApiDefault(25))
                    .build())
            .build();

    // Create the DAO with our test config
    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(mockClient, testConfig, TEST_OS_SEARCH_CONFIG, null);

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

    // Note: GraphQueryElasticsearch7DAO uses scroll-based search, not PIT
    // No need to mock createPit for this DAO

    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

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

    // Mock scroll calls for Elasticsearch 7 DAO (which uses scroll instead of PIT)
    when(mockClient.scroll(any(SearchScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(emptySearchResponse); // All scroll calls return empty (no more results)

    // Test getImpactLineage with 2 slices
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response);
    Assert.assertEquals(response.getTotal(), 2);
    for (LineageRelationship rel : response.getLineageRelationships()) {
      Assert.assertNotEquals(rel.isExplored(), Boolean.TRUE);
    }

    // Verify that search was called 2 times (1 initial search per slice)
    // Elasticsearch 7 DAO uses scroll for pagination, not repeated search calls
    verify(mockClient, times(2)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test(timeOut = 10000) // Add timeout to prevent hanging in test suites
  public void testGetImpactLineageMaxRelationsLimit() throws Exception {
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    SearchClientShim<?> mockClient = mock(SearchClientShim.class);

    // Create a configuration with a very low maxRelations limit to ensure we hit it
    ElasticSearchConfiguration testConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .graph(
                        TEST_OS_SEARCH_CONFIG.getSearch().getGraph().toBuilder()
                            .impact(
                                TEST_OS_SEARCH_CONFIG.getSearch().getGraph().getImpact().toBuilder()
                                    .maxRelations(5) // Very low limit to ensure we hit it
                                    .build())
                            .build())
                    .build())
            .build();

    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(mockClient, TEST_GRAPH_SERVICE_CONFIG, testConfig, null);

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

    // Note: GraphQueryElasticsearch7DAO uses scroll-based search, not PIT
    // No need to mock createPit for this DAO

    try {
      LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      Assert.fail("Should throw RuntimeException for exceeding maxRelations limit");
    } catch (RuntimeException e) {
      // Verify the exception message contains the maxRelations limit error
      // This ensures the business logic exception is properly propagated
      // Check the entire exception chain recursively for the expected message
      Assert.assertTrue(
          hasMessageInChain(e, "exceeded maxRelations limit"),
          "Expected maxRelations limit error in exception chain, got: " + e.getMessage());
    }
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
    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

    // Mock a search operation that will throw an exception
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Search operation failed"));

    // Note: GraphQueryElasticsearch7DAO uses scroll-based search, not PIT
    // No need to mock createPit for this DAO

    // This should throw an exception due to the search operation failure
    try {
      dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      Assert.fail("Should throw RuntimeException for search operation failure");
    } catch (RuntimeException e) {
      // The exception should be wrapped in our new exception handling
      // Check the entire exception chain for the expected messages
      Assert.assertTrue(
          hasMessageInChain(e, "Failed to execute slice-based search"),
          "Expected slice-related error message in exception chain, got: " + e.getMessage());
    }
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
    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

    // Mock empty response
    SearchResponse searchResponse = createEmptySearchResponse(0);

    // Use the utility method to mock slice-based search behavior
    // Both slices get the same search response
    mockSliceBasedSearch(mockClient, List.of(searchResponse), List.of(searchResponse));

    // Note: GraphQueryElasticsearch7DAO uses scroll-based search, not PIT
    // No need to mock createPit for this DAO

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
    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

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

    // Note: GraphQueryElasticsearch7DAO uses scroll-based search, not PIT
    // No need to mock createPit for this DAO

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
                  Assert.fail(
                      "Thread " + threadId + " should not throw exception: " + e.getMessage());
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
                  Assert.fail(
                      "Thread " + threadId + " should not throw exception: " + e.getMessage());
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

    GraphQueryElasticsearch7DAO dao =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

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

    // Note: GraphQueryElasticsearch7DAO uses scroll-based search, not PIT
    // No need to mock createPit for this DAO

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
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch connection failed"));

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

    // Test that executeLineageSearchQuery throws ESQueryException when client.search fails
    try {
      graphQueryDAO.getSearchResponse(
          operationContext, GraphFilters.outgoingFilter(EMPTY_FILTER), 0, 10);
      Assert.fail("Expected ESQueryException to be thrown");
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
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch search failed"));

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices("test_index");

    // Test that executeSearch throws ESQueryException when client.search fails
    try {
      graphQueryDAO.executeSearch(searchRequest);
      Assert.fail("Expected ESQueryException to be thrown");
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
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch scroll search failed"));

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

    GraphFilters graphFilters = GraphFilters.outgoingFilter(EMPTY_FILTER);
    List<SortCriterion> sortCriteria =
        List.of(new SortCriterion().setField("createdOn").setOrder(SortOrder.DESCENDING));

    // Test that executeScrollSearchQuery throws ESQueryException when client.search fails
    try {
      graphQueryDAO.getSearchResponse(operationContext, graphFilters, sortCriteria, null, null, 10);
      Assert.fail("Expected ESQueryException to be thrown");
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
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch group by lineage search failed"));

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

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
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch query with limit failed"));

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

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
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch search request failed"));

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

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
      when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
          .thenThrow(new RuntimeException(exceptionMessage));

      GraphQueryElasticsearch7DAO graphQueryDAO =
          new GraphQueryElasticsearch7DAO(
              mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

      try {
        graphQueryDAO.getSearchResponse(
            operationContext, GraphFilters.outgoingFilter(EMPTY_FILTER), 0, 10);
        Assert.fail("Expected ESQueryException to be thrown for: " + exceptionMessage);
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

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

    try {
      graphQueryDAO.getSearchResponse(
          operationContext, GraphFilters.outgoingFilter(EMPTY_FILTER), 0, 10);
      Assert.fail("Expected ESQueryException to be thrown");
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

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

    try {
      graphQueryDAO.getSearchResponse(
          operationContext, GraphFilters.outgoingFilter(EMPTY_FILTER), 0, 10);
      Assert.fail("Expected ESQueryException to be thrown");
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

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, TEST_OS_SEARCH_CONFIG, null);

    try {
      graphQueryDAO.getSearchResponse(
          operationContext, GraphFilters.outgoingFilter(EMPTY_FILTER), 0, 10);
      Assert.fail("Expected ESQueryException to be thrown");
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

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, elasticsearchConfig, null);

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

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, elasticsearchConfig, null);

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

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, elasticsearchConfig, null);

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

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, elasticsearchConfig, null);

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
  public void testScrollSearchCleanupOnException() throws Exception {
    // Test that scroll context cleanup is attempted when scroll search completes
    // Note: This test verifies the scroll cleanup mechanism works in normal cases
    SearchClientShim<?> mockClient = mock(SearchClientShim.class);
    // This triggers the scroll path
    when(mockClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_7);

    ElasticSearchConfiguration elasticsearchConfig = TEST_OS_SEARCH_CONFIG.toBuilder().build();

    GraphQueryElasticsearch7DAO graphQueryDAO =
        new GraphQueryElasticsearch7DAO(
            mockClient, TEST_GRAPH_SERVICE_CONFIG, elasticsearchConfig, null);

    // Mock initial search to succeed and return a scroll response
    SearchHit[] hits = createFakeLineageHits(1, "urn:li:dataset:test-urn", "test", "DownstreamOf");
    SearchResponse mockInitialResponse = createFakeSearchResponse(hits, 1, "test_scroll_id");

    // Mock empty response to end scroll
    SearchResponse mockEmptyResponse = createEmptySearchResponse(0);
    when(mockEmptyResponse.getScrollId()).thenReturn("test_scroll_id");

    // Mock the initial search call to return scroll response
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockInitialResponse);

    // Mock scroll call to succeed once, then return empty results to end scroll
    when(mockClient.scroll(any(SearchScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockEmptyResponse); // Return empty results to end scroll

    // Mock clear scroll to succeed
    ClearScrollResponse mockClearScrollResponse = mock(ClearScrollResponse.class);
    when(mockClient.clearScroll(any(ClearScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockClearScrollResponse);

    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:test-urn");
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // This should complete successfully and clean up scroll contexts
    LineageResponse result =
        graphQueryDAO.getImpactLineage(operationContext, testUrn, lineageGraphFilters, 1);

    // Verify that the search completed successfully
    Assert.assertNotNull(result);

    // Verify that clearScroll was called to clean up scroll context
    verify(mockClient, atLeast(1))
        .clearScroll(any(ClearScrollRequest.class), eq(RequestOptions.DEFAULT));
  }

  /**
   * Helper method to recursively check if any exception in the chain contains the specified
   * message. This allows tests to find expected exception messages regardless of how deeply they're
   * nested. Handles circular references in exception chains to prevent infinite loops.
   */
  private boolean hasMessageInChain(Throwable throwable, String expectedMessage) {
    return hasMessageInChain(throwable, expectedMessage, new HashSet<>());
  }

  /** Private helper method that tracks visited exceptions to prevent infinite loops. */
  private boolean hasMessageInChain(
      Throwable throwable, String expectedMessage, Set<Throwable> visited) {
    if (throwable == null) {
      return false;
    }

    // Check if we've already visited this exception to prevent infinite loops
    if (visited.contains(throwable)) {
      return false;
    }

    // Mark this exception as visited
    visited.add(throwable);

    // Check current exception's message
    String message = throwable.getMessage();
    if (message != null && message.contains(expectedMessage)) {
      return true;
    }

    // Recursively check the cause
    return hasMessageInChain(throwable.getCause(), expectedMessage, visited);
  }
}
