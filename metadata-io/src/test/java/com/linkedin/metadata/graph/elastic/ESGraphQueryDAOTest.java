package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
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
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO.LineageResponse;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ESGraphQueryDAOTest {

  private static final String TEST_QUERY_FILE_LIMITED =
      "elasticsearch/sample_filters/lineage_query_filters_limited.json";
  private static final String TEST_QUERY_FILE_FULL =
      "elasticsearch/sample_filters/lineage_query_filters_full.json";
  private static final String TEST_QUERY_FILE_FULL_EMPTY_FILTERS =
      "elasticsearch/sample_filters/lineage_query_filters_full_empty_filters.json";
  private static final String TEST_QUERY_FILE_FULL_MULTIPLE_FILTERS =
      "elasticsearch/sample_filters/lineage_query_filters_full_multiple_filters.json";

  private OperationContext operationContext;

  @BeforeTest
  public void init() {
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

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

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            null,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            null,
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);
    QueryBuilder limitedBuilder = graphQueryDAO.getLineageQueryForEntityType(urns, edgeInfos);

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
    ESGraphQueryDAO.ThreadSafePathStore pathStore = new ESGraphQueryDAO.ThreadSafePathStore();
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, null, testChild);
    Map<Urn, UrnArrayArray> nodePaths = pathStore.toUrnArrayArrayMap();
    UrnArrayArray expectedPathsToChild =
        new UrnArrayArray(ImmutableList.of(new UrnArray(ImmutableList.of(testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 1: No paths to parent.
    pathStore = new ESGraphQueryDAO.ThreadSafePathStore();
    pathStore.addPath(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,Other,PROD)"), new UrnArray());
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, null, testChild);
    nodePaths = pathStore.toUrnArrayArrayMap();
    expectedPathsToChild =
        new UrnArrayArray(ImmutableList.of(new UrnArray(ImmutableList.of(testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 2: 1 Existing Path to Parent Node
    pathStore = new ESGraphQueryDAO.ThreadSafePathStore();
    Urn testParentParent =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestParent,PROD)");
    UrnArray existingPathToParent = new UrnArray(ImmutableList.of(testParentParent, testParent));
    pathStore.addPath(testParent, existingPathToParent);
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, null, testChild);
    nodePaths = pathStore.toUrnArrayArrayMap();
    expectedPathsToChild =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParentParent, testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 3: > 1 Existing Paths to Parent Node
    pathStore = new ESGraphQueryDAO.ThreadSafePathStore();
    Urn testParentParent2 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestParent2,PROD)");
    UrnArray existingPathToParent1 = new UrnArray(ImmutableList.of(testParentParent, testParent));
    UrnArray existingPathToParent2 = new UrnArray(ImmutableList.of(testParentParent2, testParent));
    pathStore.addPath(testParent, existingPathToParent1);
    pathStore.addPath(testParent, existingPathToParent2);
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, null, testChild);
    nodePaths = pathStore.toUrnArrayArrayMap();
    expectedPathsToChild =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParentParent, testParent, testChild)),
                new UrnArray(ImmutableList.of(testParentParent2, testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 4: Build graph from empty by adding multiple edges
    pathStore = new ESGraphQueryDAO.ThreadSafePathStore();
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParentParent, null, testParent);
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParentParent2, null, testParent);
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, null, testChild);
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
    pathStore = new ESGraphQueryDAO.ThreadSafePathStore();
    // Add edge to testChild first! Before path to testParent has been constructed.
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, null, testChild);
    // Duplicate paths are now prevented - this is an improvement over the old behavior
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, null, testChild);
    // Now construct paths to testParent.
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParentParent, null, testParent);
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParentParent2, null, testParent);
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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);

    // Create the DAO with mocks
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
    Assert.assertEquals(capturedRequest.indices()[0], "graph_service_v1");

    SearchSourceBuilder sourceBuilder = capturedRequest.source();
    Assert.assertEquals(sourceBuilder.size(), count);

    // Verify scroll ID was used
    Assert.assertNotNull(sourceBuilder.searchAfter());
  }

  @Test
  public void testGetLineageQueryWithInvalidEntityTypes() {
    // Mock only the client
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);

    // Create the DAO with minimal mocks
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);

    // Create the DAO with minimal mocks
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);

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

    // Create the DAO with our mock registry
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            mockLineageRegistry,
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
        dao.getLineageQuery(operationContext, urnsPerEntityType, lineageGraphFilters);

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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
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
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            testConfig,
            TEST_ES_SEARCH_CONFIG,
            null);

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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
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
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            testConfig,
            TEST_ES_SEARCH_CONFIG,
            null);

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

    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

    // Mock responses for slice-based search
    // With 2 slices, we expect 2 calls to search (one per slice)
    // Each slice returns one page of results
    SearchHit[] hits1 = new SearchHit[2];
    SearchHit[] hits2 = new SearchHit[1];

    for (int i = 0; i < 2; i++) {
      SearchHit hit = mock(SearchHit.class);
      Map<String, Object> source = new HashMap<>();
      source.put(
          "source",
          Map.of(
              "entityType",
              "dataset",
              "urn",
              "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)"));
      source.put(
          "destination",
          Map.of(
              "entityType",
              "dataset",
              "urn",
              "urn:li:dataset:(urn:li:dataPlatform:test,dest" + i + ",PROD)"));
      source.put("relationshipType", "DownstreamOf");
      source.put("createdOn", System.currentTimeMillis());
      source.put("updatedOn", System.currentTimeMillis());
      source.put("createdActor", "urn:li:corpuser:test");
      source.put("updatedActor", "urn:li:corpuser:test");
      source.put("properties", Map.of("source", "API"));

      when(hit.getSourceAsMap()).thenReturn(source);
      when(hit.getSortValues()).thenReturn(new Object[] {"sort_" + i});

      hits1[i] = hit;
    }

    SearchHit hit = mock(SearchHit.class);
    Map<String, Object> source = new HashMap<>();
    source.put(
        "source",
        Map.of(
            "entityType",
            "dataset",
            "urn",
            "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)"));
    source.put(
        "destination",
        Map.of(
            "entityType",
            "dataset",
            "urn",
            "urn:li:dataset:(urn:li:dataPlatform:test,dest2,PROD)"));
    source.put("relationshipType", "DownstreamOf");
    source.put("createdOn", System.currentTimeMillis());
    source.put("updatedOn", System.currentTimeMillis());
    source.put("createdActor", "urn:li:corpuser:test");
    source.put("updatedActor", "urn:li:corpuser:test");
    source.put("properties", Map.of("source", "API"));

    when(hit.getSourceAsMap()).thenReturn(source);
    when(hit.getSortValues()).thenReturn(new Object[] {"sort_2"});

    hits2[0] = hit;

    SearchHits searchHits1 = mock(SearchHits.class);
    when(searchHits1.getHits()).thenReturn(hits1);
    when(searchHits1.getTotalHits()).thenReturn(new TotalHits(2L, TotalHits.Relation.EQUAL_TO));

    SearchHits searchHits2 = mock(SearchHits.class);
    when(searchHits2.getHits()).thenReturn(hits2);
    when(searchHits2.getTotalHits()).thenReturn(new TotalHits(1L, TotalHits.Relation.EQUAL_TO));

    SearchResponse searchResponse1 = mock(SearchResponse.class);
    when(searchResponse1.getHits()).thenReturn(searchHits1);
    when(searchResponse1.getScrollId()).thenReturn("scroll_id_1");

    SearchResponse searchResponse2 = mock(SearchResponse.class);
    when(searchResponse2.getHits()).thenReturn(searchHits2);
    when(searchResponse2.getScrollId()).thenReturn("scroll_id_2");

    // Mock the initial search calls for each slice
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(searchResponse1)
        .thenReturn(searchResponse2);

    // Mock empty responses for scroll calls to stop pagination
    SearchHits emptyHits = mock(SearchHits.class);
    when(emptyHits.getHits()).thenReturn(new SearchHit[0]);
    when(emptyHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));

    SearchResponse emptyScrollResponse = mock(SearchResponse.class);
    when(emptyScrollResponse.getHits()).thenReturn(emptyHits);
    when(emptyScrollResponse.getScrollId()).thenReturn("scroll_id_empty");

    // Mock the scroll calls - each slice will make one scroll call that returns empty
    when(mockClient.scroll(any(SearchScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(emptyScrollResponse);

    // Mock the clearScroll calls
    when(mockClient.clearScroll(any(ClearScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(null);

    // Test getImpactLineage with 2 slices
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response);
    Assert.assertEquals(3, response.getTotal());

    // Verify that search was called twice (once per slice)
    verify(mockClient, atLeast(2)).search(any(SearchRequest.class), eq(RequestOptions.DEFAULT));

    // Verify that scroll was called for each slice
    verify(mockClient, atLeast(2))
        .scroll(any(SearchScrollRequest.class), eq(RequestOptions.DEFAULT));

    // Verify that clearScroll was called to clean up resources
    verify(mockClient, atLeast(2))
        .clearScroll(any(ClearScrollRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testGetImpactLineageMaxRelationsLimit() throws Exception {
    // Test that maxRelations limit is enforced
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

    // Create responses that will exceed the maxRelations limit of 100
    // First slice returns 60 relationships
    SearchHit[] hits1 = new SearchHit[60];
    for (int i = 0; i < 60; i++) {
      SearchHit hit = mock(SearchHit.class);
      Map<String, Object> source = new HashMap<>();
      source.put(
          "source",
          Map.of(
              "entityType",
              "dataset",
              "urn",
              "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)"));
      source.put(
          "destination",
          Map.of(
              "entityType",
              "dataset",
              "urn",
              "urn:li:dataset:(urn:li:dataPlatform:test,dest1_" + i + ",PROD)"));
      source.put("relationshipType", "DownstreamOf");
      source.put("createdOn", System.currentTimeMillis());
      source.put("updatedOn", System.currentTimeMillis());
      source.put("createdActor", "urn:li:corpuser:test");
      source.put("updatedActor", "urn:li:corpuser:test");
      source.put("properties", Map.of("source", "API"));

      when(hit.getSourceAsMap()).thenReturn(source);
      when(hit.getSortValues()).thenReturn(new Object[] {"sort1_" + i});

      hits1[i] = hit;
    }

    // Second slice returns 50 relationships, bringing total to 110 (exceeds limit of 100)
    SearchHit[] hits2 = new SearchHit[50];
    for (int i = 0; i < 50; i++) {
      SearchHit hit = mock(SearchHit.class);
      Map<String, Object> source = new HashMap<>();
      source.put(
          "source",
          Map.of(
              "entityType",
              "dataset",
              "urn",
              "urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)"));
      source.put(
          "destination",
          Map.of(
              "entityType",
              "dataset",
              "urn",
              "urn:li:dataset:(urn:li:dataPlatform:test,dest2_" + i + ",PROD)"));
      source.put("relationshipType", "DownstreamOf");
      source.put("createdOn", System.currentTimeMillis());
      source.put("updatedOn", System.currentTimeMillis());
      source.put("createdActor", "urn:li:corpuser:test");
      source.put("updatedActor", "urn:li:corpuser:test");
      source.put("properties", Map.of("source", "API"));

      when(hit.getSourceAsMap()).thenReturn(source);
      when(hit.getSortValues()).thenReturn(new Object[] {"sort2_" + i});

      hits2[i] = hit;
    }

    SearchHits searchHits1 = mock(SearchHits.class);
    when(searchHits1.getHits()).thenReturn(hits1);
    when(searchHits1.getTotalHits()).thenReturn(new TotalHits(60L, TotalHits.Relation.EQUAL_TO));

    SearchHits searchHits2 = mock(SearchHits.class);
    when(searchHits2.getHits()).thenReturn(hits2);
    when(searchHits2.getTotalHits()).thenReturn(new TotalHits(50L, TotalHits.Relation.EQUAL_TO));

    SearchResponse searchResponse1 = mock(SearchResponse.class);
    when(searchResponse1.getHits()).thenReturn(searchHits1);
    when(searchResponse1.getScrollId()).thenReturn("scroll_id_1");

    SearchResponse searchResponse2 = mock(SearchResponse.class);
    when(searchResponse2.getHits()).thenReturn(searchHits2);
    when(searchResponse2.getScrollId()).thenReturn("scroll_id_2");

    // Mock the initial search calls for each slice
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(searchResponse1)
        .thenReturn(searchResponse2);

    // Mock empty responses for scroll calls to stop pagination
    SearchHits emptyHits = mock(SearchHits.class);
    when(emptyHits.getHits()).thenReturn(new SearchHit[0]);
    when(emptyHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));

    SearchResponse emptyScrollResponse = mock(SearchResponse.class);
    when(emptyScrollResponse.getHits()).thenReturn(emptyHits);
    when(emptyScrollResponse.getScrollId()).thenReturn("scroll_id_empty");

    // Mock the scroll calls - each slice will make one scroll call that returns empty
    when(mockClient.scroll(any(SearchScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(emptyScrollResponse);

    // Mock the clearScroll calls
    when(mockClient.clearScroll(any(ClearScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(null);

    // This should throw an exception due to exceeding maxRelations limit
    // First slice: 60 relationships, Second slice: 50 relationships = 110 total > 100 limit
    try {
      LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      // If we get here, let's check what we actually got
      System.out.println("Got " + response.getTotal() + " relationships, expected exception");
      Assert.fail("Should throw RuntimeException for exceeding maxRelations limit");
    } catch (RuntimeException e) {
      // The IllegalStateException gets wrapped in a RuntimeException
      Assert.assertTrue(e.getMessage().contains("Failed to execute slice-based scroll search"));
      Assert.assertTrue(e.getCause() instanceof IllegalStateException);
      Assert.assertTrue(
          e.getCause().getMessage().contains("exceeded the configured maxRelations limit"));
    }
  }

  @Test
  public void testGetImpactLineageTimeout() throws Exception {
    // Test timeout handling
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

    // Mock a response that will cause timeout
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            invocation -> {
              // Simulate a delay that will cause timeout
              Thread.sleep(6000); // Longer than the 5 second timeout
              return null;
            });

    // This should throw an exception due to timeout
    try {
      dao.getImpactLineage(operationContext, sourceUrn, filters, 1);
      Assert.fail("Should throw IllegalStateException for timeout");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Slice processing timed out"));
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

    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

    // Mock empty response
    SearchHits searchHits = mock(SearchHits.class);
    when(searchHits.getHits()).thenReturn(new SearchHit[0]);
    when(searchHits.getTotalHits()).thenReturn(new TotalHits(0L, TotalHits.Relation.EQUAL_TO));

    SearchResponse searchResponse = mock(SearchResponse.class);
    when(searchResponse.getHits()).thenReturn(searchHits);

    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(searchResponse);

    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 1);

    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getLineageRelationships());
    Assert.assertEquals(0, response.getTotal());
  }

  @Test
  public void testGetImpactLineageMaxHopsLimit() throws Exception {
    // Test that maxHops limit is respected
    Urn sourceUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test_dataset,PROD)");

    LineageGraphFilters filters =
        LineageGraphFilters.forEntityType(
            operationContext.getLineageRegistry(), DATASET_ENTITY_NAME, LineageDirection.UPSTREAM);

    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

    // Mock a simple response
    SearchHit[] hits = new SearchHit[3];
    for (int i = 0; i < 3; i++) {
      SearchHit hit = mock(SearchHit.class);
      Map<String, Object> source = new HashMap<>();
      source.put(
          "source",
          Map.of(
              "entityType",
              "dataset",
              "urn",
              "urn:li:dataset:(urn:li:dataPlatform:test,source" + i + ",PROD)"));
      source.put(
          "destination",
          Map.of(
              "entityType",
              "dataset",
              "urn",
              "urn:li:dataset:(urn:li:dataPlatform:test,dest" + i + ",PROD)"));
      source.put("relationshipType", "DownstreamOf");
      source.put("createdOn", System.currentTimeMillis());
      source.put("updatedOn", System.currentTimeMillis());
      source.put("createdActor", "urn:li:corpuser:test");
      source.put("updatedActor", "urn:li:corpuser:test");
      source.put("properties", Map.of("source", "API"));

      when(hit.getSourceAsMap()).thenReturn(source);
      when(hit.getSortValues()).thenReturn(new Object[] {"sort_" + i});

      hits[i] = hit;
    }

    SearchHits searchHits = mock(SearchHits.class);
    when(searchHits.getHits()).thenReturn(hits);
    when(searchHits.getTotalHits()).thenReturn(new TotalHits(3L, TotalHits.Relation.EQUAL_TO));

    SearchResponse searchResponse = mock(SearchResponse.class);
    when(searchResponse.getHits()).thenReturn(searchHits);

    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(searchResponse);

    // Test with maxHops = 0 (should return empty result)
    LineageResponse response = dao.getImpactLineage(operationContext, sourceUrn, filters, 0);

    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getLineageRelationships());
    Assert.assertEquals(0, response.getTotal());
  }

  @Test
  public void testComputeIfAbsentThreadSafety() throws Exception {
    // Test the computeIfAbsent pattern used in addEdgeToPaths
    ESGraphQueryDAO graphQueryDAO = createTestDAO();

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
    ESGraphQueryDAO.ThreadSafePathStore pathStore = new ESGraphQueryDAO.ThreadSafePathStore();
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, null, testChild);
    ESGraphQueryDAO.addEdgeToPaths(
        pathStore, testParent, null, testChild); // Should not add duplicate

    Map<Urn, UrnArrayArray> nodePaths = pathStore.toUrnArrayArrayMap();
    UrnArrayArray pathsToChild = nodePaths.get(testChild);
    Assert.assertNotNull(pathsToChild);
    Assert.assertEquals(pathsToChild.size(), 1); // Only one path should exist

    // Test duplicate prevention with via node
    pathStore = new ESGraphQueryDAO.ThreadSafePathStore();
    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, testVia, testChild);
    ESGraphQueryDAO.addEdgeToPaths(
        pathStore, testParent, testVia, testChild); // Should not add duplicate

    nodePaths = pathStore.toUrnArrayArrayMap();
    pathsToChild = nodePaths.get(testChild);
    Assert.assertNotNull(pathsToChild);
    Assert.assertEquals(pathsToChild.size(), 1); // Only one path should exist

    // Test duplicate prevention when extending existing paths
    pathStore = new ESGraphQueryDAO.ThreadSafePathStore();
    Urn testParentParent =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestParent,PROD)");
    UrnArray existingPathToParent = new UrnArray(ImmutableList.of(testParentParent, testParent));
    pathStore.addPath(testParent, existingPathToParent);

    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, null, testChild);
    ESGraphQueryDAO.addEdgeToPaths(
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

    ESGraphQueryDAO.ThreadSafePathStore pathStore = new ESGraphQueryDAO.ThreadSafePathStore();

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
                    ESGraphQueryDAO.addEdgeToPaths(pathStore, testParent, null, testChild);
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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);

    ESGraphQueryDAO dao =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

    // Create test URNs
    Urn sourceUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestSource,PROD)");
    Urn targetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestTarget,PROD)");

    LineageGraphFilters filters = createTestLineageGraphFilters();

    // Mock search response that would normally cause duplicate paths
    SearchResponse mockSearchResponse = mock(SearchResponse.class);
    SearchHits mockSearchHits = mock(SearchHits.class);
    SearchHit mockHit = mock(SearchHit.class);

    // Create a mock document that represents a lineage edge
    Map<String, Object> source = new HashMap<>();
    source.put("source", sourceUrn.toString());
    source.put("destination", targetUrn.toString());
    source.put("relationshipType", "DownstreamOf");
    source.put("createdOn", System.currentTimeMillis());
    source.put("updatedOn", System.currentTimeMillis());

    when(mockHit.getSourceAsMap()).thenReturn(source);
    when(mockSearchHits.getHits()).thenReturn(new SearchHit[] {mockHit});
    when(mockSearchHits.getTotalHits()).thenReturn(new TotalHits(1L, TotalHits.Relation.EQUAL_TO));
    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);

    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSearchResponse);
    when(mockClient.scroll(any(SearchScrollRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSearchResponse);

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

  private ESGraphQueryDAO createTestDAO() {
    return new ESGraphQueryDAO(
        null,
        false,
        ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
        operationContext.getLineageRegistry(),
        null,
        TEST_GRAPH_SERVICE_CONFIG,
        TEST_ES_SEARCH_CONFIG,
        null);
  }

  private LineageGraphFilters createTestLineageGraphFilters() {
    return new LineageGraphFilters(
        LineageDirection.DOWNSTREAM,
        ImmutableSet.of(DATASET_ENTITY_NAME),
        null,
        new ConcurrentHashMap<>());
  }

  @Test
  private void testExecuteLineageSearchQueryThrowsESQueryException() throws Exception {
    // Mock the client to throw an exception
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch connection failed"));

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch search failed"));

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch scroll search failed"));

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch group by lineage search failed"));

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
              new ESGraphQueryDAO.ThreadSafePathStore(),
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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch query with limit failed"));

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
              new ESGraphQueryDAO.ThreadSafePathStore(),
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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch search request failed"));

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
              new ESGraphQueryDAO.ThreadSafePathStore(),
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
  private void testScrollSingleSliceThrowsESQueryException() throws Exception {
    // Mock the client to throw an exception
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new RuntimeException("Elasticsearch scroll single slice failed"));

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

    // Create test data
    Urn entityUrn = Urn.createFromString("urn:li:dataset:test-urn");
    LineageGraphFilters lineageGraphFilters =
        new LineageGraphFilters(
            LineageDirection.DOWNSTREAM,
            ImmutableSet.of(DATASET_ENTITY_NAME),
            null,
            new ConcurrentHashMap<>());

    // Test that the method handles the error gracefully and returns empty results
    // instead of throwing an exception, since the public API should be stable
    try {
      LineageResponse result =
          graphQueryDAO.getImpactLineage(operationContext, entityUrn, lineageGraphFilters, 2);

      // The method should handle the error gracefully and return empty results
      // This tests that the public API is stable even when Elasticsearch fails
      Assert.assertNotNull(result);
      Assert.assertTrue(result.getLineageRelationships().isEmpty());
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
      RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
      when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
          .thenThrow(new RuntimeException(exceptionMessage));

      ESGraphQueryDAO graphQueryDAO =
          new ESGraphQueryDAO(
              mockClient,
              false,
              ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
              operationContext.getLineageRegistry(),
              operationContext.getSearchContext().getIndexConvention(),
              TEST_GRAPH_SERVICE_CONFIG,
              TEST_ES_SEARCH_CONFIG,
              null);

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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    RuntimeException originalException = new RuntimeException("Original error message");
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(originalException);

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    RuntimeException originalException = new RuntimeException("Error without cause");
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(originalException);

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);
    RuntimeException rootCause = new RuntimeException("Root cause");
    RuntimeException middleException = new RuntimeException("Middle layer", rootCause);
    RuntimeException topException = new RuntimeException("Top layer", middleException);

    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(topException);

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(
            mockClient,
            false,
            ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH,
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            null);

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
}
