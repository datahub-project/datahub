package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_GRAPH_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
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
            operationContext.getLineageRegistry(),
            null,
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG);
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
    Map<Urn, UrnArrayArray> nodePaths = new HashMap<>();
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParent, testChild);
    UrnArrayArray expectedPathsToChild =
        new UrnArrayArray(ImmutableList.of(new UrnArray(ImmutableList.of(testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 1: No paths to parent.
    nodePaths = new HashMap<>();
    nodePaths.put(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,Other,PROD)"),
        new UrnArrayArray());
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParent, testChild);
    expectedPathsToChild =
        new UrnArrayArray(ImmutableList.of(new UrnArray(ImmutableList.of(testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 2: 1 Existing Path to Parent Node
    nodePaths = new HashMap<>();
    Urn testParentParent =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestParent,PROD)");
    UrnArrayArray existingPathsToParent =
        new UrnArrayArray(
            ImmutableList.of(new UrnArray(ImmutableList.of(testParentParent, testParent))));
    nodePaths.put(testParent, existingPathsToParent);
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParent, testChild);
    expectedPathsToChild =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParentParent, testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 3: > 1 Existing Paths to Parent Node
    nodePaths = new HashMap<>();
    Urn testParentParent2 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,TestParent2,PROD)");
    UrnArrayArray existingPathsToParent2 =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParentParent, testParent)),
                new UrnArray(ImmutableList.of(testParentParent2, testParent))));
    nodePaths.put(testParent, existingPathsToParent2);
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParent, testChild);
    expectedPathsToChild =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParentParent, testParent, testChild)),
                new UrnArray(ImmutableList.of(testParentParent2, testParent, testChild))));
    Assert.assertEquals(nodePaths.get(testChild), expectedPathsToChild);

    // Case 4: Build graph from empty by adding multiple edges
    nodePaths = new HashMap<>();
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParentParent, testParent);
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParentParent2, testParent);
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParent, testChild);

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
    // Also test duplicate edge addition
    nodePaths = new HashMap<>();
    // Add edge to testChild first! Before path to testParent has been constructed.
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParent, testChild);
    // Duplicate paths WILL appear if you add the same edge twice. Documenting that here.
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParent, testChild);
    // Now construct paths to testParent.
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParentParent, testParent);
    ESGraphQueryDAO.addEdgeToPaths(nodePaths, testParentParent2, testParent);

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

    // Verify paths to testChild are INCORRECT: partial & duplicated
    expectedPathsToChild =
        new UrnArrayArray(
            ImmutableList.of(
                new UrnArray(ImmutableList.of(testParent, testChild)),
                new UrnArray(ImmutableList.of(testParent, testChild))));
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
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG);

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
    int count = 10;

    // Set up mock behavior
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockSearchResponse);

    // Call the method
    SearchResponse response =
        dao.getSearchResponse(operationContext, graphFilters, sortCriteria, scrollId, count);

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
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG);

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
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG);

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
            mockLineageRegistry,
            operationContext.getSearchContext().getIndexConvention(),
            TEST_GRAPH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG);

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
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
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
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            testConfig,
            TEST_ES_SEARCH_CONFIG);

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
    when(mockClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
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
            operationContext.getLineageRegistry(),
            operationContext.getSearchContext().getIndexConvention(),
            testConfig,
            TEST_ES_SEARCH_CONFIG);

    // Create test data
    GraphFilters graphFilters =
        GraphFilters.outgoingFilter(newFilter("urn", "urn:li:dataset:test"));
    graphFilters.setRelationshipDirection(RelationshipDirection.OUTGOING);
    List<SortCriterion> sortCriteria =
        ImmutableList.of(new SortCriterion().setField("urn").setOrder(SortOrder.DESCENDING));

    // Call method with a count that exceeds the limit
    int requestedCount = 50; // Exceeds our limit of 25
    dao.getSearchResponse(operationContext, graphFilters, sortCriteria, null, requestedCount);

    // Verify that search was called with the right parameters
    ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient).search(requestCaptor.capture(), eq(RequestOptions.DEFAULT));

    SearchRequest capturedRequest = requestCaptor.getValue();
    SearchSourceBuilder sourceBuilder = capturedRequest.source();

    // Verify the size was limited to the max (25)
    Assert.assertEquals(sourceBuilder.size(), 25);
  }
}
