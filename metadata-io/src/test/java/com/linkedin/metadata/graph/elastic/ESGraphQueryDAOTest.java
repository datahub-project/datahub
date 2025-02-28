package com.linkedin.metadata.graph.elastic;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.index.query.QueryBuilder;
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

    List<Urn> urns = List.of(Urn.createFromString("urn:li:dataset:test-urn"));
    List<Urn> urnsMultiple1 =
        ImmutableList.of(
            UrnUtils.getUrn("urn:li:dataset:test-urn"),
            UrnUtils.getUrn("urn:li:dataset:test-urn2"),
            UrnUtils.getUrn("urn:li:dataset:test-urn3"));
    List<Urn> urnsMultiple2 =
        ImmutableList.of(
            UrnUtils.getUrn("urn:li:chart:test-urn"),
            UrnUtils.getUrn("urn:li:chart:test-urn2"),
            UrnUtils.getUrn("urn:li:chart:test-urn3"));
    List<LineageRegistry.EdgeInfo> edgeInfos =
        new ArrayList<>(
            ImmutableList.of(
                new LineageRegistry.EdgeInfo(
                    "DownstreamOf",
                    RelationshipDirection.INCOMING,
                    Constants.DATASET_ENTITY_NAME)));
    List<LineageRegistry.EdgeInfo> edgeInfosMultiple1 =
        ImmutableList.of(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.OUTGOING, Constants.DATASET_ENTITY_NAME),
            new LineageRegistry.EdgeInfo(
                "Consumes", RelationshipDirection.OUTGOING, Constants.DATASET_ENTITY_NAME));
    List<LineageRegistry.EdgeInfo> edgeInfosMultiple2 =
        ImmutableList.of(
            new LineageRegistry.EdgeInfo(
                "DownstreamOf", RelationshipDirection.OUTGOING, Constants.DATA_JOB_ENTITY_NAME),
            new LineageRegistry.EdgeInfo(
                "Consumes", RelationshipDirection.OUTGOING, Constants.DATA_JOB_ENTITY_NAME));
    String entityType = "testEntityType";
    Map<String, List<Urn>> urnsPerEntityType = Map.of(entityType, urns);
    Map<String, List<Urn>> urnsPerEntityTypeMultiple =
        Map.of(
            Constants.DATASET_ENTITY_NAME,
            urnsMultiple1,
            Constants.CHART_ENTITY_NAME,
            urnsMultiple2);
    Map<String, List<LineageRegistry.EdgeInfo>> edgesPerEntityType = Map.of(entityType, edgeInfos);
    Map<String, List<LineageRegistry.EdgeInfo>> edgesPerEntityTypeMultiple =
        Map.of(
            Constants.DATASET_ENTITY_NAME, edgeInfosMultiple1,
            Constants.DATA_JOB_ENTITY_NAME, edgeInfosMultiple2);
    GraphFilters graphFilters = new GraphFilters(ImmutableList.of(Constants.DATASET_ENTITY_NAME));
    GraphFilters graphFiltersMultiple =
        new GraphFilters(
            ImmutableList.of(
                Constants.DATASET_ENTITY_NAME,
                Constants.DASHBOARD_ENTITY_NAME,
                Constants.DATA_JOB_ENTITY_NAME));
    Long startTime = 0L;
    Long endTime = 1L;

    ESGraphQueryDAO graphQueryDAO =
        new ESGraphQueryDAO(null, null, null, new GraphQueryConfiguration());
    QueryBuilder limitedBuilder =
        graphQueryDAO.getLineageQueryForEntityType(urns, edgeInfos, graphFilters);

    QueryBuilder fullBuilder =
        graphQueryDAO.getLineageQuery(
            operationContext.withLineageFlags(
                f -> new LineageFlags().setEndTimeMillis(endTime).setStartTimeMillis(startTime)),
            urnsPerEntityType,
            edgesPerEntityType,
            graphFilters);

    QueryBuilder fullBuilderEmptyFilters =
        graphQueryDAO.getLineageQuery(
            operationContext,
            urnsPerEntityType,
            edgesPerEntityType,
            GraphFilters.emptyGraphFilters);

    QueryBuilder fullBuilderMultipleFilters =
        graphQueryDAO.getLineageQuery(
            operationContext.withLineageFlags(
                f -> new LineageFlags().setEndTimeMillis(endTime).setStartTimeMillis(startTime)),
            urnsPerEntityTypeMultiple,
            edgesPerEntityTypeMultiple,
            graphFiltersMultiple);

    Assert.assertEquals(limitedBuilder.toString(), expectedQueryLimited);
    Assert.assertEquals(fullBuilder.toString(), expectedQueryFull);
    Assert.assertEquals(fullBuilderEmptyFilters.toString(), expectedQueryFullEmptyFilters);
    Assert.assertEquals(fullBuilderMultipleFilters.toString(), expectedQueryFullMultipleFilters);
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
}
