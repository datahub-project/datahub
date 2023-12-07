package com.linkedin.metadata.graph.search;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.index.query.QueryBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ESGraphQueryDAOTest {

  private static final String TEST_QUERY_FILE =
      "elasticsearch/sample_filters/lineage_query_filters_1.json";

  @Test
  private static void testGetQueryForLineageFullArguments() throws Exception {

    URL url = Resources.getResource(TEST_QUERY_FILE);
    String expectedQuery = Resources.toString(url, StandardCharsets.UTF_8);

    List<Urn> urns = new ArrayList<>();
    List<LineageRegistry.EdgeInfo> edgeInfos =
        new ArrayList<>(
            ImmutableList.of(
                new LineageRegistry.EdgeInfo(
                    "DownstreamOf",
                    RelationshipDirection.INCOMING,
                    Constants.DATASET_ENTITY_NAME)));
    GraphFilters graphFilters = new GraphFilters(ImmutableList.of(Constants.DATASET_ENTITY_NAME));
    Long startTime = 0L;
    Long endTime = 1L;

    QueryBuilder builder =
        ESGraphQueryDAO.getQueryForLineage(urns, edgeInfos, graphFilters, startTime, endTime);

    Assert.assertEquals(builder.toString(), expectedQuery);
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
