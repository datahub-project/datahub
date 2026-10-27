package io.datahubproject.metadata.context.graphql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.datahubproject.metadata.context.usage.UsageOperation;
import org.testng.annotations.Test;

public class GraphqlUsageClassificationRegistryHeuristicTest {

  @Test
  public void testSearchHeuristicMatchesCommonRoots() {
    assertTrue(GraphqlUsageClassificationRegistry.matchesSearchHeuristic("search"));
    assertTrue(GraphqlUsageClassificationRegistry.matchesSearchHeuristic("searchAcrossEntities"));
    assertTrue(GraphqlUsageClassificationRegistry.matchesSearchHeuristic("browseV2"));
    assertTrue(GraphqlUsageClassificationRegistry.matchesSearchHeuristic("semanticSearch"));
    assertFalse(GraphqlUsageClassificationRegistry.matchesSearchHeuristic("dataset"));
  }

  @Test
  public void testLineageHeuristicMatchesCommonRoots() {
    assertTrue(GraphqlUsageClassificationRegistry.matchesLineageHeuristic("searchAcrossLineage"));
    assertTrue(GraphqlUsageClassificationRegistry.matchesLineageHeuristic("scrollAcrossLineage"));
    assertFalse(GraphqlUsageClassificationRegistry.matchesLineageHeuristic("search"));
  }

  @Test
  public void testMutationDeleteHeuristic() {
    assertEquals(
        GraphqlUsageClassificationRegistry.classifyByHeuristic(
            "deleteEntity", GraphQLOperationKind.MUTATION),
        UsageOperation.ENTITY_DELETE);
    assertEquals(
        GraphqlUsageClassificationRegistry.classifyByHeuristic(
            "patchDataset", GraphQLOperationKind.MUTATION),
        UsageOperation.METADATA_WRITE);
  }

  @Test
  public void testQueryDefaultsToMetadataQuery() {
    assertEquals(
        GraphqlUsageClassificationRegistry.classifyByHeuristic(
            "dataset", GraphQLOperationKind.QUERY),
        UsageOperation.METADATA_QUERY);
  }

  @Test
  public void testLineageHeuristicBeforeSearchHeuristic() {
    assertTrue(GraphqlUsageClassificationRegistry.matchesLineageHeuristic("searchAcrossLineage"));
    assertEquals(
        GraphqlUsageClassificationRegistry.classifyByHeuristic(
            "searchAcrossLineage", GraphQLOperationKind.QUERY),
        UsageOperation.LINEAGE_QUERY);
  }
}
