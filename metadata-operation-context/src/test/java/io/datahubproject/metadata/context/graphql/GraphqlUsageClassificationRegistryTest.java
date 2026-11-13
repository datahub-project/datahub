package io.datahubproject.metadata.context.graphql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphqlUsageClassificationRegistryTest {

  private GraphqlUsageClassificationRegistry registry;

  @BeforeMethod
  public void setUp() {
    registry =
        new GraphqlUsageClassificationRegistry(
            Map.of(
                "search", UsageOperation.SEARCH_QUERY,
                "deleteEntity", UsageOperation.ENTITY_DELETE,
                "me", UsageOperation.OTHER_READ),
            List.of(
                new GraphqlPatternRule(UsageOperation.LINEAGE_QUERY, Pattern.compile(".*Lineage")),
                new GraphqlPatternRule(
                    UsageOperation.OTHER_OPERATIONS, Pattern.compile("admin.*"))));
  }

  @Test
  public void testResolveExactOperationName() {
    assertEquals(
        registry.resolve("search", GraphQLOperationKind.QUERY), UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testResolveOperationNamePattern() {
    assertEquals(
        registry.resolve("getLineage", GraphQLOperationKind.QUERY), UsageOperation.LINEAGE_QUERY);
  }

  @Test
  public void testResolveAnonymousUsesRootFields() {
    assertEquals(
        registry.resolve(
            GraphqlUsageClassificationRegistry.ANONYMOUS_OPERATION_NAME,
            GraphQLOperationKind.QUERY,
            List.of("search")),
        UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testResolveAnonymousRootFieldPattern() {
    assertEquals(
        registry.resolve(
            GraphqlUsageClassificationRegistry.ANONYMOUS_OPERATION_NAME,
            GraphQLOperationKind.QUERY,
            List.of("scrollAcrossLineage")),
        UsageOperation.LINEAGE_QUERY);
  }

  @Test
  public void testResolveMostExpensiveRootFieldWins() {
    assertEquals(
        registry.resolve(
            GraphqlUsageClassificationRegistry.ANONYMOUS_OPERATION_NAME,
            GraphQLOperationKind.QUERY,
            List.of("me", "search", "dataset")),
        UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testResolveMutationDefaultIsMetadataWrite() {
    assertEquals(
        registry.resolve(
            GraphqlUsageClassificationRegistry.ANONYMOUS_OPERATION_NAME,
            GraphQLOperationKind.MUTATION,
            List.of()),
        UsageOperation.METADATA_WRITE);
  }

  @Test
  public void testResolveQueryDefaultIsMetadataQuery() {
    assertEquals(
        registry.resolve(
            GraphqlUsageClassificationRegistry.ANONYMOUS_OPERATION_NAME,
            GraphQLOperationKind.QUERY,
            List.of()),
        UsageOperation.METADATA_QUERY);
  }

  @Test
  public void testResolveUnmappedNamedOperationFallsBackToHeuristics() {
    assertEquals(
        registry.resolve("unknownOp", GraphQLOperationKind.QUERY, List.of("browseV2")),
        UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testResolveUnmappedMutationRootUsesDeleteHeuristic() {
    assertEquals(
        registry.resolve(
            GraphqlUsageClassificationRegistry.ANONYMOUS_OPERATION_NAME,
            GraphQLOperationKind.MUTATION,
            List.of("removeTag")),
        UsageOperation.ENTITY_DELETE);
  }

  @Test
  public void testIsAnonymousOperationName() {
    assertTrue(
        registry.isAnonymousOperationName(
            GraphqlUsageClassificationRegistry.ANONYMOUS_OPERATION_NAME));
    assertFalse(registry.isAnonymousOperationName("search"));
  }

  @Test
  public void testResolveByOperationNameEmptyForAnonymous() {
    assertEquals(
        registry.resolveByOperationName(
            GraphqlUsageClassificationRegistry.ANONYMOUS_OPERATION_NAME),
        Optional.empty());
  }

  @Test
  public void testResolveByOperationNameOnlyDeprecated() {
    assertEquals(registry.resolveByOperationNameOnly("me"), UsageOperation.OTHER_READ);
    assertNull(registry.resolveByOperationNameOnly("missing"));
  }

  @Test
  public void testPatternExpenseRankPrefersHigherCost() {
    GraphqlUsageClassificationRegistry ranked =
        new GraphqlUsageClassificationRegistry(
            Map.of(),
            List.of(
                new GraphqlPatternRule(UsageOperation.OTHER_READ, Pattern.compile("foo.*")),
                new GraphqlPatternRule(UsageOperation.METADATA_WRITE, Pattern.compile("foo.*"))));
    assertEquals(
        ranked.resolve("foobar", GraphQLOperationKind.QUERY), UsageOperation.METADATA_WRITE);
  }

  @Test
  public void testGettersExposeImmutableCopies() {
    assertEquals(registry.getExactNames().get("search"), UsageOperation.SEARCH_QUERY);
    assertEquals(registry.getPatternRules().size(), 2);
  }

  @Test
  public void testHeuristicPatternRuleAccessors() {
    GraphqlPatternRule rule =
        new GraphqlPatternRule(UsageOperation.SEARCH_QUERY, Pattern.compile("search.*"));
    assertEquals(rule.operation(), UsageOperation.SEARCH_QUERY);
    assertTrue(rule.pattern().matcher("searchAcrossEntities").matches());
  }

  @Test
  public void testClassifyByHeuristicSearchVariants() {
    assertEquals(
        GraphqlUsageClassificationRegistry.classifyByHeuristic(
            "scrollResults", GraphQLOperationKind.QUERY),
        UsageOperation.SEARCH_QUERY);
    assertEquals(
        GraphqlUsageClassificationRegistry.classifyByHeuristic(
            "autoComplete", GraphQLOperationKind.QUERY),
        UsageOperation.SEARCH_QUERY);
    assertEquals(
        GraphqlUsageClassificationRegistry.classifyByHeuristic(
            "aggregateAcrossEntities", GraphQLOperationKind.QUERY),
        UsageOperation.SEARCH_QUERY);
  }

  @Test
  public void testClassifyByHeuristicRemoveMutation() {
    assertEquals(
        GraphqlUsageClassificationRegistry.classifyByHeuristic(
            "removeOwnership", GraphQLOperationKind.MUTATION),
        UsageOperation.ENTITY_DELETE);
  }

  @Test
  public void testMatchesSearchHeuristicScrollAndBrowse() {
    assertTrue(GraphqlUsageClassificationRegistry.matchesSearchHeuristic("scrollAcrossEntities"));
    assertTrue(GraphqlUsageClassificationRegistry.matchesSearchHeuristic("browsePaths"));
    assertTrue(GraphqlUsageClassificationRegistry.matchesSearchHeuristic("autoCompleteMultiple"));
    assertTrue(
        GraphqlUsageClassificationRegistry.matchesSearchHeuristic("aggregateAcrossEntities"));
  }

  @Test
  public void testMatchesLineageHeuristicSuffix() {
    assertTrue(GraphqlUsageClassificationRegistry.matchesLineageHeuristic("entityLineage"));
    assertFalse(GraphqlUsageClassificationRegistry.matchesLineageHeuristic("lineageCount"));
  }
}
