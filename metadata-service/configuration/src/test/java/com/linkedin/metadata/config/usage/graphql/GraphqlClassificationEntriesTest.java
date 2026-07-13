package com.linkedin.metadata.config.usage.graphql;

import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GraphqlClassificationEntriesTest {

  @Test
  public void testResolvedNamesReturnsEmptyForNull() {
    Assert.assertEquals(GraphqlClassificationEntries.resolvedNames(null), List.of());
  }

  @Test
  public void testResolvedNamesPrefersExplicitNames() {
    UsageOperationsManifest.GraphqlClassification graphql =
        new UsageOperationsManifest.GraphqlClassification();
    graphql.setNames(List.of("explicitOp"));
    graphql.setOperationNames(List.of("legacyOp"));
    graphql.setRootFields(List.of("legacyField"));

    Assert.assertEquals(GraphqlClassificationEntries.resolvedNames(graphql), List.of("explicitOp"));
  }

  @Test
  public void testResolvedNamesMergesLegacyAliases() {
    UsageOperationsManifest.GraphqlClassification graphql =
        new UsageOperationsManifest.GraphqlClassification();
    graphql.setOperationNames(List.of("opA", "opB"));
    graphql.setRootFields(List.of("fieldB", "fieldC"));

    Assert.assertEquals(
        GraphqlClassificationEntries.resolvedNames(graphql),
        List.of("opA", "opB", "fieldB", "fieldC"));
  }

  @Test
  public void testResolvedPatternsReturnsEmptyForNullGraphql() {
    Assert.assertEquals(GraphqlClassificationEntries.resolvedPatterns(null), List.of());
  }

  @Test
  public void testResolvedPatternsReturnsEmptyWhenUnset() {
    UsageOperationsManifest.GraphqlClassification graphql =
        new UsageOperationsManifest.GraphqlClassification();
    Assert.assertEquals(GraphqlClassificationEntries.resolvedPatterns(graphql), List.of());
  }

  @Test
  public void testResolvedPatternsCopiesList() {
    UsageOperationsManifest.GraphqlClassification graphql =
        new UsageOperationsManifest.GraphqlClassification();
    graphql.setPatterns(List.of(".*Search.*", "get.*"));

    Assert.assertEquals(
        GraphqlClassificationEntries.resolvedPatterns(graphql), List.of(".*Search.*", "get.*"));
  }
}
